package containerssh_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	containerssh "go.containerssh.io/containerssh"
	"go.containerssh.io/containerssh/config"
	"go.containerssh.io/containerssh/internal/test"
	"go.containerssh.io/containerssh/log"
	"golang.org/x/crypto/ssh"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

// Test-only values. The secret must never show up in the ContainerSSH log.
const (
	e2eWebhookSecret = "e2e-webhook-secret-7c1d93f0"
	e2eTicket        = "bldr_e2e_ticket"
	e2eApp           = "testapp"
)

// A new namespace per run, so a run does not wait for the last one to be deleted.
var e2eNamespace = fmt.Sprintf("build-testapp-%d", time.Now().Unix())

// TestProductionShapedWebhooks runs ContainerSSH in connection mode with the Kubernetes backend against auth and
// config webhooks shaped like buildio/build API::ContainersshController (Basic auth from the URL userinfo, a password
// response without environment/files, a production-like pod spec). It needs a cluster (see test.Kubernetes), so it
// only runs when CONTAINERSSH_E2E_POD_LEAK=1.
func TestProductionShapedWebhooks(t *testing.T) {
	if os.Getenv("CONTAINERSSH_E2E_POD_LEAK") != "1" {
		t.Skip("set CONTAINERSSH_E2E_POD_LEAK=1 to run against a test cluster")
	}
	kube := test.Kubernetes(t)
	cli := e2eClientset(t, kube)
	ctx := context.Background()
	if _, err := cli.CoreV1().Namespaces().Create(ctx, &core.Namespace{ObjectMeta: meta.ObjectMeta{Name: e2eNamespace}}, meta.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.CoreV1().Namespaces().Delete(ctx, e2eNamespace, meta.DeleteOptions{}) })
	if _, err := cli.CoreV1().ConfigMaps(e2eNamespace).Create(ctx, &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{Name: e2eApp + "-env"},
		Data:       map[string]string{"FOO": "bar"},
	}, meta.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	t.Run("happy path", func(t *testing.T) {
		hooks := newE2EWebhooks(t, kube, nil)
		logs := startE2EContainerSSH(t, hooks.url+"/api/containerssh", hooks.url+"/api/containerssh/configure")

		client, err := e2eDial(logs.listen)
		if err != nil {
			t.Fatalf("login failed (%v)\n%s", err, logs)
		}
		session, err := client.NewSession()
		if err != nil {
			t.Fatal(err)
		}
		out, err := session.CombinedOutput("echo hello-$BUILD_DYNO_PROCESS_TYPE-$FOO; exit 3")
		var exitErr *ssh.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitStatus() != 3 {
			t.Errorf("expected exit status 3, got %v", err)
		}
		if string(out) != "hello-run-bar\n" {
			t.Errorf("unexpected output %q", out)
		}
		_ = client.Close()

		if hooks.authorized.Load() < 2 || hooks.rejected.Load() != 0 {
			t.Errorf("webhooks: %d calls with Basic auth, %d rejected", hooks.authorized.Load(), hooks.rejected.Load())
		}
		assertPodRemoved(t, cli, hooks.connectionID(t))
		logs.assertRedacted(t)
	})

	t.Run("pod start timeout", func(t *testing.T) {
		hooks := newE2EWebhooks(t, kube, func(pod map[string]any, k map[string]any) {
			k["timeouts"] = map[string]any{"podStart": int64(5 * time.Second)}
			// The init container runs longer than podStart, so the pod is not ready in time.
			initContainer := pod["spec"].(map[string]any)["initContainers"].([]any)[0].(map[string]any)
			initContainer["command"] = []any{"/bin/bash", "-c", "sleep 20; cp /usr/bin/containerssh-agent /run/containerssh/containerssh-agent"}
		})
		logs := startE2EContainerSSH(t, hooks.url+"/api/containerssh", hooks.url+"/api/containerssh/configure")

		// The backend runs after the SSH handshake, so the server closes the connection instead of failing the login.
		if client, err := e2eDial(logs.listen); err == nil {
			defer func() { _ = client.Close() }()
			if session, err := client.NewSession(); err == nil {
				_ = session.Close()
				t.Fatal("got a session although the pod did not start in time")
			}
		}
		if !strings.Contains(logs.String(), "Failed to wait for pod to come up.") {
			t.Errorf("the pod start did not time out\n%s", logs)
		}
		assertPodRemoved(t, cli, hooks.connectionID(t))
		logs.assertRedacted(t)
	})

	t.Run("webhook unreachable", func(t *testing.T) {
		closed := fmt.Sprintf("http://containerssh:%s@127.0.0.1:%d/api/containerssh", e2eWebhookSecret, test.GetNextPort(t, "closed"))
		logs := startE2EContainerSSH(t, closed, closed+"/configure")
		if client, err := e2eDial(logs.listen); err == nil {
			_ = client.Close()
			t.Fatal("login worked without an auth server")
		}
		if !strings.Contains(logs.String(), "connection refused") {
			t.Errorf("expected a connection failure in the log\n%s", logs)
		}
		logs.assertRedacted(t)
	})
}

type e2eWebhooks struct {
	url        string
	authorized atomic.Int32
	rejected   atomic.Int32
	lock       sync.Mutex
	connID     string
}

func (h *e2eWebhooks) connectionID(t *testing.T) string {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.connID == "" {
		t.Fatal("the config server was not called")
	}
	return h.connID
}

// newE2EWebhooks starts a server that answers like API::ContainersshController#password and #configure.
// customize can change the pod and kubernetes sections of the config response.
func newE2EWebhooks(t *testing.T, kube test.KubernetesTestConfiguration, customize func(pod, k map[string]any)) *e2eWebhooks {
	h := &e2eWebhooks{}
	basicAuth := func(next func(w http.ResponseWriter, body map[string]any)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if _, password, ok := r.BasicAuth(); !ok || password != e2eWebhookSecret {
				h.rejected.Add(1)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			h.authorized.Add(1)
			body := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			next(w, body)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/containerssh/password", basicAuth(func(w http.ResponseWriter, body map[string]any) {
		password, _ := base64.StdEncoding.DecodeString(fmt.Sprint(body["passwordBase64"]))
		if body["username"] != e2eApp || string(password) != e2eTicket {
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false})
			return
		}
		// Same shape as production: metadata only, no environment or files.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success":               true,
			"authenticatedUsername": "user",
			"metadata":              map[string]any{"token": map[string]any{"value": string(password), "sensitive": true}},
		})
	}))
	mux.HandleFunc("/api/containerssh/configure", basicAuth(func(w http.ResponseWriter, body map[string]any) {
		token, _ := body["metadata"].(map[string]any)["token"].(map[string]any)
		if token == nil || token["value"] != e2eTicket {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		h.lock.Lock()
		h.connID = fmt.Sprint(body["connectionId"])
		h.lock.Unlock()
		pod := e2eProductionPod()
		k := map[string]any{
			// Production uses the in-cluster service account; the test cluster needs client certificates.
			"connection": map[string]any{
				"host": kube.Host, "serverName": kube.ServerName,
				"cacert": kube.CACert, "cert": kube.UserCert, "key": kube.UserKey,
			},
			"pod": pod,
		}
		if customize != nil {
			customize(pod, k)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"environment": map[string]any{"PS1": map[string]any{"value": `\w \$ `, "sensitive": false}},
			"config": map[string]any{
				"backend":    "kubernetes",
				"security":   map[string]any{"env": map[string]any{"mode": "filter", "allow": []any{"TERM", "COLUMNS", "LINES"}}},
				"kubernetes": k,
			},
		})
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	h.url = strings.Replace(srv.URL, "http://", "http://containerssh:"+e2eWebhookSecret+"@", 1)
	return h
}

func e2eProductionPod() map[string]any {
	agent := []any{map[string]any{"name": "agent", "mountPath": "/run/containerssh"}}
	return map[string]any{
		"mode":     "connection",
		"metadata": map[string]any{"name": fmt.Sprintf("%s-user-example-com-%d", e2eApp, time.Now().UnixNano()), "namespace": e2eNamespace},
		"spec": map[string]any{
			"initContainers": []any{map[string]any{
				"name":         "guest-image",
				"image":        "containerssh/containerssh-guest-image:latest",
				"command":      []any{"/bin/bash", "-c", "cp /usr/bin/containerssh-agent /run/containerssh/containerssh-agent"},
				"volumeMounts": agent,
			}},
			"containers": []any{map[string]any{
				"name":    "app",
				"image":   "containerssh/containerssh-guest-image:latest",
				"env":     []any{map[string]any{"name": "BUILD_DYNO_PROCESS_TYPE", "value": "run"}},
				"envFrom": []any{map[string]any{"configMapRef": map[string]any{"name": e2eApp + "-env"}}},
				"resources": map[string]any{
					"limits":   map[string]any{"cpu": "500m", "memory": "256Mi"},
					"requests": map[string]any{"cpu": "50m", "memory": "64Mi"},
				},
				"volumeMounts": []any{map[string]any{
					"name": "agent", "mountPath": "/usr/bin/containerssh-agent", "subPath": "containerssh-agent", "readOnly": true,
				}},
			}},
			"volumes":          []any{map[string]any{"name": "agent", "emptyDir": map[string]any{}}},
			"imagePullSecrets": []any{map[string]any{"name": "registry-access"}},
		},
	}
}

type e2eLog struct {
	lock   sync.Mutex
	buf    bytes.Buffer
	listen string
}

func (l *e2eLog) Write(p []byte) (int, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.buf.Write(p)
}

func (l *e2eLog) String() string {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.buf.String()
}

// assertRedacted checks that neither the secret nor the Basic auth header value was logged, and that the URL was
// logged in redacted form, so the check is not empty.
func (l *e2eLog) assertRedacted(t *testing.T) {
	t.Helper()
	logs := l.String()
	basic := base64.StdEncoding.EncodeToString([]byte("containerssh:" + e2eWebhookSecret))
	if strings.Contains(logs, e2eWebhookSecret) || strings.Contains(logs, basic) {
		t.Errorf("the webhook secret is in the ContainerSSH log:\n%s", logs)
	}
	if !strings.Contains(logs, "containerssh:xxxxx@127.0.0.1") {
		t.Errorf("the redacted webhook URL is not in the debug log:\n%s", logs)
	}
}

// startE2EContainerSSH starts ContainerSSH with the production base config at debug level and captures its log.
func startE2EContainerSSH(t *testing.T, authURL string, configURL string) *e2eLog {
	logs := &e2eLog{listen: fmt.Sprintf("127.0.0.1:%d", test.GetNextPort(t, "ContainerSSH"))}
	cfg := config.AppConfig{}
	cfg.Default()
	cfg.SSH.Listen = logs.listen
	if err := cfg.SSH.GenerateHostKey(); err != nil {
		t.Fatal(err)
	}
	cfg.Security.DefaultMode = config.ExecutionPolicyDisable
	cfg.Security.Command.Mode = config.ExecutionPolicyEnable
	cfg.Security.Shell.Mode = config.ExecutionPolicyEnable
	cfg.Security.TTY.Mode = config.ExecutionPolicyEnable
	cfg.Security.Signal.Mode = config.ExecutionPolicyEnable
	cfg.Auth.PasswordAuth.Method = config.PasswordAuthMethodWebhook
	cfg.Auth.PasswordAuth.Webhook.URL = authURL
	cfg.Auth.PasswordAuth.Webhook.AuthTimeout = 5 * time.Second
	cfg.ConfigServer.URL = configURL
	cfg.Log.Level = config.LogLevelDebug
	cfg.Log.Format = config.LogFormatText
	cfg.Log.Destination = config.LogDestinationStdout
	cfg.Log.Stdout = logs

	srv, lifecycle, err := containerssh.New(cfg, log.NewLoggerFactory())
	if err != nil {
		t.Fatalf("failed to start ContainerSSH (%v)", err)
	}
	go func() { _ = srv.RunWithLifecycle(lifecycle) }()
	t.Cleanup(func() {
		lifecycle.Stop(context.Background())
		if t.Failed() {
			t.Logf("ContainerSSH log:\n%s", logs)
		}
	})
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", logs.listen); err == nil {
			_ = c.Close()
			return logs
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("ContainerSSH did not start\n%s", logs)
	return nil
}

func e2eDial(listen string) (*ssh.Client, error) {
	return ssh.Dial("tcp", listen, &ssh.ClientConfig{
		User:            e2eApp,
		Auth:            []ssh.AuthMethod{ssh.Password(e2eTicket)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // test server
		Timeout:         2 * time.Minute,
	})
}

// assertPodRemoved waits for the session pod of the connection to be gone. The default podStop is 60s and deletion
// is graceful, so allow two minutes.
func assertPodRemoved(t *testing.T, cli *k8s.Clientset, connectionID string) {
	t.Helper()
	selector := meta.ListOptions{LabelSelector: "containerssh_connection_id=" + connectionID}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		pods, err := cli.CoreV1().Pods(e2eNamespace).List(context.Background(), selector)
		if err == nil && len(pods.Items) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session pod for connection %s was not removed (%d pods left, %v)", connectionID, len(pods.Items), err)
		}
		time.Sleep(3 * time.Second)
	}
}

func e2eClientset(t *testing.T, kube test.KubernetesTestConfiguration) *k8s.Clientset {
	cli, err := k8s.NewForConfig(&restclient.Config{
		Host: kube.Host,
		TLSClientConfig: restclient.TLSClientConfig{
			ServerName: kube.ServerName,
			CAData:     []byte(kube.CACert),
			CertData:   []byte(kube.UserCert),
			KeyData:    []byte(kube.UserKey),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return cli
}
