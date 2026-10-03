package kubernetes_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"go.containerssh.io/containerssh/config"
	"go.containerssh.io/containerssh/internal/geoip/dummy"
	"go.containerssh.io/containerssh/internal/kubernetes"
	"go.containerssh.io/containerssh/internal/metrics"
	"go.containerssh.io/containerssh/internal/sshserver"
	"go.containerssh.io/containerssh/internal/test"
	"go.containerssh.io/containerssh/log"
	"go.containerssh.io/containerssh/metadata"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

// TestPodRemovedAfterDisconnect checks that the connection-mode pod is removed when the client drops the
// connection while a pty exec is running or after the exec failed. It uses the default timeouts and a pod spec
// that copies the agent in with an init container. It needs a cluster (see test.Kubernetes), so it only runs
// when CONTAINERSSH_E2E_POD_LEAK=1.
func TestPodRemovedAfterDisconnect(t *testing.T) {
	if os.Getenv("CONTAINERSSH_E2E_POD_LEAK") != "1" {
		t.Skip("set CONTAINERSSH_E2E_POD_LEAK=1 to run against a test cluster")
	}
	// One cluster for both subtests.
	cfg := getKubernetesConfig(t)
	t.Run("silent command", func(t *testing.T) {
		testPodRemovedAfterDisconnect(t, cfg, "sleep 600", "")
	})
	t.Run("exec fails before output", func(t *testing.T) {
		testPodRemovedAfterDisconnect(t, cfg, "sleep 600", "/usr/bin/containerssh-agent-missing")
	})
}

func testPodRemovedAfterDisconnect(t *testing.T, cfg config.KubernetesConfig, program string, agentPath string) {
	logger := log.NewTestLogger(t)
	cfg.Pod.Spec.Containers = append([]core.Container(nil), cfg.Pod.Spec.Containers...)
	cfg.Pod.Mode = config.KubernetesExecutionModeConnection
	agentVolume := core.VolumeMount{Name: "agent", MountPath: "/run/containerssh"}
	cfg.Pod.Spec.InitContainers = []core.Container{{
		Name:         "guest-image",
		Image:        "containerssh/containerssh-guest-image",
		Command:      []string{"/bin/bash", "-c", "cp /usr/bin/containerssh-agent /run/containerssh/containerssh-agent"},
		VolumeMounts: []core.VolumeMount{agentVolume},
	}}
	cfg.Pod.Spec.Containers[0].VolumeMounts = []core.VolumeMount{
		{Name: "agent", MountPath: "/usr/bin/containerssh-agent", SubPath: "containerssh-agent", ReadOnly: true},
	}
	cfg.Pod.Spec.Volumes = []core.Volume{{Name: "agent", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}}}
	if agentPath != "" {
		cfg.Pod.AgentPath = agentPath
	}

	connectionID := sshserver.GenerateConnectionID()
	collector := metrics.New(dummy.New())
	backend, err := kubernetes.New(
		net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: test.GetNextPort(t, "client")},
		connectionID, cfg, logger,
		collector.MustCreateCounter("backend_requests", "", ""),
		collector.MustCreateCounter("backend_failures", "", ""),
	)
	if err != nil {
		t.Fatal(err)
	}

	user := sshserver.NewTestUser("test")
	user.RandomPassword()
	srv := sshserver.NewTestServer(t, sshserver.NewTestAuthenticationHandler(&backendHandler{backend: backend}, user), logger, nil)
	srv.Start()
	defer func() {
		// Without the fix a blocked handler can keep the server from stopping; do not hang the test.
		stopped := make(chan struct{})
		go func() { srv.Stop(10 * time.Second); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(30 * time.Second):
			t.Log("SSH server did not stop in time")
		}
	}()

	connection, err := sshserver.NewTestClient(srv.GetListen(), srv.GetHostKey(), user, logger).Connect()
	if err != nil {
		t.Fatal(err)
	}
	session := connection.MustSession()
	session.MustRequestPTY("xterm", 80, 25)
	// Without the fix the exec reply never arrives, so do not wait for it.
	go func() { _ = session.Exec(program) }()

	cli := newClientset(t, cfg)
	selector := meta.ListOptions{LabelSelector: "containerssh_connection_id=" + connectionID}
	pods, err := cli.CoreV1().Pods(cfg.Pod.Metadata.Namespace).List(context.Background(), selector)
	if err != nil || len(pods.Items) != 1 {
		t.Fatalf("expected one session pod (%v, %v)", pods, err)
	}

	time.Sleep(10 * time.Second)
	// Drop the TCP connection without closing the channel, as a killed client does.
	_ = connection.Close()

	deadline := time.Now().Add(cfg.Timeouts.PodStop + 30*time.Second)
	for time.Now().Before(deadline) {
		pods, err = cli.CoreV1().Pods(cfg.Pod.Metadata.Namespace).List(context.Background(), selector)
		if err == nil && len(pods.Items) == 0 {
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("session pod for connection %s was not removed within %s", connectionID, cfg.Timeouts.PodStop+30*time.Second)
}

type backendHandler struct {
	sshserver.AbstractHandler
	backend sshserver.NetworkConnectionHandler
}

func (h *backendHandler) OnNetworkConnection(meta metadata.ConnectionMetadata) (
	sshserver.NetworkConnectionHandler,
	metadata.ConnectionMetadata,
	error,
) {
	return h.backend, meta, nil
}

func newClientset(t *testing.T, cfg config.KubernetesConfig) *k8s.Clientset {
	cli, err := k8s.NewForConfig(&restclient.Config{
		Host: cfg.Connection.Host,
		TLSClientConfig: restclient.TLSClientConfig{
			ServerName: cfg.Connection.ServerName,
			CAData:     []byte(cfg.Connection.CAData),
			CertData:   []byte(cfg.Connection.CertData),
			KeyData:    []byte(cfg.Connection.KeyData),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return cli
}
