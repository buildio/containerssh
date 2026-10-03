package kubernetes

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"go.containerssh.io/containerssh/config"
	"go.containerssh.io/containerssh/internal/geoip/dummy"
	"go.containerssh.io/containerssh/internal/metrics"
	"go.containerssh.io/containerssh/internal/structutils"
	"go.containerssh.io/containerssh/log"
	"k8s.io/client-go/tools/remotecommand"
)

// fakeExecutor writes output to stdout, then returns err once release is closed.
type fakeExecutor struct {
	output  []byte
	err     error
	release chan struct{}
}

func (f *fakeExecutor) Stream(options remotecommand.StreamOptions) error {
	return f.StreamWithContext(context.Background(), options)
}

func (f *fakeExecutor) StreamWithContext(_ context.Context, options remotecommand.StreamOptions) error {
	if len(f.output) > 0 {
		_, _ = options.Stdout.Write(f.output)
	}
	<-f.release
	return f.err
}

func newTestExecution(t *testing.T, executor remotecommand.Executor, tty bool) *kubernetesExecutionImpl {
	cfg := config.KubernetesConfig{}
	structutils.Defaults(&cfg)
	collector := metrics.New(dummy.New())
	pod := &kubernetesPodImpl{
		config:     cfg,
		logger:     log.NewTestLogger(t),
		wg:         &sync.WaitGroup{},
		lock:       &sync.Mutex{},
		removeLock: &sync.Mutex{},
	}
	// createExec adds to the wait group; handleStream releases it in connection mode.
	pod.wg.Add(1)
	return &kubernetesExecutionImpl{
		pod:                   pod,
		exec:                  executor,
		terminalSizeQueue:     &pushSizeQueueImpl{resizeChan: make(chan remotecommand.TerminalSize), done: make(chan struct{})},
		logger:                pod.logger,
		tty:                   tty,
		backendRequestsMetric: collector.MustCreateCounter("backend_requests", "", ""),
		backendFailuresMetric: collector.MustCreateCounter("backend_failures", "", ""),
		doneChan:              make(chan struct{}),
		lock:                  &sync.Mutex{},
	}
}

// runWithTimeout calls run() and fails the test if it does not return in time. In connection mode run() is
// called while holding networkHandler.mutex, so a run() that never returns also blocks OnDisconnect and the
// pod is never removed.
func runWithTimeout(t *testing.T, e *kubernetesExecutionImpl, onExit func(int)) {
	t.Helper()
	returned := make(chan struct{})
	go func() {
		e.run(bytes.NewReader(nil), io.Discard, io.Discard, func() error { return nil }, onExit)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return")
	}
}

func TestRunReturnsWhenStreamFailsBeforePID(t *testing.T) {
	release := make(chan struct{})
	close(release)
	e := newTestExecution(t, &fakeExecutor{err: errors.New("exec failed"), release: release}, true)
	exitCode := make(chan int, 1)
	runWithTimeout(t, e, func(code int) { exitCode <- code })
	// channelHandler.run sets the initial terminal size after run() returns; this must not panic.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.resize(ctx, 25, 80); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected resize result after exit: %v", err)
	}
	select {
	case code := <-exitCode:
		if code != 137 {
			t.Fatalf("unexpected exit code: %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exit code was not sent to the client in time")
	}
}

func TestRunReadsPIDWithTTYInConnectionMode(t *testing.T) {
	pid := make([]byte, 4)
	binary.LittleEndian.PutUint32(pid, 42)
	release := make(chan struct{})
	defer close(release)
	e := newTestExecution(t, &fakeExecutor{output: pid, release: release}, true)
	runWithTimeout(t, e, func(int) {})
	if e.pid != 42 {
		t.Fatalf("unexpected pid: %d", e.pid)
	}
}

func TestSignalAfterShutdownReleasesLock(t *testing.T) {
	e := newTestExecution(t, &fakeExecutor{}, false)
	e.pid = 42
	e.pod.shutdown = true
	for i := 0; i < 2; i++ {
		done := make(chan struct{})
		go func() {
			_ = e.signal(context.Background(), "KILL")
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("signal() blocked on the execution lock")
		}
	}
}
