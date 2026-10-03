package kubernetes

import (
	"context"
	"fmt"

	"k8s.io/client-go/tools/remotecommand"
)

type pushSizeQueueImpl struct {
	resizeChan chan remotecommand.TerminalSize
	done       chan struct{}
}

func (s *pushSizeQueueImpl) Push(ctx context.Context, size remotecommand.TerminalSize) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return fmt.Errorf("program already exited")
	case s.resizeChan <- size:
		return nil
	}
}

func (s *pushSizeQueueImpl) Next() *remotecommand.TerminalSize {
	select {
	case size := <-s.resizeChan:
		return &size
	case <-s.done:
		return nil
	}
}

// Stop closes done instead of resizeChan so a concurrent Push cannot panic.
func (s *pushSizeQueueImpl) Stop() {
	close(s.done)
}
