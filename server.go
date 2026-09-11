package gompv

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Server manages the background mpv process lifecycle.
type Server struct {
	cmd        *exec.Cmd
	SocketPath string
	waitOnce   sync.Once
	waitErr    error
	waitDone   chan struct{}
}

// StartServer removes any stale socket if it exists and launches mpv in background IPC mode.
func StartServer(socketPath string, files ...string) (*Server, error) {
	_ = os.Remove(socketPath)

	args := []string{
		"--input-ipc-server=" + socketPath,
		"--idle=yes",
		"--vo=null",
		"--force-window=no",
		"--no-video",
		"--no-terminal",
		"--really-quiet",
	}
	args = append(args, files...)

	cmd := exec.Command("mpv", args...)
	// Place mpv into its own process group so we can signal the entire
	// process tree (mpv + potential children) in Stop(), rather than
	// just the main process.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start mpv process: %w", err)
	}

	return &Server{
		cmd:        cmd,
		SocketPath: socketPath,
		waitDone:   make(chan struct{}),
	}, nil
}

// StartAndConnect launches mpv and establishes a connection using retry logic.
func StartAndConnect(ctx context.Context, socketPath string, files ...string) (*Server, *Client, error) {
	srv, err := StartServer(socketPath, files...)
	if err != nil {
		return nil, nil, err
	}

	client, err := ConnectWithRetry(ctx, socketPath, 100*time.Millisecond)
	if err != nil {
		srv.Stop()
		return nil, nil, err
	}

	return srv, client, nil
}

func (s *Server) wait() error {
	s.waitOnce.Do(func() {
		s.waitErr = s.cmd.Wait()
		close(s.waitDone)
	})
	<-s.waitDone
	return s.waitErr
}

// Wait blocks until the underlying mpv process terminates.
func (s *Server) Wait() error {
	return s.wait()
}

// Stop terminates the mpv process cleanly with timeout fallback and removes the socket file.
func (s *Server) Stop() {
	if s.cmd != nil && s.cmd.Process != nil {
		pgid := s.cmd.Process.Pid
		_ = syscall.Kill(-pgid, syscall.SIGTERM)

		go func() {
			_ = s.wait()
		}()

		select {
		case <-s.waitDone:
		case <-time.After(2 * time.Second):
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			_ = s.wait()
		}
	}
	_ = os.Remove(s.SocketPath)
}
