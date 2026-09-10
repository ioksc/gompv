package gompv

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// Server manages the background mpv process.
type Server struct {
	cmd        *exec.Cmd
	SocketPath string
}

// StartServer removes any stale socket if it exists and launches mpv without a graphical user interface.
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
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start mpv process: %w", err)
	}

	return &Server{
		cmd:        cmd,
		SocketPath: socketPath,
	}, nil
}

// StartAndConnect is a convenience helper that launches mpv and connects to it using ConnectWithRetry.
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

// Wait blocks until the mpv process terminates.
func (s *Server) Wait() error {
	return s.cmd.Wait()
}

// Stop terminates the mpv process cleanly and removes the socket file.
func (s *Server) Stop() {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Signal(syscall.SIGTERM)
		_, _ = s.cmd.Process.Wait()
	}
	_ = os.Remove(s.SocketPath)
}
