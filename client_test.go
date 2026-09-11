package gompv

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseMpvResponse(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"éxito simple", `{"data":100,"error":"success","request_id":1}`, false},
		{"error explícito de mpv", `{"error":"property not found","request_id":2}`, true},
		{"sin campo error (asumido éxito)", `{"data":true,"request_id":3}`, false},
		{"respuesta vacía", ``, true},
		{"JSON malformado ya NO se ignora en silencio", `{"data": true, "error": "success"`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseMpvResponse(json.RawMessage(tt.raw))
			if (err != nil) != tt.wantErr {
				t.Errorf("parseMpvResponse(%q) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		want   int64
		wantOk bool
	}{
		{"float64 válido", float64(42), 42, true},
		{"json.Number válido", json.Number("1234567"), 1234567, true},
		{"json.Number inválido", json.Number("no-es-numero"), 0, false},
		{"string no soportado", "42", 0, false},
		{"nil no soportado", nil, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toInt64(tt.input)
			if ok != tt.wantOk || (ok && got != tt.want) {
				t.Errorf("toInt64(%v) = (%v, %v), want (%v, %v)", tt.input, got, ok, tt.want, tt.wantOk)
			}
		})
	}
}

type fakeIPCServer struct {
	listener   net.Listener
	socketPath string
}

func startFakeIPCServer(t *testing.T, handleConn func(conn net.Conn)) *fakeIPCServer {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "fake_mpv.sock")
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("no se pudo levantar el socket falso: %v", err)
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handleConn(conn)
	}()

	return &fakeIPCServer{listener: ln, socketPath: socketPath}
}

func (s *fakeIPCServer) Close() {
	_ = s.listener.Close()
}

func TestReadLoopHandlesOversizedLine(t *testing.T) {
	const oversized = 9 * 1024 * 1024 // > maxLineSize (8MB) en readLoop

	srv := startFakeIPCServer(t, func(conn net.Conn) {
		defer conn.Close()
		var sb strings.Builder
		sb.WriteString(`{"data":"`)
		sb.WriteString(strings.Repeat("x", oversized))
		sb.WriteString(`","error":"success","request_id":1}` + "\n")
		_, _ = conn.Write([]byte(sb.String()))
		time.Sleep(500 * time.Millisecond)
	})
	defer srv.Close()

	client, err := Connect(srv.socketPath)
	if err != nil {
		t.Fatalf("falló Connect: %v", err)
	}
	defer client.Close()

	select {
	case _, ok := <-client.Events():
		if ok {
			t.Error("se esperaba que el canal Events se cerrara, pero llegó un evento")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: el readLoop no cerró el canal Events tras la línea sobredimensionada")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, err := client.CommandContext(ctx, "get_property", "volume"); err == nil {
		t.Error("se esperaba error tras el cierre por línea sobredimensionada, pero Command tuvo éxito")
	}
}

func TestClientConcurrentCommands(t *testing.T) {
	srv := startFakeIPCServer(t, func(conn net.Conn) {
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			var req struct {
				RequestID int64 `json:"request_id"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
				continue
			}
			if req.RequestID == 0 {
				continue
			}
			resp := fmt.Sprintf(`{"data":true,"error":"success","request_id":%d}`+"\n", req.RequestID)
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	client, err := Connect(srv.socketPath)
	if err != nil {
		t.Fatalf("falló Connect: %v", err)
	}
	defer client.Close()

	const numGoroutines = 50
	const commandsPerGoroutine = 20

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*commandsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < commandsPerGoroutine; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := client.CommandContext(ctx, "get_property", "volume")
				cancel()
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d, comando %d: %w", id, i, err)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

func TestClientIdleConnection(t *testing.T) {
	srv := startFakeIPCServer(t, func(conn net.Conn) {
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var req struct {
				RequestID int64 `json:"request_id"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
				continue
			}
			resp := fmt.Sprintf(`{"data":"pong","error":"success","request_id":%d}`+"\n", req.RequestID)
			_, _ = conn.Write([]byte(resp))
		}
	})
	defer srv.Close()

	client, err := Connect(srv.socketPath)
	if err != nil {
		t.Fatalf("falló Connect: %v", err)
	}
	defer client.Close()

	time.Sleep(2100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	raw, err := client.CommandContext(ctx, "ping")
	if err != nil {
		t.Fatalf("comando falló tras periodo de inactividad: %v", err)
	}
	if !strings.Contains(string(raw), "pong") {
		t.Errorf("respuesta inesperada: %s", string(raw))
	}
}

func TestClientSentinelErrors(t *testing.T) {
	srv := startFakeIPCServer(t, func(conn net.Conn) {
		defer conn.Close()
		// No responde para forzar timeout
		time.Sleep(1 * time.Second)
	})
	defer srv.Close()

	client, err := Connect(srv.socketPath)
	if err != nil {
		t.Fatalf("falló Connect: %v", err)
	}

	// Timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = client.CommandContext(ctx, "slow_command")
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrTimeout) {
		t.Errorf("esperaba error de timeout, se obtuvo: %v", err)
	}

	// Cerrar y verificar ErrClosed
	_ = client.Close()
	if err := client.Send("test"); !errors.Is(err, ErrClosed) {
		t.Errorf("esperaba ErrClosed en Send tras Close, se obtuvo: %v", err)
	}
	if _, err := client.Command("test"); !errors.Is(err, ErrClosed) {
		t.Errorf("esperaba ErrClosed en Command tras Close, se obtuvo: %v", err)
	}
}

func TestConnectWithRetryValidation(t *testing.T) {
	// Intervalo inválido (<= 0)
	_, err := ConnectWithRetry(context.Background(), "/tmp/fake.sock", 0)
	if err == nil {
		t.Error("ConnectWithRetry con interval 0 debió fallar")
	}

	// Contexto cancelado antes de iniciar
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ConnectWithRetry(ctx, "/tmp/fake.sock", 100*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("esperaba context.Canceled, se obtuvo: %v", err)
	}
}
