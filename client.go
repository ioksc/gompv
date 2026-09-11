package gompv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Client handles concurrent-safe IPC communication over an mpv Unix socket.
type Client struct {
	conn          net.Conn
	reqID         atomic.Int64
	mu            sync.Mutex
	writeMu       sync.Mutex
	pending       map[int64]chan json.RawMessage
	Events        chan Event
	droppedEvents atomic.Int64

	closed chan struct{}
	once   sync.Once
}

// Connect dials the given socket path and initializes the background read loop.
func Connect(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", socketPath, err)
	}

	c := &Client{
		conn:    conn,
		pending: make(map[int64]chan json.RawMessage),
		Events:  make(chan Event, 64),
		closed:  make(chan struct{}),
	}
	c.reqID.Store(100000)
	go c.readLoop()
	return c, nil
}

// ConnectWithRetry repeatedly attempts to connect until the socket appears or context is canceled.
func ConnectWithRetry(ctx context.Context, socketPath string, interval time.Duration) (*Client, error) {
	for {
		c, err := Connect(socketPath)
		if err == nil {
			return c, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

func (c *Client) signalClosed() {
	c.once.Do(func() {
		close(c.closed)

		c.mu.Lock()
		for rid, ch := range c.pending {
			close(ch)
			delete(c.pending, rid)
		}
		c.mu.Unlock()
	})
}

func (c *Client) readLoop() {
	defer func() {
		c.signalClosed()
		close(c.Events)
	}()

	scanner := bufio.NewScanner(c.conn)
	// Tamaño reducido a 256KB como sugerencia de optimización de memoria
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)

	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		if !scanner.Scan() {
			select {
			case <-c.closed:
				return
			default:
			}
			if err := scanner.Err(); err != nil {
				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					continue
				}
			}
			return
		}

		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		var rawMsg map[string]any
		if err := decoder.Decode(&rawMsg); err != nil {
			continue
		}

		if eventType, isEvent := rawMsg["event"].(string); isEvent {
			ev := Event{
				Type: eventType,
				Raw:  rawMsg,
			}
			select {
			case c.Events <- ev:
			case <-c.closed:
				return
			default:
				c.droppedEvents.Add(1)
			}
			continue
		}

		if ridRaw, ok := rawMsg["request_id"]; ok {
			rid, ok := toInt64(ridRaw)
			if !ok {
				continue
			}
			c.mu.Lock()
			ch, exists := c.pending[rid]
			if exists {
				delete(c.pending, rid)
			}
			c.mu.Unlock()
			if exists {
				b, _ := json.Marshal(rawMsg)
				select {
				case ch <- b:
				default:
				}
			}
		}
	}
}

// DroppedEvents returns the count of events dropped due to a full buffer.
func (c *Client) DroppedEvents() int64 {
	return c.droppedEvents.Load()
}

// Send writes an asynchronous JSON command without waiting for a response.
func (c *Client) Send(args ...any) error {
	select {
	case <-c.closed:
		return fmt.Errorf("connection closed")
	default:
	}

	payload := map[string]any{
		"command": args,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.conn.Write(append(b, '\n'))
	return err
}

// Command sends a synchronous JSON command and blocks until a response is received.
func (c *Client) Command(args ...any) (json.RawMessage, error) {
	return c.CommandContext(context.Background(), args...)
}

// CommandContext sends a synchronous JSON command respecting the provided context.
func (c *Client) CommandContext(ctx context.Context, args ...any) (json.RawMessage, error) {
	select {
	case <-c.closed:
		return nil, fmt.Errorf("connection closed")
	default:
	}

	rid := c.reqID.Add(1)
	respCh := make(chan json.RawMessage, 1)

	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return nil, fmt.Errorf("connection closed")
	default:
	}
	c.pending[rid] = respCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, rid)
		c.mu.Unlock()
	}()

	payload := map[string]any{
		"command":    args,
		"request_id": rid,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	c.writeMu.Lock()
	_, err = c.conn.Write(append(b, '\n'))
	c.writeMu.Unlock()
	if err != nil {
		return nil, err
	}

	const defaultTimeout = 5 * time.Second
	timeout := defaultTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		timeout = remaining
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case resp, ok := <-respCh:
		if !ok {
			return nil, fmt.Errorf("connection closed while awaiting response")
		}
		return parseMpvResponse(resp)
	case <-c.closed:
		return nil, fmt.Errorf("connection closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("timeout awaiting response for request_id=%d", rid)
	}
}

func parseMpvResponse(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty response")
	}
	var meta struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return raw, nil
	}
	if meta.Error != "" && meta.Error != "success" {
		return raw, fmt.Errorf("mpv: %s", meta.Error)
	}
	return raw, nil
}

// ObserveProperties registers property change notifications required by the TUI client.
func (c *Client) ObserveProperties() error {
	props := []string{
		"pause",
		"time-pos",
		"duration",
		"media-title",
		"volume",
		"mute",
		"playlist-pos",
		"playlist-count",
		"eof-reached",
		"idle-active",
	}
	for i, p := range props {
		if _, err := c.Command("observe_property", i+1, p); err != nil {
			return fmt.Errorf("observe %s: %w", p, err)
		}
	}
	return nil
}

// Close gracefully shuts down the client connection and cleans up pending requests.
func (c *Client) Close() error {
	c.signalClosed()
	if c.conn != nil {
		_ = c.conn.SetReadDeadline(time.Now())
		return c.conn.Close()
	}
	return nil
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
