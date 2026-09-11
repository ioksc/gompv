package gompv

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrClosed is returned when an operation is performed on a closed client.
	ErrClosed = errors.New("gompv: connection closed")
	// ErrTimeout is returned when a command response exceeds its deadline.
	ErrTimeout = errors.New("gompv: request timeout")
)

// Client handles concurrent-safe IPC communication over an mpv Unix socket.
type Client struct {
	conn          net.Conn
	reqID         atomic.Int64
	mu            sync.Mutex
	writeMu       sync.Mutex
	pending       map[int64]chan json.RawMessage
	events        chan Event
	droppedEvents atomic.Int64

	closed chan struct{}
	once   sync.Once
}

// Events returns a read-only channel of events received from mpv.
func (c *Client) Events() <-chan Event {
	return c.events
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
		events:  make(chan Event, 64),
		closed:  make(chan struct{}),
	}
	c.reqID.Store(100000)
	go c.readLoop()
	return c, nil
}

// ConnectWithRetry repeatedly attempts to connect until the socket appears or context is canceled.
func ConnectWithRetry(ctx context.Context, socketPath string, interval time.Duration) (*Client, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		c, err := Connect(socketPath)
		if err == nil {
			return c, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
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
		close(c.events)
	}()

	scanner := bufio.NewScanner(c.conn)
	// mpv may emit long lines (extensive playlists, large metadata, etc.).
	// Use a generous limit (8MB) to prevent bufio.ErrTooLong during normal operation.
	const maxLineSize = 8 * 1024 * 1024
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var header struct {
			Event     string `json:"event"`
			RequestID any    `json:"request_id"`
		}
		if err := json.Unmarshal(line, &header); err != nil {
			continue
		}

		if header.Event != "" {
			var rawMsg map[string]any
			_ = json.Unmarshal(line, &rawMsg)
			ev := Event{
				Type: EventType(header.Event),
				Raw:  rawMsg,
			}
			select {
			case c.events <- ev:
			case <-c.closed:
				return
			default:
				c.droppedEvents.Add(1)
			}
			continue
		}

		if header.RequestID != nil {
			rid, ok := toInt64(header.RequestID)
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
				msgCopy := make(json.RawMessage, len(line))
				copy(msgCopy, line)
				select {
				case ch <- msgCopy:
				default:
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			_ = c.conn.Close()
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
		return ErrClosed
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
		return nil, ErrClosed
	default:
	}

	rid := c.reqID.Add(1)
	respCh := make(chan json.RawMessage, 1)

	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return nil, ErrClosed
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
			return nil, fmt.Errorf("%w: awaiting response for request_id=%d", ErrClosed, rid)
		}
		return parseMpvResponse(resp)
	case <-c.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("%w: awaiting response for request_id=%d", ErrTimeout, rid)
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
		return raw, fmt.Errorf("failed to unmarshal mpv response: %w", err)
	}
	if meta.Error != "" && meta.Error != "success" {
		return raw, fmt.Errorf("mpv: %s", meta.Error)
	}
	return raw, nil
}

// DefaultObservedProperties contains the common properties observed by media players.
var DefaultObservedProperties = []string{
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

// ObserveProperty registers an individual property change notification with mpv.
func (c *Client) ObserveProperty(id int64, property string) error {
	_, err := c.Command("observe_property", id, property)
	return err
}

// ObserveProperties registers change notifications for the given properties.
// If no properties are provided, DefaultObservedProperties will be observed.
func (c *Client) ObserveProperties(properties ...string) error {
	if len(properties) == 0 {
		properties = DefaultObservedProperties
	}
	for i, p := range properties {
		if err := c.ObserveProperty(int64(i+1), p); err != nil {
			return fmt.Errorf("observe %s: %w", p, err)
		}
	}
	return nil
}

// Close gracefully shuts down the client connection and cleans up pending requests.
func (c *Client) Close() error {
	c.signalClosed()
	if c.conn != nil {
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
