# gompv

[![Go Reference](https://pkg.go.dev/badge/github.com/ioksc/gompv.svg)](https://pkg.go.dev/github.com/ioksc/gompv)
[![Go Test & Build](https://github.com/ioksc/gompv/actions/workflows/go.yml/badge.svg)](https://github.com/ioksc/gompv/actions/workflows/go.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A clean, idiomatic, and concurrent-safe Go client for controlling [mpv](https://mpv.io) through its JSON-IPC interface over Unix sockets.

## Features

- **Concurrent-safe IPC**: Send commands and receive correlated responses synchronously or asynchronously.
- **Event Streaming**: Listen to mpv property changes and lifecycle events via a type-safe channel (`<-chan Event`).
- **Convenient Playback API**: Play, pause, resume, seek, toggle mute, volume controls, and playlist navigation.
- **Playlist Management**: Query entries, switch tracks, remove items, clear, and shuffle playlists.
- **Optional Process Launcher**: Built-in helper to spawn and supervise background `mpv` processes cleanly.
- **Sentinel Errors**: Standard errors (`ErrClosed`, `ErrTimeout`) for idiomatic `errors.Is` error handling.
- **Zero External Dependencies**: Pure standard library implementation.

## Requirements

- Go 1.27
- Linux / Unix environment with `mpv` installed (uses Unix domain sockets).

## Installation

```bash
go get github.com/ioksc/gompv
```

## Quick Start

### 1. Connect to an existing mpv instance

Start mpv with an IPC socket:

```bash
mpv --input-ipc-server=/tmp/mpv.sock --idle=yes
```

Connect using `gompv`:

```go
package main

import (
    "fmt"
    "log"

    "github.com/ioksc/gompv"
)

func main() {
    client, err := gompv.Connect("/tmp/mpv.sock")
    if err != nil {
        log.Fatalf("connect: %v", err)
    }
    defer client.Close()

    // Play a file
    if err := client.LoadFile("song.flac"); err != nil {
        log.Fatalf("load file: %v", err)
    }

    // Toggle pause
    if err := client.TogglePause(); err != nil {
        log.Fatalf("pause: %v", err)
    }

    fmt.Println("Playing file via mpv!")
}
```

### 2. Spawn and supervise mpv automatically

If you want `gompv` to manage the lifecycle of the background `mpv` process:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/ioksc/gompv"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    socketPath := "/tmp/mpv-app.sock"
    server, client, err := gompv.StartAndConnect(ctx, socketPath, "song.mp3")
    if err != nil {
        log.Fatal(err)
    }
    defer server.Stop()
    defer client.Close()

    // Observe properties to receive change events
    _ = client.ObserveProperties("volume", "time-pos", "pause")

    go func() {
        for ev := range client.Events() {
            if name, val, ok := ev.PropertyFloat(); ok {
                fmt.Printf("Property %s updated to %v\n", name, val)
            }
        }
    }()

    _ = server.Wait()
}
```

## API Highlights

### Low-level IPC Commands

- `client.Command(args ...any) (json.RawMessage, error)`: Synchronously sends a command and waits for response.
- `client.CommandContext(ctx, args ...any)`: Context-aware synchronous command with custom deadlines.
- `client.Send(args ...any) error`: Asynchronous fire-and-forget command.

### High-level Playback Controls

- `client.TogglePause()`, `client.Pause()`, `client.Resume()`, `client.Stop()`
- `client.Next()`, `client.Prev()`
- `client.SeekRelative(seconds)`, `client.SeekAbsolute(seconds)`
- `client.SetVolume(vol)`, `client.GetVolume()`, `client.SetMute(bool)`, `client.ToggleMute()`

### Playlists

- `client.GetPlaylist() ([]PlaylistItem, int, error)`
- `client.PlayIndex(index)`
- `client.RemoveIndex(index)`
- `client.ClearPlaylist()`
- `client.Shuffle()`
- `client.LoadFile(path, mode...)`

### Events & Properties

- `client.Events() <-chan Event`: Channel streaming typed events from mpv.
- `client.ObserveProperties(properties ...string)`: Observes properties (defaults to common media properties if empty).
- `client.ObserveProperty(id int64, property string)`: Observes a specific property.
- `ev.PropertyChange() (name string, data any, ok bool)`
- `ev.PropertyFloat() (name string, val float64, ok bool)`
- `ev.PropertyBool() (name string, val bool, ok bool)`
- `ev.PropertyString() (name string, val string, ok bool)`

### Error Handling

Standard sentinel errors are provided for robust inspection via `errors.Is`:

- `gompv.ErrClosed`: Returned when an operation is attempted on a closed connection.
- `gompv.ErrTimeout`: Returned when a command response exceeds its deadline.

```go
if errors.Is(err, gompv.ErrTimeout) {
    log.Println("Command timed out")
}
```

## License

MIT License. See [LICENSE](LICENSE) for details.
