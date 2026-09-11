package gompv

import (
	"encoding/json"
	"fmt"
	"path/filepath"
)

// PlaylistItem represents an entry within the mpv playlist.
type PlaylistItem struct {
	Filename string `json:"filename"`
	Title    string `json:"title"`
	Current  bool   `json:"current"`
	Playing  bool   `json:"playing"`
}

// DisplayName returns the metadata title or, if unavailable, the base file name.
func (p PlaylistItem) DisplayName() string {
	if p.Title != "" {
		return p.Title
	}
	if p.Filename != "" {
		return filepath.Base(p.Filename)
	}
	return "(unknown)"
}
// GetPlaylist queries the current playlist and returns the active track index (-1 if none).
func (c *Client) GetPlaylist() ([]PlaylistItem, int, error) {
	raw, err := c.Command("get_property", "playlist")
	if err != nil {
		return nil, -1, err
	}

	// Restauramos la estructura con el envoltorio "data" porque c.Command
	// devuelve el objeto JSON completo de la respuesta de mpv.
	var resp struct {
		Data []PlaylistItem `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, -1, fmt.Errorf("failed to unmarshal playlist: %w", err)
	}

	currentIdx := -1
	for i, item := range resp.Data {
		if item.Current {
			currentIdx = i
			break
		}
	}

	return resp.Data, currentIdx, nil
}

// PlayIndex switches playback to the specified playlist index.
func (c *Client) PlayIndex(idx int) error {
	_, err := c.Command("playlist-play-index", idx)
	return err
}

// RemoveIndex removes a track from the playlist by its index.
func (c *Client) RemoveIndex(idx int) error {
	_, err := c.Command("playlist-remove", idx)
	return err
}

// Shuffle randomizes the order of tracks in the playlist.
func (c *Client) Shuffle() error {
	_, err := c.Command("playlist-shuffle")
	return err
}

// ClearPlaylist removes all entries from the playlist.
func (c *Client) ClearPlaylist() error {
	_, err := c.Command("playlist-clear")
	return err
}

// LoadFile appends or replaces a file in the playlist.
// Mode can be "replace", "append", or "append-play". Defaults to "replace".
func (c *Client) LoadFile(path string, mode ...string) error {
	m := "replace"
	if len(mode) > 0 && mode[0] != "" {
		m = mode[0]
	}
	_, err := c.Command("loadfile", path, m)
	return err
}
