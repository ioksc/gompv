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
	return c.Send("playlist-play-index", idx)
}

// RemoveIndex removes a track from the playlist by its index.
func (c *Client) RemoveIndex(idx int) error {
	return c.Send("playlist-remove", idx)
}

// Shuffle randomizes the order of tracks in the playlist.
func (c *Client) Shuffle() error {
	return c.Send("playlist-shuffle")
}

// TogglePause toggles the playback state between pause and play.
func (c *Client) TogglePause() error {
	return c.Send("cycle", "pause")
}

// SeekRelative performs a relative seek by the specified number of seconds.
func (c *Client) SeekRelative(seconds float64) error {
	return c.Send("seek", seconds, "relative")
}
