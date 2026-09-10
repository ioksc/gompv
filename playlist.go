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

// GetPlaylist retrieves the current playlist and returns the index of the active item (-1 if none).
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
	_, err := c.Command("playlist-play-index", idx)
	return err
}

// RemoveIndex removes a track from the playlist by its index.
func (c *Client) RemoveIndex(idx int) error {
	_, err := c.Command("playlist-remove", idx)
	return err
}

// Shuffle shuffles the tracks in the current playlist.
func (c *Client) Shuffle() error {
	_, err := c.Command("playlist-shuffle")
	return err
}

// TogglePause toggles between pause and play states.
func (c *Client) TogglePause() error {
	_, err := c.Command("cycle", "pause")
	return err
}

// SeekRelative seeks forward or backward by the specified number of seconds.
func (c *Client) SeekRelative(seconds float64) error {
	_, err := c.Command("seek", seconds, "relative")
	return err
}
