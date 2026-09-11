package gompv

import (
	"encoding/json"
	"fmt"
)

// TogglePause toggles playback state between pause and play.
func (c *Client) TogglePause() error {
	_, err := c.Command("cycle", "pause")
	return err
}

// Pause pauses the current playback.
func (c *Client) Pause() error {
	_, err := c.Command("set_property", "pause", true)
	return err
}

// Resume resumes playback if paused.
func (c *Client) Resume() error {
	_, err := c.Command("set_property", "pause", false)
	return err
}

// Stop terminates the currently playing file.
func (c *Client) Stop() error {
	_, err := c.Command("stop")
	return err
}

// Next advances to the next entry in the playlist.
func (c *Client) Next() error {
	_, err := c.Command("playlist-next", "weak")
	return err
}

// Prev moves to the previous entry in the playlist.
func (c *Client) Prev() error {
	_, err := c.Command("playlist-prev", "weak")
	return err
}

// SeekRelative performs a relative seek by the specified number of seconds.
func (c *Client) SeekRelative(seconds float64) error {
	return c.Send("seek", seconds, "relative")
}

// SeekAbsolute performs an absolute seek to the specified timestamp in seconds.
func (c *Client) SeekAbsolute(seconds float64) error {
	return c.Send("seek", seconds, "absolute")
}

// SetVolume sets playback volume (0-100+).
func (c *Client) SetVolume(volume float64) error {
	_, err := c.Command("set_property", "volume", volume)
	return err
}

// GetVolume retrieves the current volume level.
func (c *Client) GetVolume() (float64, error) {
	raw, err := c.Command("get_property", "volume")
	if err != nil {
		return 0, err
	}
	var resp struct {
		Data any `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return 0, fmt.Errorf("failed to parse volume: %w", err)
	}
	v, ok := toFloat(resp.Data)
	if !ok {
		return 0, fmt.Errorf("unexpected volume data: %v", resp.Data)
	}
	return v, nil
}

// SetMute sets the mute state.
func (c *Client) SetMute(mute bool) error {
	_, err := c.Command("set_property", "mute", mute)
	return err
}

// ToggleMute toggles mute state.
func (c *Client) ToggleMute() error {
	_, err := c.Command("cycle", "mute")
	return err
}
