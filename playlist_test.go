package gompv

import (
	"testing"
)

func TestPlaylistItemDisplayName(t *testing.T) {
	tests := []struct {
		name string
		item PlaylistItem
		want string
	}{
		{"usa título si existe", PlaylistItem{Title: "Mi Canción", Filename: "/x/song.mp3"}, "Mi Canción"},
		{"cae al nombre base del archivo", PlaylistItem{Filename: "/music/rock/song.flac"}, "song.flac"},
		{"desconocido si no hay nada", PlaylistItem{}, "(unknown)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.item.DisplayName(); got != tt.want {
				t.Errorf("DisplayName() = %q, want %q", got, tt.want)
			}
		})
	}
}
