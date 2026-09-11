package gompv

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"testing"
)

func TestPlaybackHelpers(t *testing.T) {
	srv := startFakeIPCServer(t, func(conn net.Conn) {
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var req struct {
				Command   []any `json:"command"`
				RequestID int64 `json:"request_id"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
				continue
			}
			if req.RequestID == 0 {
				continue
			}
			// Simular respuesta exitosa para get_property volume
			if len(req.Command) >= 2 && req.Command[0] == "get_property" && req.Command[1] == "volume" {
				resp := fmt.Sprintf(`{"data":80.0,"error":"success","request_id":%d}`+"\n", req.RequestID)
				_, _ = conn.Write([]byte(resp))
				continue
			}
			resp := fmt.Sprintf(`{"data":true,"error":"success","request_id":%d}`+"\n", req.RequestID)
			_, _ = conn.Write([]byte(resp))
		}
	})
	defer srv.Close()

	client, err := Connect(srv.socketPath)
	if err != nil {
		t.Fatalf("falló Connect: %v", err)
	}
	defer client.Close()

	if err := client.TogglePause(); err != nil {
		t.Errorf("TogglePause falló: %v", err)
	}
	if err := client.Pause(); err != nil {
		t.Errorf("Pause falló: %v", err)
	}
	if err := client.Resume(); err != nil {
		t.Errorf("Resume falló: %v", err)
	}
	if err := client.Next(); err != nil {
		t.Errorf("Next falló: %v", err)
	}
	if err := client.Prev(); err != nil {
		t.Errorf("Prev falló: %v", err)
	}
	if err := client.Stop(); err != nil {
		t.Errorf("Stop falló: %v", err)
	}
	if err := client.SeekRelative(10); err != nil {
		t.Errorf("SeekRelative falló: %v", err)
	}
	if err := client.SeekAbsolute(60); err != nil {
		t.Errorf("SeekAbsolute falló: %v", err)
	}
	if err := client.SetVolume(80); err != nil {
		t.Errorf("SetVolume falló: %v", err)
	}
	vol, err := client.GetVolume()
	if err != nil || vol != 80.0 {
		t.Errorf("GetVolume() = (%v, %v), want (80, nil)", vol, err)
	}
	if err := client.SetMute(true); err != nil {
		t.Errorf("SetMute falló: %v", err)
	}
	if err := client.ToggleMute(); err != nil {
		t.Errorf("ToggleMute falló: %v", err)
	}
}
