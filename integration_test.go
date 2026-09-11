package gompv

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

func checkMpv(t *testing.T) {
	if _, err := exec.LookPath("mpv"); err != nil {
		t.Skip("mpv no está instalado en el sistema, omitiendo test")
	}
}

func TestServerLifecycle(t *testing.T) {
	checkMpv(t)
	socketPath := "/tmp/gompv_server_test.sock"
	srv, err := StartServer(socketPath)
	if err != nil {
		t.Fatalf("falló StartServer: %v", err)
	}
	defer srv.Stop()

	if srv.SocketPath != socketPath {
		t.Errorf("ruta de socket incorrecta")
	}
}

func TestClientConnectionAndProperties(t *testing.T) {
	checkMpv(t)
	socketPath := "/tmp/gompv_client_test.sock"
	srv, client, err := StartAndConnect(context.Background(), socketPath)
	if err != nil {
		t.Fatalf("falló StartAndConnect: %v", err)
	}
	defer srv.Stop()
	defer client.Close()

	if err := client.ObserveProperties(); err != nil {
		t.Errorf("falló ObserveProperties: %v", err)
	}

	// Verificar que podemos obtener una propiedad básica
	raw, err := client.Command("get_property", "volume")
	if err != nil {
		t.Errorf("no se pudo obtener volume: %v", err)
	}
	t.Logf("Volumen inicial: %s", string(raw))
}

func TestPlaylistOperations(t *testing.T) {
	checkMpv(t)
	socketPath := "/tmp/gompv_playlist_test.sock"
	srv, client, err := StartAndConnect(context.Background(), socketPath)
	if err != nil {
		t.Fatalf("falló StartAndConnect: %v", err)
	}
	defer srv.Stop()
	defer client.Close()

	_, _, err = client.GetPlaylist()
	if err != nil {
		t.Errorf("falló GetPlaylist: %v", err)
	}

	// Estos comandos pueden fallar si la playlist está vacía, pero no deberían panicar
	_ = client.PlayIndex(0)
	_ = client.RemoveIndex(0)
	_ = client.Shuffle()
}

func TestPlaybackControl(t *testing.T) {
	checkMpv(t)
	socketPath := "/tmp/gompv_control_test.sock"
	srv, client, err := StartAndConnect(context.Background(), socketPath)
	if err != nil {
		t.Fatalf("falló StartAndConnect: %v", err)
	}
	defer srv.Stop()
	defer client.Close()

	if err := client.TogglePause(); err != nil {
		t.Errorf("falló TogglePause: %v", err)
	}

	// Verificar que el estado cambió
	raw, err := client.Command("get_property", "pause")
	if err != nil {
		t.Errorf("falló get_property pause: %v", err)
	}
	t.Logf("Estado de pausa: %s", string(raw))
}

func TestEventsHandling(t *testing.T) {
	checkMpv(t)
	socketPath := "/tmp/gompv_events_test.sock"
	srv, client, err := StartAndConnect(context.Background(), socketPath)
	if err != nil {
		t.Fatalf("falló StartAndConnect: %v", err)
	}
	defer srv.Stop()
	defer client.Close()

	// 1. Es crucial observar las propiedades para recibir eventos de cambio
	if err := client.ObserveProperties(); err != nil {
		t.Fatal(err)
	}

	// 2. Limpiar eventos antiguos que pudieron llegar durante la conexión
	time.Sleep(100 * time.Millisecond)
	for len(client.Events) > 0 {
		<-client.Events
	}

	// 3. Enviar el comando de cambio de volumen
	if _, err := client.Command("set_property", "volume", 85); err != nil {
		t.Fatalf("falló al enviar comando de volumen: %v", err)
	}

	// 4. Esperar específicamente el evento de 'volume'
	foundVolume := false
	timeout := time.After(2 * time.Second)
	
	for !foundVolume {
		select {
		case ev := <-client.Events:
			name, val, ok := ev.PropertyChange()
			if ok {
				t.Logf("Evento recibido: %s = %v", name, val)
				if name == "volume" {
					foundVolume = true
				}
			}
		case <-timeout:
			t.Error("timeout esperando evento de cambio de volumen")
			return
		}
	}

	if client.DroppedEvents() > 0 {
		t.Errorf("se perdieron %d eventos inesperadamente", client.DroppedEvents())
	}
}

func TestConnectionRetry(t *testing.T) {
	checkMpv(t)
	socketPath := "/tmp/gompv_retry_test.sock"
	
	srv, err := StartServer(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := ConnectWithRetry(ctx, socketPath, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("falló ConnectWithRetry: %v", err)
	}
	defer client.Close()

	if err := client.Send("print-text", "hello retry"); err != nil {
		t.Errorf("falló envío tras reconexión: %v", err)
	}
}
