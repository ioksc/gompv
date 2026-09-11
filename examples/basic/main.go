package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ioksc/gompv"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <archivo-de-audio-o-video>")
		return
	}
	file := os.Args[1]

	socketPath := "/tmp/mpv-example.sock"

	// Capturar Ctrl+C (SIGINT) y SIGTERM para que los defer y la terminación de mpv se ejecuten
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connCtx, connCancel := context.WithTimeout(ctx, 10*time.Second)
	defer connCancel()

	// Iniciar mpv y conectarse por socket IPC
	server, client, err := gompv.StartAndConnect(connCtx, socketPath, file)
	if err != nil {
		log.Fatalf("Error al iniciar mpv: %v", err)
	}
	defer server.Stop()
	defer client.Close()

	// Detener mpv limpiamente al recibir señal de cancelación
	go func() {
		<-ctx.Done()
		server.Stop()
	}()

	// Observar propiedades básicas
	if err := client.ObserveProperties("time-pos"); err != nil {
		log.Printf("Advertencia al observar propiedades: %v", err)
	}

	fmt.Printf("Reproduciendo: %s (Presiona Ctrl+C para salir)\n", file)

	// Escuchar eventos en una goroutine
	go func() {
		for ev := range client.Events() {
			if name, val, ok := ev.PropertyFloat(); ok {
				if name == "time-pos" {
					fmt.Printf("\rPosición: %.1fs", val)
				}
			}
		}
	}()

	// Esperar a que mpv termine o sea interrumpido
	_ = server.Wait()
	fmt.Println("\nReproducción finalizada.")
}
