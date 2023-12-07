package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/blueambertech/ETL/api"
)

var shutdownChannel chan os.Signal = make(chan os.Signal, 1)

func main() {
	server := createServer()
	api.RegisterHandlers()
	go listenForRequests(server)
	waitForShutdown(server)
}

func createServer() *http.Server {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	return &http.Server{
		Addr: ":" + port,
	}
}

func listenForRequests(s *http.Server) {
	log.Println("Service started")
	if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Println("HTTP server error:", err)
		shutdownChannel <- syscall.SIGKILL
	}
	log.Println("Stopped serving new connections")
}

func waitForShutdown(server *http.Server) {
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)
	<-shutdownChannel

	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownRelease()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Service shutdown error: %v", err)
	}
	log.Println("Service shutdown complete")
}
