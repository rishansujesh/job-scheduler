package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rishansujesh/job-scheduler/internal/api/proto"
	redisx "github.com/rishansujesh/job-scheduler/internal/redis"
)

// StartServers starts gRPC on grpcAddr and REST gateway on httpAddr.
func StartServers(db *sql.DB, rdb *redis.Client, httpAddr, grpcAddr string) error {
	// gRPC server (in-process)
	grpcServer := grpc.NewServer()
	js := New(db, rdb, redisx.StreamsFromEnv())
	proto.RegisterJobServiceServer(grpcServer, js)

	// REST gateway connects to the in-process gRPC via local dial
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := proto.RegisterJobServiceHandlerFromEndpoint(ctx, mux, grpcAddr, opts); err != nil {
		return err
	}

	// Health & readiness served on the same HTTP mux
	httpMux := http.NewServeMux()
	httpMux.Handle("/v1/", mux)
	httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "service": "api"})
	})
	httpMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Start gRPC server
	go func() {
		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		log.Printf("gRPC listening on %s", grpcAddr)
		if err := grpcServer.Serve(l); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	// Start HTTP server
	s := &http.Server{
		Addr:              httpAddr,
		Handler:           httpMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("REST listening on %s", httpAddr)
	return s.ListenAndServe()
}
