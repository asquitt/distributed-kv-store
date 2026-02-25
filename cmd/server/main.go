package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/asquitt/distributed-kv-store/pkg/api"
	"github.com/asquitt/distributed-kv-store/pkg/raft"
	"github.com/asquitt/distributed-kv-store/pkg/storage"
)

func main() {
	id := flag.String("id", "", "Node ID")
	addr := flag.String("addr", "localhost:8080", "Listen address (host:port)")
	peers := flag.String("peers", "", "Comma-separated list of peer addresses (host:port)")
	flag.Parse()

	if *id == "" {
		fmt.Fprintln(os.Stderr, "error: -id is required")
		flag.Usage()
		os.Exit(1)
	}

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	store := storage.NewStore()
	applyCh := make(chan raft.ApplyMsg, 256)

	node := raft.NewNode(*id, peerList, nil, applyCh) // transport set below
	transport := raft.NewHTTPTransport(node)
	node.SetTransport(transport)

	apiServer := api.NewServer(store, node)

	// Combine Raft transport and API handlers on a single HTTP server
	mux := http.NewServeMux()
	mux.Handle("/raft/", transport.Handler())
	mux.Handle("/kv/", apiServer.Handler())
	mux.Handle("/health", apiServer.Handler())

	// Apply committed entries to the store
	go func() {
		for msg := range applyCh {
			switch msg.Command.Op {
			case "set":
				if err := store.Set(msg.Command.Key, msg.Command.Value); err != nil {
					log.Printf("apply set error: %v", err)
				}
			case "delete":
				if err := store.Delete(msg.Command.Key); err != nil {
					log.Printf("apply delete error: %v", err)
				}
			}
			log.Printf("applied entry %d: %s %s", msg.Index, msg.Command.Op, msg.Command.Key)
		}
	}()

	node.Start()
	log.Printf("Node %s listening on %s (peers: %v)", *id, *addr, peerList)

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		node.Stop()
		os.Exit(0)
	}()

	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
