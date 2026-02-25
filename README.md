# Distributed Key-Value Store

A distributed key-value store implementing Raft consensus, built from scratch in Go.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node A    │────▶│   Node B    │────▶│   Node C    │
│  (Leader)   │◀────│ (Follower)  │◀────│ (Follower)  │
└──────┬──────┘     └─────────────┘     └─────────────┘
       │
   Client API
   PUT/GET/DELETE
```

### Components

- **Raft Consensus** (`pkg/raft/`) — Leader election, log replication, and commit tracking
- **Storage Engine** (`pkg/storage/`) — Thread-safe in-memory key-value store
- **HTTP Transport** (`pkg/raft/http_transport.go`) — JSON-over-HTTP RPCs for inter-node communication
- **Client API** (`pkg/api/`) — RESTful HTTP API for key-value operations
- **Server** (`cmd/server/`) — Entry point that wires everything together

## Getting Started

### Build

```bash
go build ./cmd/server
```

### Run a 3-node cluster

```bash
# Terminal 1
./server -id node1 -addr localhost:8001 -peers localhost:8002,localhost:8003

# Terminal 2
./server -id node2 -addr localhost:8002 -peers localhost:8001,localhost:8003

# Terminal 3
./server -id node3 -addr localhost:8003 -peers localhost:8001,localhost:8002
```

### Client operations

```bash
# Write a key (must hit the leader)
curl -X PUT localhost:8001/kv/mykey -d 'myvalue'

# Read a key (any node)
curl localhost:8001/kv/mykey

# Delete a key (must hit the leader)
curl -X DELETE localhost:8001/kv/mykey

# Health check
curl localhost:8001/health
```

## Testing

```bash
go test ./... -v
```

## Tech Stack

- Go (standard library only, zero external dependencies)
- Raft consensus protocol (from-scratch implementation)
- HTTP/JSON for inter-node RPCs
