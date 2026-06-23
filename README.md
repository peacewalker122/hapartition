# Hapartition

A Redis-compatible TCP server with consistent hash sharding and gossip-based replication. Built for learning distributed systems — hash rings, SWIM gossip, LWW conflict resolution, and read repair.

```
redis-cli -p 6379 SET mykey myvalue
redis-cli -p 6379 GET mykey
```

## Features

- **Redis wire protocol** — `SET`, `GET`, `DEL`, `PING` via RESP (Redis Serialization Protocol)
- **Consistent hash sharding** — Ketama-style ring with xxHash, virtual nodes, and `MOVED` redirection
- **Gossip membership** — SWIM protocol via HashiCorp Memberlist for automatic node discovery, failure detection, and cluster membership
- **Async replication** — key writes broadcast to all nodes via gossip; each node stores only the replicas it owns
- **Last-writer-wins (LWW)** — every write carries a monotonic version; stale writes are silently rejected
- **Anti-entropy** — periodic full-state sync between random peers for convergence after partitions
- **Protobuf wire format** — gossip messages encoded with Protocol Buffers
- **HTTP management** — `GET /info`, `POST /join` for cluster introspection
- **Pluggable discovery** — `Discoverer` interface for seed node resolution (static list, DNS, k8s headless service)

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  redis-cli -p 6379 SET key value                     │
└──────────────┬──────────────────────────────────────┘
               │ RESP over TCP
               ▼
┌─────────────────────────────┐
│  internal/server (TCP)      │
│  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
│  1. hashring.GetNode(key)   │
│  2. MOVED if remote         │
│  3. store.Set(key, value)   │← assigns monotonic version
│  4. gossip.Broadcast()      │← (key, value, version) via protobuf
└──────────────┬──────────────┘
               │ memberlist gossip
               ▼
┌─────────────────────────────┐
│  internal/gossip            │
│  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
│  Broadcast → every node     │
│    → check GetReplicas()    │← "am I a replica?"
│    → store.SetWithVersion() │← LWW merge
│                             │
│  Anti-entropy (30s)         │
│    → pick random peer       │
│    → exchange StoreSnapshot │
│    → merge with LWW         │
└─────────────────────────────┘

┌─────────────────────────────┐
│  HTTP /info via net/http    │
│  returns cluster members    │
│  with Redis & gossip addrs  │
└─────────────────────────────┘
```

## Quick start

### Single node

```bash
go build -o hapartition ./cmd/hapartition/
./hapartition --port 6379 --http 8080
```

```bash
redis-cli -p 6379 PING
# → PONG

redis-cli -p 6379 SET hello world
# → OK

redis-cli -p 6379 GET hello
# → world

redis-cli -p 6379 DEL hello
# → (integer) 1

curl -s http://localhost:8080/info | jq .
```

### Multi-node cluster

Start three nodes (each in its own terminal or tmux pane). **Important:** on the same machine, each node needs a unique `--node-id` — `os.Hostname()` is identical for all processes, which breaks memberlist and the hashring.

```bash
# Terminal 1 — seed node
./hapartition --node-id node-a --port 6379 --http 8080 --gossip-port 7946

# Terminal 2 — joins node 1
./hapartition --node-id node-b --port 6380 --http 8081 --gossip-port 7947 \
  --join 127.0.0.1:7946

# Terminal 3 — joins node 1
./hapartition --node-id node-c --port 6381 --http 8082 --gossip-port 7948 \
  --join 127.0.0.1:7946
```

Now keys are distributed across nodes. A `SET` on the wrong node returns `MOVED`:

```bash
redis-cli -p 6379 SET mykey value
# → OK  (key owned by node 6379)

redis-cli -p 6380 SET mykey value
# → MOVED 127.0.0.1:6379  (redirect to owner)
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | `os.Hostname()` | Unique node ID (required when running multiple nodes on the same machine) |
| `--port` | `6379` | Redis-compatible TCP port |
| `--http` | `8080` | HTTP management port (GET /info, POST /join) |
| `--gossip-port` | `7946` | Memberlist gossip port (TCP+UDP) |
| `--join` | `""` | Comma-separated gossip seed addresses (`host:port`) |
| `--rf` | `2` | Replication factor (number of replicas per key) |

### Discovery abstractions

For k3s or Kubernetes, implement the `Discoverer` interface:

```go
type Discoverer interface {
    Discover() ([]string, error)
}
```

The built-in `--join` flag uses `staticDiscoverer`. For k8s, write a DNS-based discoverer that resolves a headless service:

```go
type DNSDiscoverer struct {
    Service string // e.g. "redis-gossip.default.svc.cluster.local"
    Port    int
}

func (d *DNSDiscoverer) Discover() ([]string, error) {
    _, addrs, err := net.LookupHost(d.Service)
    // ... append port and return
}
```

## How it works

### Hash ring

The consistent hash ring (`internal/hashring`) uses 256 virtual nodes per physical node (`Ketama`-style). Key lookup is O(log N) via binary search on the sorted ring. The `getNode(key)` returns the owning node; `getReplicas(key, n)` returns the next n distinct nodes clockwise for replication.

```
Ring:   [nodeA:0] ─ [nodeB:42] ─ [nodeA:99] ─ [nodeC:150] ─ [nodeB:201] ─ ...
                ↑  key hash lands here → owner = nodeB
```

### Gossip replication

When `SET` is called:
1. The local store writes the value with a monotonic version (global counter)
2. The gossip handler broadcasts `(key, value, version)` to all cluster nodes via memberlist
3. Each receiving node checks `hashring.GetReplicas(key, rf)` — if it's one of the replica nodes, it stores with `SetWithVersion`
4. `SetWithVersion` compares the incoming version against the stored version. If the stored version is >= the incoming version, the write is rejected (LWW)

### Anti-entropy

Every 30 seconds, each node picks a random peer and sends its full store snapshot as an `EntryBatch` (protobuf). The receiving node merges every entry with LWW semantics. This catches any writes missed during a node outage.

### Membership

Memberlist handles all cluster membership:
- **Join** — a new node contacts seed nodes via `--join`
- **Failure detection** — SWIM protocol with suspicion and indirect probing
- **Leave** — graceful shutdown via `SIGINT`/`SIGTERM`
- The hashring updates automatically on `NotifyJoin` and `NotifyLeave` events

## Project structure

```
cmd/hapartition/main.go     Entry point — flags, gossip setup, signal handling
├── internal/
│   ├── gossip/             Memberlist wrapper, broadcast, anti-entropy
│   │   └── pb/             Protobuf definition + generated code
│   ├── hashring/           Consistent hash ring (xxHash, virtual nodes)
│   ├── mgmt/               HTTP management server (GET /info, POST /join)
│   └── server/             Redis-compatible TCP server (RESP handler, dispatch)
├── pkg/
│   ├── api/                RESP protocol types, reader, writer
│   └── store/              In-memory KV store with versioning and LWW
├── go.mod
└── go.sum
```

## Redis commands

| Command | Status | Notes |
|---------|--------|-------|
| `PING` | ✓ | |
| `SET key value` | ✓ | Async replication to cluster |
| `GET key` | ✓ | Returns value or nil |
| `DEL key [key ...]` | ✓ | Local only (no replication) |
| `NODE.JOIN key address` | ✓ | Adds node to hashring (doesn't affect gossip membership — use `--join` for that) |
| `NODE.LIST` | ✓ | Returns memberlist nodes and Redis addresses |
| `NODE.PING` | ✗ | Deprecated — memberlist handles health checks |
| `NODE.LEAVE` | ✗ | Deprecated — use shutdown to leave the cluster |

## Development

```bash
go build ./...
go test -race -count=1 ./...
go vet ./...
```

All tests pass under `-race`.

### Adding a discovery backend

Implement the `gossip.Discoverer` interface and inject it via `Config.Discoverer`:

```go
import "github.com/peacewalker122/hapartition/internal/gossip"

type MyDiscoverer struct {
    // ...
}

func (d *MyDiscoverer) Discover() ([]string, error) {
    // return ["host:port", ...]
}
```

### Regenerating protobuf

```bash
protoc --go_out=. --go_opt=module=github.com/peacewalker122/hapartition \
  internal/gossip/pb/gossip.proto
```

## License

MIT
