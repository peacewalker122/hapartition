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
┌──────────────────────────────────────────────────────────────────┐
│  redis-cli  │  curl /info  │  (other nodes) gossiping            │
└──────┬───────────┬──────────────────────┬───────────────────────┘
       │           │                      │
       │ TCP:6379  │ HTTP:8080            │ memberlist (TCP+UDP:7946)
       ▼           ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     cmd/hapartition/main.go                         │
│  parses flags → wires modules → handles SIGINT/SIGTERM shutdown     │
│  ┌──────────────┐  ┌────────────┐  ┌───────────────────────────┐  │
│  │internal/     │  │internal/   │  │internal/                  │  │
│  │server        │  │gossip      │  │mgmt                      │  │
│  │              │  │            │  │                           │  │
│  │ RESP parser  │  │ memberlist │  │ GET /        dashboard   │  │
│  │ dispatch     │◄─┤ broadcast  │  │ GET /info    JSON members │  │
│  │ hashring     │  │ anti-entropy│ │ GET /ring    hash viz    │  │
│  │ MOVED        │  │ mTLS        │  │ GET /events  event log   │  │
│  │              │  │            │  │ GET /events/  SSE stream │  │
│  │              │  │            │  │   stream                 │  │
│  │              │  │            │  │ POST /join   seed addr   │  │
│  └──────┬───────┘  └──────┬─────┘  └───────────────────────────┘  │
│         │                 │                                        │
│         ▼                 ▼                                        │
│  ┌──────────────────────────────────────────────────────────┐     │
│  │ pkg/store                                                │     │
│  │  in-memory KV with monotonic versioning, LWW merge       │     │
│  │  Set(key, val) → assigns version                         │     │
│  │  SetWithVersion(key, val, ver) → LWW compare             │     │
│  │  Snapshot() → full dump for anti-entropy exchange        │     │
│  └──────────────────────────────────────────────────────────┘     │
│         ▲                                                        │
│         │                                                        │
│  ┌──────┴───────┐                                                │
│  │ pkg/api      │                                                │
│  │  RESP reader │                                                │
│  │  RESP writer │                                                │
│  └──────────────┘                                                │
└─────────────────────────────────────────────────────────────────────┘

│  internal/hashring                │
│  consistent hash ring (Ketama)    │
│  xxHash, 256 vnodes per node      │
│  GetNode(key) → owner             │
│  GetReplicas(key, n) → n replicas │
│  GetSlotRanges() → CLUSTER SLOTS  │
│  RingSnapshot() → /ring API       │
└───────────────────────────────────┘
```

## Module responsibilities

### `cmd/hapartition/main.go` — Entry point

Parses CLI flags, wires all modules together, loads TLS config, and handles
`SIGINT`/`SIGTERM` orchestrated shutdown (HTTP first → gossip leave → Redis).

### `internal/server` — Redis-compatible TCP server

Listens on the Redis port, parses RESP protocol via `pkg/api`, dispatches
commands:
- **Key commands** (`SET`/`GET`/`DEL`) → hash ring lookup → local store or
  `MOVED` redirection
- **Cluster commands** (`CLUSTER SLOTS`/`NODES`/`INFO`/`KEYSLOT`) → ring
  introspection for cluster-aware Redis clients
- **Node commands** (`NODE.LIST`/`NODE.JOIN`) — direct cluster management

### `internal/gossip` — Memberlist wrapper

SWIM gossip via HashiCorp Memberlist. Handles:
- **Broadcast** — when a key is written, serializes `(key, value, version)` as
  protobuf and fan-out to all cluster members
- **Replication** — on receive, checks `hashring.GetReplicas()` to decide if
  this node is a replica; applies LWW merge via `store.SetWithVersion()`
- **Anti-entropy** — every 30s picks a random peer, exchanges full store
  snapshot, merges with LWW. Catches writes missed during partitions
- **Membership** — `NotifyJoin`/`NotifyLeave` callbacks update the hash ring
- **mTLS** — optional TLS transport for gossip traffic with peer verification

### `internal/mgmt` — HTTP management server

Serves alongside the Redis port on a separate listener:
- `GET /` — embedded dashboard HTML (`dashboard.html`) with hash ring
  visualization, live event log via SSE
- `GET /info` — JSON node info, members, key count
- `GET /ring` — hash ring snapshot (hex hashes per virtual node)
- `GET /events` — recent cluster events as JSON
- `GET /events/stream` — Server-Sent Events stream for real-time UI updates
- `POST /join` — request to join a gossip seed address

### `internal/hashring` — Consistent hash ring

Ketama-style ring with 256 virtual nodes per physical node and xxHash.
Binary search lookup for O(log N) key → node mapping. Supports:
- `GetNode(key)` — owner of a key
- `GetReplicas(key, n)` — next n distinct nodes clockwise (for replication)
- `GetSlotRanges()` — slot ranges per node for `CLUSTER SLOTS`
- `RingSnapshot()` — full sorted ring for dashboard visualization

### `pkg/store` — Versioned KV store

In-memory key-value store with:
- `Set(key, value)` — assigns a monotonic version counter
- `SetWithVersion(key, value, version)` — LWW merge: rejects if stored version
  ≥ incoming version
- `Snapshot()` — full dump for anti-entropy exchange
- `Del(key)` — local deletion (not replicated)

### `pkg/api` — RESP protocol

Reader and writer for the Redis Serialization Protocol:
- `Reader` — parses `+OK`, `-ERR`, `:42`, `$6\r\nfoobar\r\n`, `*2\r\n...`,
  and inline commands
- `Writer` — serialises RESP values to the wire

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
| `--tls-cert` | `""` | TLS certificate file for mTLS gossip |
| `--tls-key` | `""` | TLS private key file for mTLS gossip |
| `--tls-ca` | `""` | CA certificate file for verifying peer certs (falls back to system pool) |
| `--tls-insecure` | `false` | Skip peer certificate verification (self-signed dev certs) |

### TLS / mTLS

When `--tls-cert` and `--tls-key` are set, gossip traffic uses mutual TLS. Each
node presents its certificate; peers verify against the CA in `--tls-ca`. If
`--tls-insecure` is set, peer verification is skipped (useful with self-signed
certs in development).

The node cert's SAN must include a `ServerName` value of `"hapartition"` — the
cluster sets this on every peer connection so hostname verification passes even
when connecting by IP.

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

## Kubernetes deployment

The `deploy/k3s/` directory contains manifests for running on k3s or any
Kubernetes cluster with cert-manager:

| File | Purpose |
|------|---------|
| `namespace.yaml` | `hapartition` namespace |
| `service.yaml` | Headless gossip service + NodePort Redis/HTTP |
| `ca-bootstrap.yaml` | SelfSigned issuer + CA certificate bootstrap |
| `certs.yaml` | Per-node TLS certificates (cert-manager) |
| `deployment.yaml` | StatefulSet with per-node cert mounts |

Apply in order:

```bash
kubectl apply -f deploy/k3s/namespace.yaml
kubectl apply -f deploy/k3s/ca-bootstrap.yaml
# wait for ClusterIssuer/internal-ca to become Ready
kubectl wait --for=condition=Ready clusterissuer/internal-ca --timeout=60s
kubectl apply -f deploy/k3s/certs.yaml
kubectl apply -f deploy/k3s/service.yaml
kubectl apply -f deploy/k3s/deployment.yaml
```

Each pod gets its own TLS certificate from cert-manager, mounted at
`/etc/tls/<pod-name>/`. Gossip traffic uses mTLS with `--tls-insecure` for
self-signed CA (see [TLS / mTLS](#tls--mtls)).

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

### HTTP management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Cluster dashboard (HTML with hashring viz, SSE event log) |
| `/info` | GET | Node ID, key count, member list |
| `/ring` | GET | Hash ring snapshot (hex hashes, node assignments) |
| `/events` | GET | Recent cluster events (join, leave, key changes) |
| `/events/stream` | GET | Server-sent events (real-time cluster updates) |
| `/join` | POST | Join a gossip seed (`{"address":"host:port"}`) |

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
| `INFO` | ✓ | Redis-compatible server info |
| `CLUSTER SLOTS` | ✓ | Slot-to-node mapping for cluster-aware clients |
| `CLUSTER NODES` | ✓ | Node list with IDs and addresses |
| `CLUSTER INFO` | ✓ | Cluster state summary |
| `CLUSTER KEYSLOT key` | ✓ | Hash slot for a key |
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

All tests pass under `-race`. Integration tests use [Testcontainers](https://testcontainers.com/) to spin up a real Redis instance for cross-validation. Benchmarks cover single-node and cluster workloads.

```bash
# unit + integration + race
go test -race -count=1 ./...

# benchmarks
go test -bench=. -benchmem ./...
```

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
