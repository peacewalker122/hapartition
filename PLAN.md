# Plan: Redis-Compatible TCP Server (RESP Protocol)

## Summary

Build a minimal Redis-compatible TCP server in Go that speaks the **RESP** (Redis Serialization Protocol) wire format. It accepts connections from standard `redis-cli`, maintains an in-memory key-value store, implements `SET`, `GET`, and `DEL` commands with concurrent-safety, and tracks cluster node membership with `NODE.JOIN`, `NODE.LIST`, `NODE.PING`, and `NODE.LEAVE` commands. Also exposes an HTTP management server (stdlib `net/http`) on a separate port with `POST /join`, `POST /heartbeat`, and `GET /info` endpoints that share the same membership state. Includes a consistent hashring (`hashring.Hashring` interface) with virtual nodes and xxHash that automatically assigns keys to nodes; `SET`/`GET`/`DEL` return `MOVED` errors when a key belongs to a different node.

---

## Acceptance criteria

- [x] Server listens on a TCP port (default 6379) and accepts connections from `redis-cli`.
- [x] RESP protocol parser handles all basic wire types: Simple String, Error, Integer, Bulk String, Array.
- [x] `SET key value` stores a value — responds `+OK\r\n`.
- [x] `GET key` retrieves a value — responds `$-1\r\n` (nil bulk string) if missing, or the stored value.
- [x] `DEL key [key ...]` deletes one or more keys — responds `:N\r\n` (integer count of deleted keys).
- [x] Multiple concurrent `redis-cli` connections work without data races.
- [x] `redis-cli PING` responds with `+PONG\r\n`.
- [x] Invalid commands return a RESP error reply.
- [x] Server shuts down gracefully on SIGINT/SIGTERM.
- [x] `NODE.JOIN <nodeid> <address>` registers a peer with `healthy` status — responds `+OK\r\n`.
- [x] `NODE.LIST` returns all known peers as an array of `[nodeid, address, status, last_seen]`.
- [x] `NODE.PING <nodeid>` refreshes last_seen and resets status to `healthy`.
- [x] `NODE.LEAVE <nodeid>` removes a peer — responds `:N\r\n` (1 if removed, 0 if unknown).
- [x] Node ID is the hostname (auto-detected via `os.Hostname`).
- [x] Peer tracking is thread-safe under concurrent access.
- [x] HTTP management server runs on a configurable port (default 8080).
- [x] `POST /join` registers a peer via JSON body `{"node_id": ..., "address": ...}`.
- [x] `POST /heartbeat` updates last_seen via `{"node_id": ...}`.
- [x] `GET /info` returns JSON with `node_id` and `peers` array.
- [x] HTTP and Redis servers share the same membership state (nodes added via one are visible via the other).
- [x] Both servers shut down gracefully on SIGINT/SIGTERM.
- [x] Consistent hashring maps keys to nodes using xxHash with configurable virtual replicas (default 256).
- [x] Hashring is abstracted behind a `Hashring` interface for swap-in of different algorithms.
- [x] `NODE.JOIN` automatically adds the node to the hashring; `NODE.LEAVE` removes it.
- [x] `SET`/`GET`/`DEL` check the hashring — returns `-MOVED <address>` if the key belongs to a different node.
- [x] Keys owned by this node are stored/retrieved locally as before.

---

## Files to create or modify

| File | Action | Purpose |
|------|--------|---------|
| `go.mod` | modify | Add `github.com/cespare/xxhash/v2` dependency |
| `cmd/hapartition/main.go` | create | Entry point: parse flags, start server, handle signals |
| `pkg/api/types.go` | create | RESP data types (SimpleString, Error, Integer, BulkString, Array) |
| `pkg/api/reader.go` | create | RESP wire protocol parser — reads from `io.Reader` |
| `pkg/api/writer.go` | create | RESP wire protocol writer — writes values to `io.Writer` |
| `pkg/store/store.go` | create | Concurrent-safe in-memory key-value store (`map[string]string` + `sync.RWMutex`) |
| `internal/server/server.go` | create | TCP server: accept loop, per-connection goroutines, command dispatch |
| `internal/membership/membership.go` | create | Cluster node membership: peer tracking with status (healthy/suspected/dead) |
| `internal/mgmt/mgmt.go` | create | HTTP management server: `POST /join`, `POST /heartbeat`, `GET /info` |
| `internal/hashring/hashring.go` | create | `Hashring` interface + consistent hashring implementation with virtual nodes and xxHash |
| `.gitignore` | create | Go standard gitignore |

---

## Task list

### Phase 1: RESP Protocol Layer

- [x] **Task 1.1 — RESP type definitions** (`resp/types.go`)
  Define Go types that represent each RESP wire type:
  - `SimpleString` / `Error` / `Integer` / `BulkString` / `Array`
  - A `Value` interface or sum type for unified handling.

- [x] **Task 1.2 — RESP reader** (`resp/reader.go`)
  Parse RESP protocol from a `bufio.Reader`:
  - `+OK\r\n` → SimpleString
  - `-ERR ...\r\n` → Error
  - `:1\r\n` → Integer
  - `$5\r\nhello\r\n` → BulkString
  - `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n` → Array
  - Also handle inline commands (space-separated, `\r\n` terminated) for simple redis-cli input.

- [x] **Task 1.3 — RESP writer** (`resp/writer.go`)
  Write any RESP value back to an `io.Writer`:
  - `Flush()` method to ensure buffered writes are sent.

### Phase 2: In-Memory Store

- [x] **Task 2.1 — Thread-safe store** (`store/store.go`)
  - `map[string]string` guarded by `sync.RWMutex`.
  - Methods: `Set(key, value string)`, `Get(key string) (string, bool)`, `Del(keys ...string) int`.
  - `Get` returns a `(value, ok)` pair — callers use it to produce nil bulk string for missing keys.

### Phase 3: TCP Server

- [x] **Task 3.1 — Command dispatch** (`server/server.go`)
  Parse the RESP array from the client, identify the command name (case-insensitive: `SET`, `GET`, `DEL`, `PING`), validate argument count, execute against the store, and write the RESP response.
  - `PING` → `+PONG\r\n`
  - `SET key value` → `+OK\r\n`
  - `GET key` → `$<len>\r\n<value>\r\n` or `$-1\r\n`
  - `DEL key [key ...]` → `:<count>\r\n`
  - Unknown command → `-ERR unknown command '<cmd>'\r\n`
  - Wrong arity → `-ERR wrong number of arguments for '<cmd>' command\r\n`

- [x] **Task 3.2 — Connection handler** (`server/server.go`)
  For each accepted TCP connection:
  - Spawn a goroutine.
  - Loop: read RESP command, execute, write response.
  - Close connection on read error or client disconnect.

- [x] **Task 3.3 — Server lifecycle** (`server/server.go` + `main.go`)
  - `NewServer(addr string) *Server` → `ListenAndServe()` → `Shutdown(ctx)`.
  - `main.go`: parse `--port` flag (default 6379), create server, listen for SIGINT/SIGTERM, graceful shutdown.

### Phase 4: Integration & Polish

- [x] **Task 4.1 — Test with redis-cli**
  - Start server, connect with `redis-cli -p 6379`.
  - Verify: `PING`, `SET`, `GET`, `GET` on missing key, `DEL`, `DEL` on missing key, garbage input.
  - Run multiple concurrent `redis-cli` sessions.

- [x] **Task 4.2 — Unit tests**
  - `resp/reader_test.go`: test parsing of every RESP type, inline commands, edge cases (empty bulk string, null bulk string).
  - `resp/writer_test.go`: test round-trip write-then-read.
  - `store/store_test.go`: test concurrent Set/Get/Del with `go test -race`.
  - `server/server_test.go`: connect a raw TCP client, send RESP commands, verify responses.

### Phase 5: Node Membership

- [x] **Task 5.1 — Membership package** (`membership/membership.go`)
  - Thread-safe peer tracking with `map[string]*Peer + sync.RWMutex`.
  - Each `Peer` has `NodeID`, `Address`, `Status` (healthy/suspected/dead), `LastSeen`.
  - Methods: `Join`, `Ping`, `Suspect`, `MarkDead`, `Remove`, `Get`, `Peers`, `Count`.

- [x] **Task 5.2 — Wire membership into server** (`server/server.go`)
  - Server constructor accepts `nodeID`, creates a `Membership` instance.
  - New commands: `NODE.JOIN`, `NODE.LIST`, `NODE.PING`, `NODE.LEAVE`.
  - Logger includes node ID in startup message.

- [x] **Task 5.3 — Auto-detect node ID** (`main.go`)
  - `os.Hostname()` called on startup, passed as node ID to server constructor.

- [x] **Task 5.4 — Membership tests** (`membership/membership_test.go`)
  - Table-driven tests for status transitions, join/ping/remove, concurrency with `-race`.

- [x] **Task 5.5 — Server node command tests** (`server/server_test.go`)
  - Integration tests for `NODE.JOIN`, `NODE.LIST`, `NODE.PING`, `NODE.LEAVE`, error cases.

### Phase 6: HTTP Management Server

- [x] **Task 6.1 — HTTP management server** (`mgmt/mgmt.go`)
  - Stdlib `net/http` server on a separate port (default 8080).
  - `POST /join` — accepts `{"node_id", "address"}`, calls `membership.Join()`.
  - `POST /heartbeat` — accepts `{"node_id"}`, calls `membership.Ping()`.
  - `GET /info` — returns JSON with `node_id` and `peers` array.
  - Shares the same `*membership.Membership` instance as the Redis TCP server.

- [x] **Task 6.2 — Wire into main** (`main.go`)
  - Add `--http` flag for HTTP management port.
  - Create `mgmt.Server` with the Redis server's membership.
  - Graceful shutdown of both servers on SIGINT/SIGTERM.

- [x] **Task 6.3 — mgmt server tests** (`mgmt/mgmt_test.go`)
  - Tests for join, heartbeat, info endpoints, error cases.

### Phase 7: Consistent Hashring

- [x] **Task 7.1 — Hashring interface + implementation** (`hashring/hashring.go`)
  - `Hashring` interface: `AddNode`, `RemoveNode`, `GetNode`, `Nodes`.
  - Consistent hashring with sorted ring entries and binary search lookup.
  - Virtual replicas per physical node (default 256, configurable per `AddNode`).
  - xxHash (`github.com/cespare/xxhash/v2`) for hashing.
  - Thread-safe (`sync.RWMutex`).

- [x] **Task 7.2 — Hashring tests** (`hashring/hashring_test.go`)
  - Tests: add/remove nodes, key lookup, empty ring, distribution balance, determinism, concurrent access.

- [x] **Task 7.3 — Wire hashring into server** (`server/server.go`)
  - `server.New` creates a `Hashring` instance and adds the local node.
  - `handleKeyCommand` checks `ring.GetNode(key)` — if owner != local nodeID, returns `-MOVED <address>`.
  - `NODE.JOIN` dispatch also calls `ring.AddNode` (eager sync).
  - `NODE.LEAVE` dispatch also calls `ring.RemoveNode` (eager sync).
  - Expose `Ring()` method for mgmt/test access.

- [x] **Task 7.4 — MOVED redirection tests** (`server/server_test.go`)
  - `TestSetReturnsMovedForRemoteKey` — SET returns MOVED when key belongs to remote node.
  - `TestGetReturnsMovedForRemoteKey` — GET returns MOVED for remote-owned key.
  - `TestSetStoresLocallyWhenOwner` — SET succeeds when key belongs to local node.
  - `TestNodeJoinUpdatesRing` — NODE.JOIN also updates the ring.
  - `TestNodeLeaveUpdatesRing` — NODE.LEAVE also updates the ring.

- [x] **Task 7.5 — Manual verification with redis-cli**
  - Verified single-node SET/GET works normally.
  - After NODE.JOIN, some keys get MOVED (owned by remote), some OK (owned locally).
  - After NODE.LEAVE, all keys go back to local.

---

## Test strategy

| Layer | Approach | Tools |
|-------|----------|-------|
| **RESP parse** | Table-driven tests with raw byte inputs and expected parsed values | `testing`, `testify/assert` (optional) |
| **Store concurrency** | N goroutines hammering Set/Get/Del with `-race` flag | `go test -race` |
| **Integration** | Start server on a random port, connect with `net.Dial`, send RESP bytes, assert responses | `testing`, `net` package |
| **Manual** | Run server on 6379, connect with `redis-cli`, verify interactive session | `redis-cli` |

---

## Out of scope

- Persistence / AOF / RDB snapshots
- Authentication (`AUTH`)
- Pub/Sub, transactions (`MULTI/EXEC`), Lua scripting
- Data structures beyond strings (lists, sets, sorted sets, hashes)
- Key expiry (`EXPIRE`, `TTL`)
- Replication / cluster mode
- TLS support
- Performance benchmarking or optimization
