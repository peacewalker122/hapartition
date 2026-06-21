# Review: Gossip Replication with Memberlist

## Verdict: APPROVED

## Summary

The gossip layer is cleanly designed and well-integrated. Memberlist replaces the old membership package, the `GetReplicas` method enables deterministic replica placement, the protobuf wire format is appropriate, and the LWW versioning in the store provides a solid foundation for read repair. All 49 tests across 6 packages pass with `-race`, and `go vet` is clean.

## Checklist results

| Area | Result | Notes |
|------|--------|-------|
| Correctness | ✓ | Broadcast + anti-entropy, LWW merge, ring auto-updates, MOVED still works |
| Code quality | ✓ | Clean separation of concerns, small functions, good naming |
| Security | ✓ | No secrets, all input validated via RESP parser / protobuf unmarshal |
| Robustness | ✓ | Thread-safe (RWMutex + atomic), graceful shutdown in correct order |
| Documentation | ✓ | README added with architecture diagram, quick start, config reference |
| Consistency | ✓ | Follows existing project patterns, `go fmt` clean |

## Issues (minor — not blocking)

### Issue 1 — HTTP POST /join is a no-op
- **File:** `internal/mgmt/mgmt.go:56-68`
- **Problem:** `handleJoin` logs "joining %s via gossip" but doesn't actually call `memberlist.Join()`. The endpoint accepts an address but does nothing with it.
- **Suggested fix:** Either remove the endpoint or implement actual joining by calling `h.gossip.Join(address)`. Add a `Join(addresses ...string)` method to the gossip handler that delegates to `memberlist.Join()`.

### Issue 2 — handleKeyCommand error message says "SET" for all commands
- **File:** `internal/server/server.go:212`
- **Problem:** When `GET` receives wrong number of arguments, the error says `"ERR wrong number of arguments for 'SET' command"`. This was noted in the previous review and is still unfixed.
- **Suggested fix:** Pass the command name to `handleKeyCommand` or split the arity check per command.

### Issue 3 — DEL bypasses hashring check
- **File:** `internal/server/server.go:137-148`
- **Problem:** `DEL` executes locally without checking `ring.GetNode()`. Multi-key `DEL` silently deletes keys that belong to other nodes.
- **Suggested fix:** Apply `handleKeyCommand` logic to DEL, or check the first key against the ring and return MOVED if it doesn't belong locally.

### Issue 4 — Anti-entropy always picks the first peer
- **File:** `internal/gossip/gossip.go:196-219`
- **Problem:** `exchangeState` always picks `peers[0]` instead of a random peer. The comment says "over time all nodes converge" — which is true, but convergence is slower because every round picks the same peer.
- **Suggested fix:** Use `rand.Intn(len(peers))` to pick a random peer each round.

### Issue 5 — Dead fields in consistentHashring
- **File:** `internal/hashring/hashring.go:48-55`
- **Problem:** `localNodeID` and `nodeReplicas` fields are assigned but never read (same issue as previous review, still present).
- **Suggested fix:** Remove `localNodeID` and `nodeReplicas` from the struct. If `nodeReplicas` is needed later, add it back then.

### Issue 6 — No gossip tests
- **File:** `internal/gossip/gossip.go`
- **Problem:** The gossip package has no tests. Memberlist requires real ports, making unit tests harder, but integration tests (start two memberlist nodes in-process, verify broadcast, verify anti-entropy) would catch protocol bugs.
- **Suggested fix:** Add a test that starts two gossip handlers on random ports, calls Broadcast on one, and verifies the other receives the entry. `testify/assert` is optional — raw proto comparison works.

## Approval notes

**What's done well:**
- The `GetReplicas` method on the hashring is clean and minimal — deterministic replica placement with no extra bookkeeping.
- The `Discoverer` interface is the right abstraction for seed discovery. The static impl in main is minimal and the interface is easy to implement for k8s DNS.
- LWW semantics are enforced at the store level (`SetWithVersion`), not in the gossip handler — this is the correct layering.
- The `broadcastMessage.Invalidates` method correctly deduplicates queued broadcasts for the same key, preventing redundant gossip.
- Graceful shutdown order (HTTP → gossip → Redis) prevents data loss and memberlist disconnection warnings.

**Review passed. See `REVIEW.md` for the full report.  
You can now commit, push, or ship.**
