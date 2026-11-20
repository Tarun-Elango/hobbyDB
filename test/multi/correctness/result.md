# Correctness, 5-node cluster

2026-11-18 · darwin/arm64 · 5 `cmd/server` processes on localhost · crashes = SIGKILL · stall = SIGSTOP/SIGCONT

```bash
KV_BENCH=1 go test -v -count=1 ./test/multi/correctness/
```

## Results

| check | `read.from=leader` | `read.from=any` |
|---|---|---|
| Linearizable under 2 leader crashes (6 clients, 4 hot keys, ~600 ops) | **PASS** (4/4 keys, 3/3 runs) | not linearizable (4/4 keys, 33 provably stale reads) |
| No lost acked writes (crash ≤2 nodes, 10–14 faults, then whole-cluster crash + restart from WAL) | **PASS** (0 of ~300 lost, 5/5 replicas converge, 3/3 runs) | same |
| Read-your-writes (ack on leader, then GET on each follower) | 0 / 400 stale | 41 / 400 stale (10%) |
| Monotonic reads (random node per read, value keeps rising) | 0 went backwards | 2155 / 7811 went backwards (28%) |
| Stalled old leader (SIGSTOP, new leader commits `new`, GET on old leader, SIGCONT) | **FAIL**: returns `old` (3/3 runs) | returns `old` |

## Takeaways

- **Writes are safe.** No acked write was lost after leader crashes, random crashes, or a full-cluster cold restart (back to a working leader in 0.25–1.2 s). All replicas converge.
- **`any` mode is eventually consistent, not linearizable.** Follower reads lag the leader's commits. Reads that hop between nodes see values go back in time. This is expected and matches the comment in `settings.properties`.
- **Bug: `leader` mode is only linearizable while leaders crash cleanly.** `WaitLeaderRead` ([raft/apply.go](../../../raft/apply.go)) checks only the node's *own* belief that it is leader. A leader that was stalled (GC pause, VM freeze, or partition) wakes up and serves a read before it learns it has been replaced. That read misses writes the new leader already acknowledged.
  - **Fix (Raft §8, ReadIndex):** before answering, the leader must confirm it is still leader by getting a heartbeat round acked by a majority, *after* recording `readIndex`.
  - Cheaper alternative: a leader lease (step down if no majority contact within the election timeout). This assumes bounded clock drift.
- Writes keep a fixed `ClientID/Seq` across retries, so a retried write applies once. This is why the linearizability check can treat a timed-out write as "maybe applied".

## What each test does

- `TestLinearizable`: records every GET/SET with call/return times. Each key's history is checked by a small Wing–Gong linearizability search in `linearizability_test.go` (unit-tested by `TestChecker`). Writes that were never acked count as "maybe happened".
- `TestNoLostAckedWrites`: 8 writers write unique keys during chaos, then a full-cluster crash and restart. Every acked key is read from the leader, then from every node.
- `TestStaleReads`: the read-your-writes, monotonic-reads and stalled-leader scenarios. In `leader` mode any stale read fails the test; in `any` mode the numbers are only reported.
