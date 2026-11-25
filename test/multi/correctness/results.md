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
