# Single-node benchmark

2026-11-25 17:31 · darwin/arm64, 10 CPUs, go1.27.3 · 3s per throughput case

One `cmd/server` node over TCP; every write is an fsync'd Raft-log append. Latencies are client-side.

## Summary

| metric | result |
|---|---|
| Write, 1 conn (per-op fsync cost) | 213 ops/s, p50 / p99 4.03 / 10.1 ms |
| Write, 64 conns (concurrency scaling) | 224 ops/s, p50 / p99 302 / 463 ms |
| Write bandwidth, 64 KiB values | 12.3 MiB/s |
| Read, 32 conns, no writers | 183547 ops/s, p50 / p99 0.17 / 0.34 ms |
| Read, 32 conns, 16 writers active | 195967 ops/s, p50 / p99 0.16 / 0.32 ms |
| Mixed 95/5, 32 conns | 4538 ops/s |
| Restart -> ready, 1000000-record WAL | 1933 ms, RSS 462 MiB |

## Startup vs WAL length

Median of 3 restarts. `listen` = WAL replayed, port open. `ready` = first write acked (adds the 150-300 ms election + re-apply). Churned log: live keys << records.

| WAL records | WAL MiB | live keys | listen ms | ready ms | RSS MiB |
|---|---|---|---|---|---|
| 0 | 0.0 | 0 | 10.8 | 233 | 9 |
| 100000 | 16.5 | 100000 | 152 | 364 | 63 |
| 1000000 | 155.0 | 91008 | 1457 | 1933 | 462 |

## Writes (SET/DEL 95/5)

Synchronous clients, one request in flight per connection. MiB/s = SET value bytes.

| value | conns | ops/s | MiB/s | p50 / p99 ms |
|---|---|---|---|---|
| 128 B | 1 | 213 | 0.02 | 4.03 / 10.1 |
| 128 B | 16 | 231 | 0.03 | 67.9 / 108 |
| 128 B | 64 | 224 | 0.03 | 302 / 463 |
| 4 KiB | 16 | 212 | 0.79 | 76.2 / 93.2 |
| 64 KiB | 16 | 204 | 12.26 | 77.2 / 124 |

## Reads under write load (128 B, 32 reader conns)

Readers only GET, writers only SET/DEL, separate connections, same keyspace, at the same time.

| writer conns | reads/s | read p50 / p99 ms | writes/s | write p50 / p99 ms |
|---|---|---|---|---|
| 0 | 183547 | 0.17 / 0.34 | - | - |
| 1 | 197414 | 0.16 / 0.33 | 237 | 3.98 / 7.50 |
| 16 | 195967 | 0.16 / 0.32 | 248 | 65.0 / 97.9 |

## Mixed (128 B, 32 conns, GET + SET/DEL on each conn)

Closed loop: a connection's next GET waits for its previous write, so write latency caps total ops/s.

| read/write | total ops/s | reads/s | writes/s | read p50 / p99 ms | write p50 / p99 ms |
|---|---|---|---|---|---|
| 95/5 | 4538 | 4317 | 221 | 0.13 / 0.65 | 147 / 242 |
| 50/50 | 460 | 230 | 229 | 0.19 / 4.29 | 141 / 210 |

