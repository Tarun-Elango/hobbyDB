# Single-node benchmark

2026-11-17 19:32 · darwin/arm64, 10 CPUs, go1.27.3 · 3s per throughput case

One `cmd/server` node over TCP; every write is an fsync'd Raft-log append. Latencies are client-side.

## Summary

| metric | result |
|---|---|
| Write, 1 conn (per-op fsync cost) | 197 ops/s, p50 / p99 4.92 / 13.0 ms |
| Write, 64 conns (concurrency scaling) | 212 ops/s, p50 / p99 290 / 555 ms |
| Write bandwidth, 64 KiB values | 15.0 MiB/s |
| Read, 32 conns, no writers | 182153 ops/s, p50 / p99 0.16 / 0.41 ms |
| Read, 32 conns, 16 writers active | 188408 ops/s, p50 / p99 0.15 / 0.49 ms |
| Mixed 95/5, 32 conns | 4464 ops/s |
| Restart -> ready, 1000000-record WAL | 2068 ms, RSS 1124 MiB |

## Startup vs WAL length

Median of 3 restarts. `listen` = WAL replayed, port open. `ready` = first write acked (adds the 150-300 ms election + re-apply). Churned log: live keys << records.

| WAL records | WAL MiB | live keys | listen ms | ready ms | RSS MiB |
|---|---|---|---|---|---|
| 0 | 0.0 | 0 | 12.6 | 311 | 9 |
| 100000 | 16.5 | 100000 | 151 | 366 | 97 |
| 1000000 | 155.0 | 91008 | 1596 | 2068 | 1124 |

## Writes (SET/DEL 95/5)

Synchronous clients, one request in flight per connection. MiB/s = SET value bytes.

| value | conns | ops/s | MiB/s | p50 / p99 ms |
|---|---|---|---|---|
| 128 B | 1 | 197 | 0.02 | 4.92 / 13.0 |
| 128 B | 16 | 201 | 0.02 | 81.6 / 132 |
| 128 B | 64 | 212 | 0.02 | 290 / 555 |
| 4 KiB | 16 | 242 | 0.90 | 66.3 / 101 |
| 64 KiB | 16 | 251 | 15.00 | 64.2 / 109 |

## Reads under write load (128 B, 32 reader conns)

Readers only GET, writers only SET/DEL, separate connections, same keyspace, at the same time.

| writer conns | reads/s | read p50 / p99 ms | writes/s | write p50 / p99 ms |
|---|---|---|---|---|
| 0 | 182153 | 0.16 / 0.41 | - | - |
| 1 | 147521 | 0.16 / 1.46 | 217 | 4.03 / 14.5 |
| 16 | 188408 | 0.15 / 0.49 | 280 | 56.1 / 104 |

## Mixed (128 B, 32 conns, GET + SET/DEL on each conn)

Closed loop: a connection's next GET waits for its previous write, so write latency caps total ops/s.

| read/write | total ops/s | reads/s | writes/s | read p50 / p99 ms | write p50 / p99 ms |
|---|---|---|---|---|---|
| 95/5 | 4464 | 4244 | 220 | 0.09 / 2.23 | 144 / 241 |
| 50/50 | 445 | 223 | 222 | 0.22 / 9.82 | 145 / 231 |

