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

-------------------

# Single-node benchmark

2026-11-25 22:25 · darwin/arm64, 10 CPUs, go1.27.3 · 3s per throughput case

One `cmd/server` node over TCP; every write is an fsync'd Raft-log append. Latencies are client-side.

## Summary

| metric | result |
|---|---|
| Write, 1 conn (per-op fsync cost) | 228 ops/s, p50 / p99 4.00 / 8.21 ms |
| Write, 64 conns (concurrency scaling) | 4576 ops/s, p50 / p99 12.8 / 31.9 ms |
| Write bandwidth, 64 KiB values | 66.4 MiB/s |
| Read, 32 conns, no writers | 184056 ops/s, p50 / p99 0.17 / 0.34 ms |
| Read, 32 conns, 16 writers active | 196097 ops/s, p50 / p99 0.16 / 0.35 ms |
| Mixed 95/5, 32 conns | 59776 ops/s |
| Restart -> ready, 1000000-record WAL | 1841 ms, RSS 514 MiB |

## Startup vs WAL length

Median of 3 restarts. `listen` = WAL replayed, port open. `ready` = first write acked (adds the 150-300 ms election + re-apply). Churned log: live keys << records.

| WAL records | WAL MiB | live keys | listen ms | ready ms | RSS MiB |
|---|---|---|---|---|---|
| 0 | 0.0 | 0 | 9.13 | 263 | 9 |
| 100000 | 16.5 | 100000 | 160 | 456 | 71 |
| 1000000 | 155.0 | 91008 | 1464 | 1841 | 514 |

## Writes (SET/DEL 95/5)

Synchronous clients, one request in flight per connection. MiB/s = SET value bytes.

| value | conns | ops/s | MiB/s | p50 / p99 ms |
|---|---|---|---|---|
| 128 B | 1 | 228 | 0.03 | 4.00 / 8.21 |
| 128 B | 16 | 1283 | 0.15 | 12.0 / 26.0 |
| 128 B | 64 | 4576 | 0.53 | 12.8 / 31.9 |
| 4 KiB | 16 | 1243 | 4.62 | 12.2 / 24.2 |
| 64 KiB | 16 | 1122 | 66.42 | 13.8 / 34.1 |

## Reads under write load (128 B, 32 reader conns)

Readers only GET, writers only SET/DEL, separate connections, same keyspace, at the same time.

| writer conns | reads/s | read p50 / p99 ms | writes/s | write p50 / p99 ms |
|---|---|---|---|---|
| 0 | 184056 | 0.17 / 0.34 | - | - |
| 1 | 197815 | 0.16 / 0.32 | 235 | 3.98 / 8.68 |
| 16 | 196097 | 0.16 / 0.35 | 2560 | 5.81 / 16.8 |

## Mixed (128 B, 32 conns, GET + SET/DEL on each conn)

Closed loop: a connection's next GET waits for its previous write, so write latency caps total ops/s.

| read/write | total ops/s | reads/s | writes/s | read p50 / p99 ms | write p50 / p99 ms |
|---|---|---|---|---|---|
| 95/5 | 59776 | 56739 | 3037 | 0.10 / 0.28 | 7.96 / 20.1 |
| 50/50 | 4790 | 2394 | 2396 | 0.26 / 0.78 | 12.0 / 28.0 |

-------------------

# Single-node benchmark

2026-11-26 14:00 · darwin/arm64, 10 CPUs, go1.27.3 · 3s per throughput case

One `cmd/server` node over TCP; every write is an fsync'd Raft-log append. Latencies are client-side.

## Summary

| metric | result |
|---|---|
| Write, 1 conn (per-op fsync cost) | 229 ops/s, p50 / p99 3.99 / 8.29 ms |
| Write, 64 conns (concurrency scaling) | 4567 ops/s, p50 / p99 12.8 / 30.4 ms |
| Write bandwidth, 64 KiB values | 58.9 MiB/s |
| Read, 32 conns, no writers | 183048 ops/s, p50 / p99 0.17 / 0.34 ms |
| Read, 32 conns, 16 writers active | 195200 ops/s, p50 / p99 0.16 / 0.33 ms |
| Mixed 95/5, 32 conns | 51913 ops/s |
| Restart -> ready, 1000000-record WAL | 1982 ms, RSS 519 MiB |

## Startup vs WAL length

Median of 3 restarts. `listen` = WAL replayed, port open. `ready` = first write acked (adds the 150-300 ms election + re-apply). Churned log: live keys << records.

| WAL records | WAL MiB | live keys | listen ms | ready ms | RSS MiB |
|---|---|---|---|---|---|
| 0 | 0.0 | 0 | 13.2 | 303 | 9 |
| 100000 | 16.5 | 100000 | 150 | 376 | 76 |
| 1000000 | 155.0 | 91008 | 1486 | 1982 | 519 |

## Writes (SET/DEL 95/5)

Synchronous clients, one request in flight per connection. MiB/s = SET value bytes.

| value | conns | ops/s | MiB/s | p50 / p99 ms |
|---|---|---|---|---|
| 128 B | 1 | 229 | 0.03 | 3.99 / 8.29 |
| 128 B | 16 | 1518 | 0.18 | 9.82 / 21.1 |
| 128 B | 64 | 4567 | 0.53 | 12.8 / 30.4 |
| 4 KiB | 16 | 1419 | 5.28 | 11.0 / 23.9 |
| 64 KiB | 16 | 995 | 58.90 | 15.1 / 36.8 |

## Reads under write load (128 B, 32 reader conns)

Readers only GET, writers only SET/DEL, separate connections, same keyspace, at the same time.

| writer conns | reads/s | read p50 / p99 ms | writes/s | write p50 / p99 ms |
|---|---|---|---|---|
| 0 | 183048 | 0.17 / 0.34 | - | - |
| 1 | 197460 | 0.16 / 0.32 | 263 | 3.87 / 7.04 |
| 16 | 195200 | 0.16 / 0.33 | 1740 | 9.06 / 16.8 |

## Mixed (128 B, 32 conns, GET + SET/DEL on each conn)

Closed loop: a connection's next GET waits for its previous write, so write latency caps total ops/s.

| read/write | total ops/s | reads/s | writes/s | read p50 / p99 ms | write p50 / p99 ms |
|---|---|---|---|---|---|
| 95/5 | 51913 | 49283 | 2629 | 0.11 / 0.28 | 9.55 / 21.3 |
| 50/50 | 5225 | 2617 | 2608 | 0.23 / 0.92 | 11.6 / 24.0 |

-------------------

# Single-node benchmark

2026-11-26 16:53 · darwin/arm64, 10 CPUs, go1.27.3 · 3s per throughput case

One `cmd/server` node over TCP; every write is an fsync'd Raft-log append. Latencies are client-side.

## Summary

| metric | result |
|---|---|
| Write, 1 conn (per-op fsync cost) | 232 ops/s, p50 / p99 3.99 / 8.21 ms |
| Write, 64 conns (concurrency scaling) | 4951 ops/s, p50 / p99 12.2 / 24.9 ms |
| Write bandwidth, 64 KiB values | 97.1 MiB/s |
| Read, 32 conns, no writers | 192961 ops/s, p50 / p99 0.16 / 0.35 ms |
| Read, 32 conns, 16 writers active | 198996 ops/s, p50 / p99 0.15 / 0.33 ms |
| Mixed 95/5, 32 conns | 50417 ops/s |
| Restart -> ready, 1000000-record WAL | 1974 ms, RSS 523 MiB |

## Startup vs WAL length

Median of 3 restarts. `listen` = WAL replayed, port open. `ready` = first write acked (adds the 150-300 ms election + re-apply). Churned log: live keys << records.

| WAL records | WAL MiB | live keys | listen ms | ready ms | RSS MiB |
|---|---|---|---|---|---|
| 0 | 0.0 | 0 | 12.5 | 261 | 9 |
| 100000 | 16.5 | 100000 | 150 | 479 | 76 |
| 1000000 | 155.0 | 91008 | 1467 | 1974 | 523 |

## Writes (SET/DEL 95/5)

Synchronous clients, one request in flight per connection. MiB/s = SET value bytes.

| value | conns | ops/s | MiB/s | p50 / p99 ms |
|---|---|---|---|---|
| 128 B | 1 | 232 | 0.03 | 3.99 / 8.21 |
| 128 B | 16 | 1434 | 0.17 | 11.0 / 23.1 |
| 128 B | 64 | 4951 | 0.57 | 12.2 / 24.9 |
| 4 KiB | 16 | 2041 | 7.60 | 7.35 / 16.3 |
| 64 KiB | 16 | 1637 | 97.07 | 8.97 / 22.1 |

## Reads under write load (128 B, 32 reader conns)

Readers only GET, writers only SET/DEL, separate connections, same keyspace, at the same time.

| writer conns | reads/s | read p50 / p99 ms | writes/s | write p50 / p99 ms |
|---|---|---|---|---|
| 0 | 192961 | 0.16 / 0.35 | - | - |
| 1 | 196917 | 0.16 / 0.32 | 266 | 3.86 / 7.09 |
| 16 | 198996 | 0.15 / 0.33 | 1690 | 9.24 / 19.1 |

## Mixed (128 B, 32 conns, GET + SET/DEL on each conn)

Closed loop: a connection's next GET waits for its previous write, so write latency caps total ops/s.

| read/write | total ops/s | reads/s | writes/s | read p50 / p99 ms | write p50 / p99 ms |
|---|---|---|---|---|---|
| 95/5 | 50417 | 47863 | 2554 | 0.12 / 0.28 | 9.76 / 20.6 |
| 50/50 | 5325 | 2664 | 2662 | 0.25 / 0.74 | 11.6 / 25.6 |

