# Single-node benchmark results

Black-box runs of one `cmd/server` node (no peers) over TCP. Every write is an fsync'd Raft-log append.
Each run is appended below; see `harness_test.go` for how the workloads are built.

## Run 2026-11-15 15:57:27

- Machine: darwin/arm64, 10 CPUs, go1.27.3
- Measured window per throughput case: 5s

### Startup time vs WAL length

Median of 3 restarts, 128 B values. `listen` = process start -> client port open (WAL replay into memory). `ready` = process start -> first write acked (also needs the election timeout of 150-300 ms and re-applying the whole log to the store). Records are a churned history (90% overwrite / 10% delete over up to 100k keys), so live keys << records.

| WAL records | WAL size | live keys | listen ms | ready ms | server RSS MiB |
|---|---|---|---|---|---|
| 0 | 0.0 MiB | 0 | 10.8 | 283 | 9 |
| 10000 | 1.7 MiB | 10000 | 31.8 | 352 | 20 |
| 100000 | 16.5 MiB | 100000 | 155 | 366 | 97 |
| 1000000 | 155.0 MiB | 91008 | 1496 | 2018 | 657 |

### Write / update throughput by payload size

95% SET / 5% DEL on a pre-filled keyspace (80% of ops hit the hottest 20% of keys). Synchronous clients, one request in flight per connection. MiB/s counts value bytes of SETs only.

| value size | conns | keys | ops/s | MiB/s | p50 ms | p99 ms | WAL growth MiB | RSS MiB |
|---|---|---|---|---|---|---|---|---|
| 16 B | 1 | 50000 | 230 | 0.00 | 4.04 | 7.08 | 0.3 | 52 |
| 16 B | 16 | 50000 | 221 | 0.00 | 73.3 | 117 | 0.3 | 199 |
| 16 B | 64 | 50000 | 224 | 0.00 | 284 | 503 | 0.3 | 435 |
| 128 B | 1 | 50000 | 236 | 0.03 | 4.02 | 6.95 | 0.4 | 74 |
| 128 B | 16 | 50000 | 226 | 0.03 | 71.5 | 118 | 0.4 | 158 |
| 128 B | 64 | 50000 | 233 | 0.03 | 277 | 517 | 0.4 | 328 |
| 1 KiB | 1 | 48395 | 228 | 0.21 | 4.08 | 8.07 | 1.3 | 269 |
| 1 KiB | 16 | 48395 | 224 | 0.20 | 71.6 | 113 | 1.3 | 357 |
| 1 KiB | 64 | 48395 | 229 | 0.21 | 278 | 494 | 1.3 | 376 |
| 4 KiB | 1 | 12240 | 246 | 0.92 | 4.00 | 6.04 | 4.7 | 169 |
| 4 KiB | 16 | 12240 | 251 | 0.93 | 65.1 | 87.4 | 4.8 | 239 |
| 4 KiB | 64 | 12240 | 245 | 0.91 | 262 | 455 | 4.7 | 269 |
| 64 KiB | 1 | 1000 | 256 | 15.28 | 3.99 | 5.74 | 76.5 | 366 |
| 64 KiB | 16 | 1000 | 254 | 15.11 | 62.1 | 111 | 76.1 | 358 |
| 64 KiB | 64 | 1000 | 255 | 15.27 | 253 | 379 | 76.6 | 353 |

### Mixed read/write throughput

Pre-filled keyspace, GET vs SET/DEL (writes are ~90% SET / 10% DEL), 80/20 hot-key skew. `read mix` is the GET percentage. Reads are served straight from the in-memory store (server.read.from=any); writes go through Raft + fsync.

| value size | read mix | conns | total ops/s | reads/s | writes/s | GET hit rate | read p50 / p99 ms | write p50 / p99 ms |
|---|---|---|---|---|---|---|---|---|
| 128 B | 100/0 | 32 | 184893 | 184893 | 0 | 100% | 0.17 / 0.34 | - / - |
| 128 B | 95/5 | 32 | 4484 | 4254 | 231 | 100% | 0.12 / 2.55 | 140 / 230 |
| 128 B | 80/20 | 32 | 1152 | 931 | 221 | 100% | 0.12 / 4.23 | 142 / 248 |
| 128 B | 50/50 | 32 | 457 | 231 | 227 | 100% | 0.21 / 13.2 | 139 / 245 |
| 4 KiB | 100/0 | 32 | 157207 | 157207 | 0 | 100% | 0.19 / 0.50 | - / - |
| 4 KiB | 95/5 | 32 | 4669 | 4423 | 246 | 100% | 0.12 / 1.25 | 130 / 227 |
| 4 KiB | 80/20 | 32 | 1385 | 1106 | 279 | 98% | 0.18 / 1.52 | 120 / 212 |
| 4 KiB | 50/50 | 32 | 530 | 270 | 260 | 98% | 0.17 / 1.55 | 130 / 224 |
| 128 B | 95/5 | 1 | 4480 | 4253 | 227 | 100% | 0.05 / 0.07 | 3.83 / 5.41 |
| 128 B | 95/5 | 8 | 4729 | 4490 | 239 | 100% | 0.05 / 0.17 | 33.8 / 50.6 |
| 128 B | 95/5 | 128 | 6004 | 5714 | 290 | 100% | 0.12 / 3.67 | 431 / 938 |

## Run 2026-11-15 15:58:19

- Machine: darwin/arm64, 10 CPUs, go1.27.3
- Measured window per throughput case: 5s

### Reads while writers are active (dedicated connections)

Reader connections only GET, writer connections only SET/DEL (95/5), all hitting the same pre-filled keyspace at the same time.

| value size | reader conns | writer conns | reads/s | read p50 / p99 ms | writes/s | write p50 / p99 ms |
|---|---|---|---|---|---|---|
| 128 B | 32 | 0 | 184258 | 0.17 / 0.35 | - | - / - |
| 128 B | 32 | 1 | 190807 | 0.16 / 0.38 | 242 | 4.00 / 6.13 |
| 128 B | 32 | 16 | 184813 | 0.16 / 0.44 | 288 | 56.8 / 88.9 |
| 4 KiB | 32 | 0 | 156797 | 0.19 / 0.52 | - | - / - |
| 4 KiB | 32 | 1 | 160314 | 0.19 / 0.50 | 257 | 3.98 / 4.97 |
| 4 KiB | 32 | 16 | 160276 | 0.19 / 0.48 | 311 | 53.0 / 86.0 |

### Observations (2026-11-15 runs, macOS arm64, local disk)

- **Writes are fsync-bound, not payload-bound.** ~230-310 ops/s for every value size from 16 B to 64 KiB (64 KiB = ~15 MiB/s), and the figure does not rise with more connections: p50 latency just scales with connection count (4 ms at 1 conn, ~70 ms at 16, ~280 ms at 64). Each write does its own `fsync` (Go uses `F_FULLFSYNC` on macOS, ~4 ms) inside `Node.Propose` while holding the node lock, so there is no group commit. Batching concurrent proposals into one fsync is the big write-throughput lever.
- **Reads are memory-speed and unaffected by writers.** ~160-185k GET/s on 32 connections, p99 under 0.5 ms, with or without 1-16 writers on other connections (second table above). In the interleaved table, throughput collapses at 5% writes only because each connection waits out its own 4 ms write; that is client-side closed-loop behaviour, not server read contention.
- **Startup scales with WAL length, not live data.** The WAL is never compacted or snapshotted, so a 1M-record log (155 MiB, only ~91k live keys) takes ~1.5 s to replay and ~2.0 s to be write-ready, using 657 MiB RSS vs ~97 MiB for 100k records. Floor with an empty log is ~280 ms (the 150-300 ms election timeout).
- Memory: the node keeps the full log in memory *and* the store, so RSS is roughly log + data, which matters for the "DB capped at RAM" assumption.

## Run 2026-11-15 16:07:00

- Machine: darwin/arm64, 10 CPUs, go1.27.3
- Measured window per throughput case: 5s

### Startup time vs WAL length

Median of 3 restarts, 128 B values. `listen` = process start -> client port open (WAL replay into memory). `ready` = process start -> first write acked (also needs the election timeout of 150-300 ms and re-applying the whole log to the store). Records are a churned history (90% overwrite / 10% delete over up to 100k keys), so live keys << records.

| WAL records | WAL size | live keys | listen ms | ready ms | server RSS MiB |
|---|---|---|---|---|---|
| 0 | 0.0 MiB | 0 | 12.2 | 282 | 8 |
| 10000 | 1.7 MiB | 10000 | 36.0 | 319 | 21 |
| 100000 | 16.5 MiB | 100000 | 151 | 442 | 98 |
| 1000000 | 155.0 MiB | 91008 | 1512 | 1943 | 657 |

### Write / update throughput by payload size

95% SET / 5% DEL on a pre-filled keyspace (80% of ops hit the hottest 20% of keys). Synchronous clients, one request in flight per connection. MiB/s counts value bytes of SETs only.

| value size | conns | keys | ops/s | MiB/s | p50 ms | p99 ms | WAL growth MiB | RSS MiB |
|---|---|---|---|---|---|---|---|---|
| 16 B | 1 | 50000 | 208 | 0.00 | 4.31 | 8.04 | 0.3 | 50 |
| 16 B | 16 | 50000 | 227 | 0.00 | 71.0 | 112 | 0.3 | 142 |
| 16 B | 64 | 50000 | 340 | 0.00 | 180 | 372 | 0.3 | 395 |
| 128 B | 1 | 50000 | 254 | 0.03 | 4.02 | 8.24 | 0.4 | 70 |
| 128 B | 16 | 50000 | 272 | 0.03 | 60.9 | 103 | 0.5 | 159 |
| 128 B | 64 | 50000 | 297 | 0.03 | 207 | 446 | 0.5 | 393 |
| 1 KiB | 1 | 48395 | 262 | 0.24 | 4.13 | 9.05 | 1.5 | 259 |
| 1 KiB | 16 | 48395 | 394 | 0.36 | 40.1 | 80.3 | 2.1 | 370 |
| 1 KiB | 64 | 48395 | 252 | 0.23 | 268 | 497 | 1.5 | 416 |
| 4 KiB | 1 | 12240 | 257 | 0.96 | 3.99 | 6.10 | 4.9 | 232 |
| 4 KiB | 16 | 12240 | 265 | 0.98 | 64.1 | 101 | 5.1 | 187 |
| 4 KiB | 64 | 12240 | 246 | 0.92 | 259 | 479 | 4.8 | 257 |
| 64 KiB | 1 | 1000 | 255 | 15.22 | 3.99 | 5.11 | 76.2 | 354 |
| 64 KiB | 16 | 1000 | 260 | 15.45 | 61.9 | 104 | 77.6 | 359 |
| 64 KiB | 64 | 1000 | 258 | 15.46 | 248 | 415 | 77.6 | 352 |

### Mixed read/write throughput

Pre-filled keyspace, GET vs SET/DEL (writes are ~90% SET / 10% DEL), 80/20 hot-key skew. `read mix` is the GET percentage. Reads are served straight from the in-memory store (server.read.from=any); writes go through Raft + fsync.

| value size | read mix | conns | total ops/s | reads/s | writes/s | GET hit rate | read p50 / p99 ms | write p50 / p99 ms |
|---|---|---|---|---|---|---|---|---|
| 128 B | 100/0 | 32 | 185973 | 185973 | 0 | 100% | 0.17 / 0.34 | - / - |
| 128 B | 95/5 | 32 | 4381 | 4156 | 225 | 100% | 0.09 / 2.03 | 139 / 250 |
| 128 B | 80/20 | 32 | 1144 | 923 | 220 | 100% | 0.16 / 7.10 | 145 / 223 |
| 128 B | 50/50 | 32 | 442 | 224 | 218 | 100% | 0.14 / 9.62 | 144 / 265 |
| 4 KiB | 100/0 | 32 | 156372 | 156372 | 0 | 100% | 0.20 / 0.48 | - / - |
| 4 KiB | 95/5 | 32 | 4699 | 4453 | 247 | 100% | 0.12 / 1.49 | 130 / 225 |
| 4 KiB | 80/20 | 32 | 1203 | 959 | 244 | 99% | 0.15 / 1.25 | 130 / 240 |
| 4 KiB | 50/50 | 32 | 497 | 252 | 245 | 98% | 0.16 / 1.88 | 130 / 241 |
| 128 B | 95/5 | 1 | 3714 | 3523 | 191 | 100% | 0.05 / 0.07 | 4.27 / 6.93 |
| 128 B | 95/5 | 8 | 4327 | 4111 | 216 | 100% | 0.05 / 0.35 | 37.1 / 56.3 |
| 128 B | 95/5 | 128 | 4793 | 4568 | 225 | 100% | 0.24 / 6.06 | 567 / 1013 |

### Reads while writers are active (dedicated connections)

Reader connections only GET, writer connections only SET/DEL (95/5), all hitting the same pre-filled keyspace at the same time.

| value size | reader conns | writer conns | reads/s | read p50 / p99 ms | writes/s | write p50 / p99 ms |
|---|---|---|---|---|---|---|
| 128 B | 32 | 0 | 182763 | 0.17 / 0.35 | - | - / - |
| 128 B | 32 | 1 | 194320 | 0.16 / 0.35 | 241 | 4.01 / 6.09 |
| 128 B | 32 | 16 | 184969 | 0.16 / 0.43 | 270 | 58.1 / 154 |
| 4 KiB | 32 | 0 | 158271 | 0.19 / 0.50 | - | - / - |
| 4 KiB | 32 | 1 | 162282 | 0.19 / 0.43 | 251 | 3.99 / 5.25 |
| 4 KiB | 32 | 16 | 162630 | 0.19 / 0.44 | 316 | 53.9 / 87.6 |

