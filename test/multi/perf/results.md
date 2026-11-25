# Performance, multi-node cluster

2026-11-25 17:35 · darwin/arm64, 10 CPUs, go1.27.3 · all nodes on one machine (shared disk) · 3s per case · 128 B values · `server.read.from=any`

Closed-loop clients, one request in flight per connection. Latency columns are p50 / p99 / p999 ms, client-side. Reads go to node `g % n`, writes to the leader.

## Scaling with cluster size

16 conns. Write-only run, then read-only run.

| nodes | writes/s | write lat | redirects | reads/s | read lat | errors |
|---|---|---|---|---|---|---|
| 1 | 212 | 75.9 / 96.0 / 96.2 | 0 | 145352 | 0.11 / 0.22 / 0.35 | 0 |
| 3 | 182 | 85.6 / 107 / 107 | 0 | 156821 | 0.10 / 0.24 / 0.47 | 0 |
| 5 | 163 | 97.4 / 136 / 172 | 0 | 150450 | 0.10 / 0.25 / 0.58 | 0 |

## Mixed workload (5 nodes)

32 conns, each picks GET or SET per op. A conn's next read waits for its previous write, so write latency caps the read rate.

| read/write | total ops/s | reads/s | read lat | writes/s | write lat | redirects | errors |
|---|---|---|---|---|---|---|---|
| 90/10 | 1982 | 1795 | 0.15 / 0.64 / 0.81 | 187 | 169 / 191 / 301 | 0 | 0 |
| 50/50 | 365 | 182 | 0.15 / 0.82 / 12.6 | 183 | 172 / 301 / 328 | 0 | 0 |

## Concurrency sweep (5 nodes)

Write-only run, then read-only run. `redirects` = NotLeader replies to writes, i.e. the leader moved.

| conns | writes/s | write lat | redirects | reads/s | read lat | errors |
|---|---|---|---|---|---|---|
| 1 | 73 | 12.4 / 25.1 / 27.0 | 0 | 78361 | 0.01 / 0.02 / 0.05 | 0 |
| 8 | 163 | 48.7 / 72.0 / 72.1 | 0 | 114960 | 0.06 / 0.16 / 0.31 | 0 |
| 16 | 167 | 94.1 / 135 / 159 | 0 | 153716 | 0.10 / 0.24 / 0.49 | 0 |
| 32 | 186 | 171 / 260 / 311 | 0 | 186630 | 0.16 / 0.40 / 0.90 | 0 |
| 64 | 187 | 327 / 607 / 658 | 0 | 203030 | 0.29 / 0.87 / 2.25 | 0 |

## Slow followers (5 nodes)

8 writer conns. Each slow follower gets +10ms each way on its link to the leader (+20ms RTT). All peer links go through a test proxy.

| slow followers | writes/s | write lat | redirects | errors |
|---|---|---|---|---|
| 0 | 143 | 56.0 / 71.0 / 73.1 | 0 | 0 |
| 1 | 158 | 51.9 / 68.0 / 72.9 | 0 | 0 |
| 2 | 163 | 48.7 / 70.0 / 70.1 | 0 | 0 |
| 3 | 132 | 60.1 / 79.4 / 79.4 | 0 | 0 |

