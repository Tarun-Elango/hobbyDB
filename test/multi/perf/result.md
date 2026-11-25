# Performance, multi-node cluster

2026-11-24 18:15 · darwin/arm64, 10 CPUs, go1.27.3 · all nodes on one machine (shared disk) · 3s per case · 128 B values · `server.read.from=any`

Closed-loop clients, one request in flight per connection. Latency columns are p50 / p99 / p999 ms, client-side. Reads go to node `g % n`, writes to the leader.

## Scaling with cluster size

16 conns. Write-only run, then read-only run.

| nodes | writes/s | write lat | redirects | reads/s | read lat | errors |
|---|---|---|---|---|---|---|
| 1 | 228 | 70.0 / 95.0 / 105 | 0 | 142620 | 0.11 / 0.24 / 0.36 | 0 |
| 3 | 209 | 78.1 / 110 / 115 | 0 | 160597 | 0.09 / 0.23 / 0.41 | 0 |
| 5 | 289 | 56.7 / 93.0 / 100 | 0 | 152831 | 0.10 / 0.24 / 0.46 | 0 |

## Mixed workload (5 nodes)

32 conns, each picks GET or SET per op. A conn's next read waits for its previous write, so write latency caps the read rate.

| read/write | total ops/s | reads/s | read lat | writes/s | write lat | redirects | errors |
|---|---|---|---|---|---|---|---|
| 90/10 | 2095 | 1898 | 0.12 / 0.63 / 11.4 | 197 | 165 / 253 / 269 | 0 | 0 |
| 50/50 | 391 | 196 | 0.12 / 0.62 / 10.3 | 195 | 163 / 254 / 271 | 0 | 0 |

## Concurrency sweep (5 nodes)

Write-only run, then read-only run. `redirects` = NotLeader replies to writes, i.e. the leader moved.

| conns | writes/s | write lat | redirects | reads/s | read lat | errors |
|---|---|---|---|---|---|---|
| 1 | 71 | 14.0 / 23.1 / 23.1 | 0 | 78027 | 0.01 / 0.02 / 0.05 | 0 |
| 8 | 262 | 31.5 / 57.2 / 66.1 | 0 | 122639 | 0.06 / 0.15 / 0.27 | 0 |
| 16 | 187 | 85.4 / 108 / 152 | 0 | 155013 | 0.10 / 0.26 / 0.60 | 0 |
| 32 | 205 | 161 / 214 / 276 | 0 | 184351 | 0.16 / 0.43 / 1.24 | 0 |
| 64 | 205 | 318 / 574 / 600 | 0 | 200587 | 0.29 / 0.94 / 2.38 | 0 |

## Slow followers (5 nodes)

8 writer conns. Each slow follower gets +10ms each way on its link to the leader (+20ms RTT). All peer links go through a test proxy.

| slow followers | writes/s | write lat | redirects | errors |
|---|---|---|---|---|
| 0 | 161 | 49.9 / 70.0 / 70.1 | 0 | 0 |
| 1 | 159 | 49.9 / 74.4 / 82.3 | 0 | 0 |
| 2 | 159 | 49.0 / 74.9 / 76.9 | 0 | 0 |
| 3 | 143 | 55.9 / 71.5 / 75.9 | 0 | 0 |
