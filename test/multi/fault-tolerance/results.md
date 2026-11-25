# Fault tolerance, 5-node cluster

2026-11-18 16:33 · darwin/arm64, 10 CPUs, go1.27.3 · all nodes on localhost · nodes killed with SIGKILL

Quorum is 3 of 5, so the cluster must survive 2 nodes down and refuse writes with 3 down. `lost` = acked writes missing when re-read from the leader afterwards (must be 0).

## Followers down (leader alive)

8 writer conns, 3s of unique-key SETs after the kill.

| down | alive | writes/s | p50 / p99 ms | errors | lost | result |
|---|---|---|---|---|---|---|
| 0 | 5 | 80 | 99.9 / 130 | 0 | 0 | PASS |
| 1 | 4 | 80 | 103 / 117 | 0 | 0 | PASS |
| 2 | 3 | 97 | 85.0 / 90.2 | 0 | 0 | PASS |

## Leader killed mid-load

8 writer conns; kill at t=1s, load runs 3s more. `failover` = kill -> first acked write (detection + 150-300 ms election + client redirect).

| killed | writes/s before | failover ms | max gap ms | writes/s after | client errors | lost | result |
|---|---|---|---|---|---|---|---|
| leader | 70 | 290 | 307 | 85 | 8 | 0 | PASS |
| leader + 1 follower | 71 | 331 | 375 | 93 | 8 | 0 | PASS |

## Quorum lost (3 of 5 down)

One SET q=v2 sent to each survivor (q=v1 committed before the kill). Then one killed node restarts from its WAL. `q after` may be v1 or v2: a write that timed out on a live leader is *unknown*, not failed, and may commit once the quorum returns.

| killed | write outcome per survivor | acked | reads of q | restart 1 -> write ok | q after | result |
|---|---|---|---|---|---|---|
| 3 followers (leader alive) | leader: Error(commit timeout) in 5001 ms; follower: NotLeader in 0.41 ms | 0 | v1 v1 | 32.5 ms | v2 | PASS |
| leader + 2 followers | follower: NotLeader in 0.38 ms; follower: NotLeader in 0.19 ms | 0 | v1 v1 | 510 ms | v1 | PASS |
