# Fault tolerance, 5-node cluster

2026-11-25 17:33 · darwin/arm64, 10 CPUs, go1.27.3 · all nodes on localhost · nodes killed with SIGKILL

Quorum is 3 of 5, so the cluster must survive 2 nodes down and refuse writes with 3 down. `lost` = acked writes missing when re-read from the leader afterwards (must be 0).

## Followers down (leader alive)

8 writer conns, 3s of unique-key SETs after the kill.

| down | alive | writes/s | p50 / p99 ms | errors | lost | result |
|---|---|---|---|---|---|---|
| 0 | 5 | 143 | 56.0 / 83.8 | 0 | 0 | PASS |
| 1 | 4 | 151 | 52.2 / 75.1 | 0 | 0 | PASS |
| 2 | 3 | 181 | 44.2 / 72.3 | 0 | 0 | PASS |

## Leader killed mid-load

8 writer conns; kill at t=1s, load runs 3s more. `failover` = kill -> first acked write (detection + 150-300 ms election + client redirect).

| killed | writes/s before | failover ms | max gap ms | writes/s after | client errors | lost | result |
|---|---|---|---|---|---|---|---|
| leader | 164 | 351 | 365 | 171 | 8 | 0 | PASS |
| leader + 1 follower | 155 | 284 | 286 | 171 | 8 | 0 | PASS |

## Quorum lost (3 of 5 down)

One SET q=v2 sent to each survivor (q=v1 committed before the kill). Then one killed node restarts from its WAL. `q after` may be v1 or v2: a write that timed out on a live leader is *unknown*, not failed, and may commit once the quorum returns.

| killed | write outcome per survivor | acked | reads of q | restart 1 -> write ok | q after | result |
|---|---|---|---|---|---|---|
| 3 followers (leader alive) | leader: Error(commit timeout) in 5002 ms; follower: NotLeader in 0.35 ms | 0 | v1 v1 | 31.8 ms | v2 | PASS |
| leader + 2 followers | follower: NotLeader in 0.69 ms; follower: NotLeader in 0.40 ms | 0 | v1 v1 | 164 ms | v1 | PASS |

-------------------

# Fault tolerance, 5-node cluster

2026-11-25 22:23 · darwin/arm64, 10 CPUs, go1.27.3 · all nodes on localhost · nodes killed with SIGKILL

Quorum is 3 of 5, so the cluster must survive 2 nodes down and refuse writes with 3 down. `lost` = acked writes missing when re-read from the leader afterwards (must be 0).

## Followers down (leader alive)

8 writer conns, 3s of unique-key SETs after the kill.

| down | alive | writes/s | p50 / p99 ms | errors | lost | result |
|---|---|---|---|---|---|---|
| 0 | 5 | 196 | 40.2 / 64.1 | 0 | 0 | PASS |
| 1 | 4 | 204 | 39.8 / 70.4 | 0 | 0 | PASS |
| 2 | 3 | 224 | 35.9 / 54.3 | 0 | 0 | PASS |

## Leader killed mid-load

8 writer conns; kill at t=1s, load runs 3s more. `failover` = kill -> first acked write (detection + 150-300 ms election + client redirect).

| killed | writes/s before | failover ms | max gap ms | writes/s after | client errors | lost | result |
|---|---|---|---|---|---|---|---|
| leader | 193 | 257 | 264 | 203 | 8 | 0 | PASS |
| leader + 1 follower | 188 | 577 | 589 | 221 | 8 | 0 | PASS |

## Quorum lost (3 of 5 down)

One SET q=v2 sent to each survivor (q=v1 committed before the kill). Then one killed node restarts from its WAL. `q after` may be v1 or v2: a write that timed out on a live leader is *unknown*, not failed, and may commit once the quorum returns.

| killed | write outcome per survivor | acked | reads of q | restart 1 -> write ok | q after | result |
|---|---|---|---|---|---|---|
| 3 followers (leader alive) | leader: Error(commit timeout) in 5001 ms; follower: NotLeader in 0.46 ms | 0 | v1 v1 | 54.5 ms | v2 | PASS |
| leader + 2 followers | follower: NotLeader in 0.57 ms; follower: NotLeader in 0.33 ms | 0 | v1 v1 | 164 ms | v1 | PASS |

-------------------

# Fault tolerance, 5-node cluster

2026-11-26 13:58 · darwin/arm64, 10 CPUs, go1.27.3 · all nodes on localhost · nodes killed with SIGKILL

Quorum is 3 of 5, so the cluster must survive 2 nodes down and refuse writes with 3 down. `lost` = acked writes missing when re-read from the leader afterwards (must be 0).

## Followers down (leader alive)

8 writer conns, 3s of unique-key SETs after the kill.

| down | alive | writes/s | p50 / p99 ms | errors | lost | result |
|---|---|---|---|---|---|---|
| 0 | 5 | 200 | 39.7 / 71.2 | 0 | 0 | PASS |
| 1 | 4 | 217 | 36.8 / 57.2 | 0 | 0 | PASS |
| 2 | 3 | 220 | 36.0 / 55.1 | 0 | 0 | PASS |

## Leader killed mid-load

8 writer conns; kill at t=1s, load runs 3s more. `failover` = kill -> first acked write (detection + 150-300 ms election + client redirect).

| killed | writes/s before | failover ms | max gap ms | writes/s after | client errors | lost | result |
|---|---|---|---|---|---|---|---|
| leader | 168 | 234 | 256 | 208 | 8 | 0 | PASS |
| leader + 1 follower | 186 | 225 | 230 | 224 | 8 | 0 | PASS |

## Quorum lost (3 of 5 down)

One SET q=v2 sent to each survivor (q=v1 committed before the kill). Then one killed node restarts from its WAL. `q after` may be v1 or v2: a write that timed out on a live leader is *unknown*, not failed, and may commit once the quorum returns.

| killed | write outcome per survivor | acked | reads of q | restart 1 -> write ok | q after | result |
|---|---|---|---|---|---|---|
| 3 followers (leader alive) | leader: Error(commit timeout) in 5002 ms; follower: NotLeader in 0.58 ms | 0 | v1 v1 | 64.5 ms | v2 | PASS |
| leader + 2 followers | follower: NotLeader in 1.25 ms; follower: NotLeader in 0.37 ms | 0 | v1 v1 | 215 ms | v1 | PASS |

-------------------

# Fault tolerance, 5-node cluster

2026-11-26 16:51 · darwin/arm64, 10 CPUs, go1.27.3 · all nodes on localhost · nodes killed with SIGKILL

Quorum is 3 of 5, so the cluster must survive 2 nodes down and refuse writes with 3 down. `lost` = acked writes missing when re-read from the leader afterwards (must be 0).

## Followers down (leader alive)

8 writer conns, 3s of unique-key SETs after the kill.

| down | alive | writes/s | p50 / p99 ms | errors | lost | result |
|---|---|---|---|---|---|---|
| 0 | 5 | 209 | 38.0 / 55.4 | 0 | 0 | PASS |
| 1 | 4 | 239 | 33.0 / 57.5 | 0 | 0 | PASS |
| 2 | 3 | 235 | 33.9 / 53.5 | 0 | 0 | PASS |

## Leader killed mid-load

8 writer conns; kill at t=1s, load runs 3s more. `failover` = kill -> first acked write (detection + 150-300 ms election + client redirect).

| killed | writes/s before | failover ms | max gap ms | writes/s after | client errors | lost | result |
|---|---|---|---|---|---|---|---|
| leader | 207 | 209 | 211 | 234 | 8 | 0 | PASS |
| leader + 1 follower | 189 | 236 | 240 | 245 | 8 | 0 | PASS |

## Quorum lost (3 of 5 down)

One SET q=v2 sent to each survivor (q=v1 committed before the kill). Then one killed node restarts from its WAL. `q after` may be v1 or v2: a write that timed out on a live leader is *unknown*, not failed, and may commit once the quorum returns.

| killed | write outcome per survivor | acked | reads of q | restart 1 -> write ok | q after | result |
|---|---|---|---|---|---|---|
| 3 followers (leader alive) | leader: Error(commit timeout) in 5002 ms; follower: NotLeader in 0.52 ms | 0 | v1 v1 | 58.0 ms | v2 | PASS |
| leader + 2 followers | follower: NotLeader in 0.60 ms; follower: NotLeader in 0.36 ms | 0 | v1 v1 | 182 ms | v1 | PASS |

