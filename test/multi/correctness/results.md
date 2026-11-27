# Correctness, 5-node cluster

2026-11-25 17:32 · darwin/arm64, 10 CPUs, go1.27.3 · 5 `cmd/server` processes on localhost · crashes = SIGKILL · stall = SIGSTOP/SIGCONT

## Linearizability under leader crashes

6 clients doing random GET/SET on 4 hot keys for 6s while the leader is SIGKILLed and restarted twice. `read.from=leader` must be linearizable; `read.from=any` is expected to serve stale follower reads.

| read.from | ops | linearizable keys | provably stale reads | result |
|---|---|---|---|---|
| leader | 982 | 4/4 | 0 | PASS |
| any | 1103 | 0/4 | 38 | expected stale |

## No lost acked writes

8 writers for 6s under random node crashes (<=2 down at once), followed by a full-cluster SIGKILL and cold restart from WALs.

| acked | unknown | faults | restart -> leader | lost | converged | result |
|---|---|---|---|---|---|---|
| 935 | 0 | 9 | 240ms | 0 | 5/5 | PASS |

## Stale reads

Read-your-writes (100 leader SETs then GET on each follower), monotonic reads (random node GETs over 3s), and stalled old leader (SIGSTOP, new leader commits `new`, GET on old leader, SIGCONT).

| read.from | read-your-writes stale | monotonic backwards | paused old leader GET | result |
|---|---|---|---|---|
| leader | 0/400 (0%) | 0/5189 | NotLeader | ryw:PASS / mono:PASS / paused:PASS |
| any | 72/400 (18%) | 495/1598 | old | ryw:info / mono:info / paused:info |

-------------------

# Correctness, 5-node cluster

2026-11-25 22:22 · darwin/arm64, 10 CPUs, go1.27.3 · 5 `cmd/server` processes on localhost · crashes = SIGKILL · stall = SIGSTOP/SIGCONT

## Linearizability under leader crashes

6 clients doing random GET/SET on 4 hot keys for 6s while the leader is SIGKILLed and restarted twice. `read.from=leader` must be linearizable; `read.from=any` is expected to serve stale follower reads.

| read.from | ops | linearizable keys | provably stale reads | result |
|---|---|---|---|---|
| leader | 1101 | 4/4 | 0 | PASS |
| any | 1174 | 0/4 | 48 | expected stale |

## No lost acked writes

8 writers for 6s under random node crashes (<=2 down at once), followed by a full-cluster SIGKILL and cold restart from WALs.

| acked | unknown | faults | restart -> leader | lost | converged | result |
|---|---|---|---|---|---|---|
| 1119 | 0 | 9 | 223ms | 0 | 5/5 | PASS |

## Stale reads

Read-your-writes (100 leader SETs then GET on each follower), monotonic reads (random node GETs over 3s), and stalled old leader (SIGSTOP, new leader commits `new`, GET on old leader, SIGCONT).

| read.from | read-your-writes stale | monotonic backwards | paused old leader GET | result |
|---|---|---|---|---|
| leader | 0/400 (0%) | 0/4622 | NotLeader | ryw:PASS / mono:PASS / paused:PASS |
| any | 78/400 (20%) | 227/1620 | old | ryw:info / mono:info / paused:info |

-------------------

# Correctness, 5-node cluster

2026-11-26 13:57 · darwin/arm64, 10 CPUs, go1.27.3 · 5 `cmd/server` processes on localhost · crashes = SIGKILL · stall = SIGSTOP/SIGCONT

## Linearizability under leader crashes

6 clients doing random GET/SET on 4 hot keys for 6s while the leader is SIGKILLed and restarted twice. `read.from=leader` must be linearizable; `read.from=any` is expected to serve stale follower reads.

| read.from | ops | linearizable keys | provably stale reads | result |
|---|---|---|---|---|
| leader | 1052 | 4/4 | 0 | PASS |
| any | 1121 | 0/4 | 43 | expected stale |

## No lost acked writes

8 writers for 6s under random node crashes (<=2 down at once), followed by a full-cluster SIGKILL and cold restart from WALs.

| acked | unknown | faults | restart -> leader | lost | converged | result |
|---|---|---|---|---|---|---|
| 1220 | 0 | 9 | 272ms | 0 | 5/5 | PASS |

## Stale reads

Read-your-writes (100 leader SETs then GET on each follower), monotonic reads (random node GETs over 3s), and stalled old leader (SIGSTOP, new leader commits `new`, GET on old leader, SIGCONT).

| read.from | read-your-writes stale | monotonic backwards | paused old leader GET | result |
|---|---|---|---|---|
| leader | 0/400 (0%) | 0/4981 | NotLeader | ryw:PASS / mono:PASS / paused:PASS |
| any | 87/400 (22%) | 403/1626 | old | ryw:info / mono:info / paused:info |

-------------------

# Correctness, 5-node cluster

2026-11-26 16:49 · darwin/arm64, 10 CPUs, go1.27.3 · 5 `cmd/server` processes on localhost · crashes = SIGKILL · stall = SIGSTOP/SIGCONT

## Linearizability under leader crashes

6 clients doing random GET/SET on 4 hot keys for 6s while the leader is SIGKILLed and restarted twice. `read.from=leader` must be linearizable; `read.from=any` is expected to serve stale follower reads.

| read.from | ops | linearizable keys | provably stale reads | result |
|---|---|---|---|---|
| leader | 1097 | 4/4 | 0 | PASS |
| any | 1154 | 0/4 | 49 | expected stale |

## No lost acked writes

8 writers for 6s under random node crashes (<=2 down at once), followed by a full-cluster SIGKILL and cold restart from WALs.

| acked | unknown | faults | restart -> leader | lost | converged | result |
|---|---|---|---|---|---|---|
| 1204 | 0 | 9 | 278ms | 0 | 5/5 | PASS |

## Stale reads

Read-your-writes (100 leader SETs then GET on each follower), monotonic reads (random node GETs over 3s), and stalled old leader (SIGSTOP, new leader commits `new`, GET on old leader, SIGCONT).

| read.from | read-your-writes stale | monotonic backwards | paused old leader GET | result |
|---|---|---|---|---|
| leader | 0/400 (0%) | 0/4828 | NotLeader | ryw:PASS / mono:PASS / paused:PASS |
| any | 72/400 (18%) | 280/1670 | old | ryw:info / mono:info / paused:info |

