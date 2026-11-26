# Failure and recovery, 5-node cluster

2026-11-25 17:34 · darwin/arm64, 10 CPUs, go1.27.3 · localhost · crash = SIGKILL · partition = test proxy black-holes peer links

`lost` = acked writes missing on any node after recovery (must be 0). `converge` = time until every node holds the same key count.

## Leader election

Leader SIGKILLed 5 times, no load. Time = kill -> first acked write on the new leader. Election timeout is 150-300 ms.

| rounds | min ms | median ms | max ms | result |
|---|---|---|---|---|
| 5 | 208 | 257 | 279 | PASS |

## Follower rejoin and catch-up

Follower SIGKILLed, cluster takes 3 s of writes, follower restarts. `catch-up` = restart -> its key count matches the leader. `lost` checked on the rejoined node.

| case | keys missed while down | total keys | catch-up ms | lost | result |
|---|---|---|---|---|---|
| WAL intact | 526 | 721 | 59.4 | 0 | PASS |
| disk wiped | 472 | 639 | 126 | 0 | PASS |

## Full-cluster crash and restart

8 writer conns; all 5 nodes SIGKILLed at t=2 s mid-load, then restarted together. `lost` checked on every node.

| acked before crash | restart -> leader ms | then converge ms | lost | result |
|---|---|---|---|---|
| 349 | 325 | 12.2 | 0 | PASS |

## Partition: old leader in the minority (2 | 3)

q=v1 committed, then {leader, 1 follower} cut off from the other 3. SET q=v2 sent to the old leader, SET q=v3 to the new one. Then heal.

| majority elects ms | q=v3 on new leader | q=v2 on old leader | read q on old leader (cut off) | heal -> old leader has v3 ms | q on all nodes after | result |
|---|---|---|---|---|---|---|
| 237 | OK | Error after 5002 ms | v1 | 34.7 | v3 v3 v3 v3 v3 | PASS |

## Partition: symmetric 2 | 3 (leader in the majority)

8 writer conns for 7 s; 2 followers cut off at t=1 s, healed at t=4 s.

| writes/s before | during | after heal | max gap during ms | max gap after heal ms | leader changed | client errors | converge ms | lost | result |
|---|---|---|---|---|---|---|---|---|---|
| 146 | 154 | 169 | 61.9 | 320 | yes | 0 | 138 | 0 | PASS |

## Flapping links

8 writer conns for 7 s; from t=0.5 s to 5.5 s a random 2 | 3 split is on for 300 ms, off for 200 ms (seeded).

| flaps | writes/s before | while flapping | after | max gap ms | client errors | converge ms | lost | result |
|---|---|---|---|---|---|---|---|---|
| 10 | 160 | 45 | 115 | 1414 | 0 | 13.0 | 0 | PASS |

## Rolling restart

8 writer conns for 11 s; each node in turn gets SIGTERM, is down 300 ms, restarts, then 1 s to settle. Followers first, leader last.

| writes/s before | during roll | max gap ms | client errors | converge ms | lost | result |
|---|---|---|---|---|---|---|
| 170 | 154 | 283 | 8 | 12.6 | 0 | PASS |

-------------------

# Failure and recovery, 5-node cluster

2026-11-25 22:23 · darwin/arm64, 10 CPUs, go1.27.3 · localhost · crash = SIGKILL · partition = test proxy black-holes peer links

`lost` = acked writes missing on any node after recovery (must be 0). `converge` = time until every node holds the same key count.

## Leader election

Leader SIGKILLed 5 times, no load. Time = kill -> first acked write on the new leader. Election timeout is 150-300 ms.

| rounds | min ms | median ms | max ms | result |
|---|---|---|---|---|
| 5 | 207 | 232 | 487 | PASS |

## Follower rejoin and catch-up

Follower SIGKILLed, cluster takes 3 s of writes, follower restarts. `catch-up` = restart -> its key count matches the leader. `lost` checked on the rejoined node.

| case | keys missed while down | total keys | catch-up ms | lost | result |
|---|---|---|---|---|---|
| WAL intact | 621 | 815 | 82.5 | 0 | PASS |
| disk wiped | 627 | 819 | 135 | 0 | PASS |

## Full-cluster crash and restart

8 writer conns; all 5 nodes SIGKILLed at t=2 s mid-load, then restarted together. `lost` checked on every node.

| acked before crash | restart -> leader ms | then converge ms | lost | result |
|---|---|---|---|---|
| 344 | 225 | 12.5 | 0 | PASS |

## Partition: old leader in the minority (2 | 3)

q=v1 committed, then {leader, 1 follower} cut off from the other 3. SET q=v2 sent to the old leader, SET q=v3 to the new one. Then heal.

| majority elects ms | q=v3 on new leader | q=v2 on old leader | read q on old leader (cut off) | heal -> old leader has v3 ms | q on all nodes after | result |
|---|---|---|---|---|---|---|
| 174 | OK | Error after 5001 ms | v1 | 73.2 | v3 v1 v3 v3 v3 | **FAIL** |

## Partition: symmetric 2 | 3 (leader in the majority)

8 writer conns for 7 s; 2 followers cut off at t=1 s, healed at t=4 s.

| writes/s before | during | after heal | max gap during ms | max gap after heal ms | leader changed | client errors | converge ms | lost | result |
|---|---|---|---|---|---|---|---|---|---|
| 166 | 192 | 207 | 36.9 | 294 | no | 0 | 189 | 0 | PASS |

## Flapping links

8 writer conns for 7 s; from t=0.5 s to 5.5 s a random 2 | 3 split is on for 300 ms, off for 200 ms (seeded).

| flaps | writes/s before | while flapping | after | max gap ms | client errors | converge ms | lost | result |
|---|---|---|---|---|---|---|---|---|
| 10 | 196 | 97 | 194 | 550 | 0 | 14.0 | 0 | PASS |

## Rolling restart

8 writer conns for 11 s; each node in turn gets SIGTERM, is down 300 ms, restarts, then 1 s to settle. Followers first, leader last.

| writes/s before | during roll | max gap ms | client errors | converge ms | lost | result |
|---|---|---|---|---|---|---|
| 174 | 168 | 302 | 8 | 13.5 | 0 | PASS |

