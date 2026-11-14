# Key-value store

Distributed key-value store with Raft consensus, durable WAL, and a custom binary TCP protocol for clients and peer replication.

Run all server commands from the repo root (or `cd` into the paths shown). Use **three terminals** for the cluster, then a fourth for the client.

## Cluster layout (3 nodes)

| Node   | Node ID  | Client port | Replication port |
|--------|----------|-------------|--------------------|
| Node A | `node-a` | `:9000`     | `:9001`            |
| Node B | `node-b` | `:9010`     | `:9002`            |
| Node C | `node-c` | `:9020`     | `:9003`            |

Raft elects the leader automatically. Pass every node's client address in `-client-peers` so a follower can hand the client that leader's address. The client follows that address, and if the leader dies it asks the remaining nodes and switches to the next leader.

Optional profiling: add `-pprof-addr=127.0.0.1:8080` (use a different port per process).

## Run node A

```bash
cd cmd/server
go run . \
  -node-id=node-a \
  -client-addr=:9000 \
  -replication-addr=:9001 \
  -wal=data/node-a.wal \
  -peers=node-b=127.0.0.1:9002,node-c=127.0.0.1:9003 \
  -client-peers=node-b=127.0.0.1:9010,node-c=127.0.0.1:9020
```

## Run node B (follower until elected)

```bash
cd cmd/server
go run . \
  -node-id=node-b \
  -client-addr=:9010 \
  -replication-addr=:9002 \
  -wal=data/node-b.wal \
  -peers=node-a=127.0.0.1:9001,node-c=127.0.0.1:9003 \
  -client-peers=node-a=127.0.0.1:9000,node-c=127.0.0.1:9020
```

## Run node C (follower until elected)

```bash
cd cmd/server
go run . \
  -node-id=node-c \
  -client-addr=:9020 \
  -replication-addr=:9003 \
  -wal=data/node-c.wal \
  -peers=node-a=127.0.0.1:9001,node-b=127.0.0.1:9002 \
  -client-peers=node-a=127.0.0.1:9000,node-b=127.0.0.1:9010
```

## Run client

Connect with every client port. A write sent to a follower is retried on the leader, and a dead leader is replaced by whichever node wins the next election:

```bash
cd client
go run . --9000 --9010 --9020
```

Examples: `set foo bar`, `get foo`, `del foo`, `len`, `ping`, `quit`.

## Compare WAL files

From `cmd/server` after writes (paths are per node):

```bash
cmp data/node-a.wal data/node-b.wal
cmp data/node-a.wal data/node-c.wal
```

No output from `cmp` means the files are identical.

## Inspect WAL bytes

```bash
xxd data/node-a.wal | head
xxd data/node-b.wal | head
xxd data/node-c.wal | head
```

Raft persistent metadata (term/vote) is stored separately, e.g. `data/node-a.wal-raft.state`.
