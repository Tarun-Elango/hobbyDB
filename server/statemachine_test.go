package server

import (
	"context"
	"testing"
	"time"

	"kvStore/proto"
	"kvStore/raft"
	"kvStore/store"
)

func set(key, value string, client, seq uint64) proto.Command {
	return proto.Command{Op: proto.OpSet, Key: []byte(key), Value: []byte(value), ClientID: client, Seq: seq}
}

func TestStateMachineAppliesEachClientWriteOnce(t *testing.T) {
	st := store.NewStore[string, []byte]()
	m := NewStateMachine(st)
	get := func() string { v, _ := st.Get("k"); return string(v) }

	m.Apply(set("k", "a1", 1, 1))
	m.Apply(set("k", "b1", 2, 1)) // another client, its own sequence
	m.Apply(set("k", "a1", 1, 1)) // retry of client 1's first write: skipped
	if got := get(); got != "b1" {
		t.Fatalf("k = %q, want b1 (retry must not overwrite a newer write)", got)
	}

	m.Apply(set("k", "a2", 1, 2))
	if got := get(); got != "a2" {
		t.Fatalf("k = %q, want a2", got)
	}

	// a delete retried after a newer set from the same client: skipped
	m.Apply(proto.Command{Op: proto.OpDel, Key: []byte("k"), ClientID: 1, Seq: 2})
	if got := get(); got != "a2" {
		t.Fatalf("k = %q, want a2 (old seq must be skipped)", got)
	}

	// no client id: always applied
	m.Apply(set("k", "x", 0, 0))
	m.Apply(set("k", "y", 0, 0))
	if got := get(); got != "y" {
		t.Fatalf("k = %q, want y", got)
	}

	m.Apply(proto.Command{Op: proto.OpNoop})
	if got := get(); got != "y" {
		t.Fatalf("no-op changed the store: %q", got)
	}
}

// End to end on a single-node leader: the same write proposed twice (a retry
// after the first attempt committed) is answered OK both times but applied once.
func TestRetriedWriteThroughRaftAppliedOnce(t *testing.T) {
	node := raft.NewNode("a")
	node.Role = raft.Leader
	node.CurrentTerm = 1
	st := store.NewStore[string, []byte]()
	s := New(st, Config{RaftNode: node})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.RunApply(ctx, NewStateMachine(st).Apply)

	for _, cmd := range []proto.Command{
		set("k", "first", 1, 1),
		set("k", "other", 2, 1),
		set("k", "first", 1, 1), // retry
	} {
		resp, err := s.dispatch(ctx, cmd)
		if err != nil || resp.Status != proto.StatusOk {
			t.Fatalf("dispatch %+v = %+v, %v", cmd, resp, err)
		}
	}
	if v, _ := st.Get("k"); string(v) != "other" {
		t.Fatalf("k = %q, want other", v)
	}
	if got := len(node.Snapshot().Log); got != 3 {
		t.Fatalf("log length = %d, want 3 (the retry is still logged, just not applied)", got)
	}
}
