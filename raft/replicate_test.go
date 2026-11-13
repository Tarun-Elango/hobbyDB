package raft

import (
	"testing"

	"kvStore/proto"
)

func TestHandleAppendEntriesCopiesLog(t *testing.T) {
	follower := NewNode("b")
	follower.CurrentTerm = 1
	follower.Log = []LogEntry{{
		Index: 1,
		Term:  1,
		Command: proto.Command{
			Op:    proto.OpSet,
			Key:   []byte("k"),
			Value: []byte("old"),
		},
	}}

	res, err := follower.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "a",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{{
			Index: 2,
			Term:  1,
			Command: proto.Command{
				Op:    proto.OpSet,
				Key:   []byte("k"),
				Value: []byte("new"),
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Fatal("append was rejected")
	}
	log := follower.Snapshot().Log
	if len(log) != 2 || string(log[1].Command.Value) != "new" {
		t.Fatalf("log = %+v", log)
	}
}

func TestHandleAppendEntriesReplacesDivergentSuffix(t *testing.T) {
	follower := NewNode("b")
	follower.CurrentTerm = 2
	follower.Log = []LogEntry{
		{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v1")}},
		{Index: 2, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("diverged")}},
	}

	res, err := follower.HandleAppendEntries(AppendEntriesArgs{
		Term:         2,
		LeaderID:     "a",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{{
			Index: 2,
			Term:  2,
			Command: proto.Command{
				Op:    proto.OpSet,
				Key:   []byte("k"),
				Value: []byte("leader"),
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Fatal("append was rejected")
	}
	log := follower.Snapshot().Log
	if len(log) != 2 || log[1].Term != 2 || string(log[1].Command.Value) != "leader" {
		t.Fatalf("log = %+v", log)
	}
}

func TestHandleAppendResultBacksUpThenAdvances(t *testing.T) {
	leader := NewNode("a")
	leader.Role = Leader
	leader.CurrentTerm = 1
	leader.nextIndex = map[string]uint64{"b": 3}
	leader.matchIndex = map[string]uint64{"b": 0}

	leader.handleAppendResult("b", 1, 2, 1, AppendEntriesResult{Term: 1, Success: false})
	if leader.nextIndex["b"] != 2 {
		t.Fatalf("next index after mismatch = %d, want 2", leader.nextIndex["b"])
	}

	leader.handleAppendResult("b", 1, 2, 1, AppendEntriesResult{Term: 1, Success: true})
	if leader.nextIndex["b"] != 3 || leader.matchIndex["b"] != 2 {
		t.Fatalf("next=%d match=%d", leader.nextIndex["b"], leader.matchIndex["b"])
	}
}
