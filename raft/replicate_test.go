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

func TestCommitIndexAdvancesWhenMajorityHasCurrentTerm(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b", "c"}, []LogEntry{
		{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("old")}},
		{Index: 2, Term: 2, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("new")}},
	})

	// leader + one peer is a majority of 3. The acked entry is from this term,
	// so the older prefix commits with it.
	leader.handleAppendResult("b", 2, 2, 1, AppendEntriesResult{Term: 2, Success: true})
	if got := leader.Snapshot().CommitIndex; got != 2 {
		t.Fatalf("commit index = %d, want 2", got)
	}
}

func TestOldTermMajorityDoesNotAdvanceCommit(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b", "c"}, []LogEntry{
		{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("old")}},
	})

	leader.handleAppendResult("b", 2, 1, 1, AppendEntriesResult{Term: 2, Success: true})
	leader.handleAppendResult("c", 2, 1, 1, AppendEntriesResult{Term: 2, Success: true})
	if got := leader.Snapshot().CommitIndex; got != 0 {
		t.Fatalf("commit index = %d, want 0", got)
	}
}

func TestCommitIndexNeedsFullQuorum(t *testing.T) {
	leader := newTestLeader(t, "a", 1, []string{"b", "c", "d", "e"}, []LogEntry{
		{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")}},
	})

	// 5 nodes need 3 copies. The leader plus one peer is not enough.
	leader.handleAppendResult("b", 1, 1, 1, AppendEntriesResult{Term: 1, Success: true})
	if got := leader.Snapshot().CommitIndex; got != 0 {
		t.Fatalf("commit index after one peer = %d, want 0", got)
	}

	leader.handleAppendResult("c", 1, 1, 1, AppendEntriesResult{Term: 1, Success: true})
	if got := leader.Snapshot().CommitIndex; got != 1 {
		t.Fatalf("commit index after two peers = %d, want 1", got)
	}
}

func TestProposeCommitsImmediatelyOnSingleNode(t *testing.T) {
	leader := NewNode("a")
	leader.Role = Leader
	leader.CurrentTerm = 3

	entry, err := leader.Propose(proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if got := leader.Snapshot().CommitIndex; got != entry.Index {
		t.Fatalf("commit index = %d, want %d", got, entry.Index)
	}
}

func TestFollowerCommitFollowsLeaderUpToMatchedPrefix(t *testing.T) {
	follower := NewNode("b")
	follower.CurrentTerm = 1
	follower.Log = []LogEntry{{
		Index: 1, Term: 1,
		Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v1")},
	}}

	res, err := follower.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "a",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		LeaderCommit: 5,
		Entries: []LogEntry{{
			Index: 2, Term: 1,
			Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v2")},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Fatal("append was rejected")
	}
	// the leader says 5 is committed, but this follower has only matched through 2.
	if got := follower.Snapshot().CommitIndex; got != 2 {
		t.Fatalf("commit index = %d, want 2", got)
	}

	res, err = follower.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "a",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		LeaderCommit: 1,
		Entries: []LogEntry{{
			Index: 3, Term: 1,
			Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v3")},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Fatal("append was rejected")
	}
	if got := follower.Snapshot().CommitIndex; got != 2 {
		t.Fatalf("commit index moved backward to %d", got)
	}
}

func newTestLeader(t *testing.T, id string, term uint64, peers []string, log []LogEntry) *Node {
	t.Helper()
	leader := NewNode(id)
	if err := leader.SetPeers(peers); err != nil {
		t.Fatal(err)
	}
	leader.Role = Leader
	leader.CurrentTerm = term
	leader.Log = log
	leader.nextIndex = make(map[string]uint64, len(peers))
	leader.matchIndex = make(map[string]uint64, len(peers))
	for _, peer := range peers {
		leader.nextIndex[peer] = uint64(len(log)) + 1
		leader.matchIndex[peer] = 0
	}
	return leader
}
