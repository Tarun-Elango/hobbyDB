package raft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kvStore/proto"
)

// A single node writes, restarts, wins its election again, and the old entry
// is committed and applied again via the leader's no-op.
func TestLogSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "raft.log")

	open := func() *Node {
		n, err := OpenNode("a", filepath.Join(dir, "state"))
		if err != nil {
			t.Fatal(err)
		}
		if err := n.OpenLog(logPath); err != nil {
			t.Fatal(err)
		}
		if err := n.SetPeers(nil); err != nil {
			t.Fatal(err)
		}
		return n
	}
	elect := func(n *Node) {
		n.mu.Lock()
		n.Role = Candidate
		term := n.CurrentTerm
		n.mu.Unlock()
		n.becomeLeader(term)
	}

	n1 := open()
	elect(n1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go n1.RunApply(ctx, func(proto.Command) {})
	if _, err := n1.Propose(ctx, proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := n1.CloseLog(); err != nil {
		t.Fatal(err)
	}

	n2 := open()
	if got := len(n2.Snapshot().Log); got != 2 { // no-op + set
		t.Fatalf("restored log length = %d, want 2", got)
	}
	elect(n2)
	applied := make(chan proto.Command, 4)
	go n2.RunApply(ctx, func(c proto.Command) { applied <- c })
	for {
		select {
		case c := <-applied:
			if c.Op == proto.OpSet && string(c.Key) == "k" && string(c.Value) == "v" {
				return
			}
		case <-ctx.Done():
			t.Fatal("restored entry was never applied")
		}
	}
}

// A node whose state file is lost (or older than its log) must not boot:
// it would have forgotten votes it already cast.
func TestOpenLogRejectsLogAheadOfState(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state")
	logPath := filepath.Join(dir, "raft.log")

	n, err := OpenNode("a", statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.SetPersistentState(3, "a"); err != nil {
		t.Fatal(err)
	}
	if err := n.OpenLog(logPath); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	err = n.persistAppendLocked(LogEntry{Index: 1, Term: 3, Command: proto.Command{Op: proto.OpNoop}})
	n.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.CloseLog(); err != nil {
		t.Fatal(err)
	}

	// state file intact: boots
	n2, err := OpenNode("a", statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := n2.OpenLog(logPath); err != nil {
		t.Fatalf("boot with matching state: %v", err)
	}
	_ = n2.CloseLog()

	// state file deleted: OpenNode recreates it at term 0, OpenLog must refuse
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	n3, err := OpenNode("a", statePath)
	if err != nil {
		t.Fatal(err)
	}
	err = n3.OpenLog(logPath)
	if !errors.Is(err, ErrLogAheadOfState) {
		t.Fatalf("OpenLog() error = %v, want ErrLogAheadOfState", err)
	}
	if got := len(n3.Snapshot().Log); got != 0 {
		t.Fatalf("log loaded despite error: %d entries", got)
	}
}

func TestHandleAppendEntriesBatchDurableAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state")
	logPath := filepath.Join(dir, "raft.log")

	openFollower := func() *Node {
		t.Helper()
		n, err := OpenNode("b", statePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.OpenLog(logPath); err != nil {
			t.Fatal(err)
		}
		return n
	}

	f := openFollower()

	// Initial multi-entry batch from empty log.
	batch1 := []LogEntry{
		{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpNoop}},
		{Index: 2, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k1"), Value: []byte("v1"), ClientID: 10, Seq: 1}},
		{Index: 3, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k2"), Value: []byte("v2"), ClientID: 10, Seq: 2}},
	}
	res, err := f.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "a",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      batch1,
		LeaderCommit: 3,
	})
	if err != nil {
		t.Fatalf("batch1 HandleAppendEntries: %v", err)
	}
	if !res.Success {
		t.Fatal("batch1 was rejected")
	}

	// Overlapping batch: index 3 matches, indices 4 and 5 are new.
	batch2 := []LogEntry{
		{Index: 3, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k2"), Value: []byte("v2"), ClientID: 10, Seq: 2}},
		{Index: 4, Term: 1, Command: proto.Command{Op: proto.OpDel, Key: []byte("k1"), ClientID: 10, Seq: 3}},
		{Index: 5, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k3"), Value: []byte("v3")}},
	}
	res, err = f.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "a",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      batch2,
		LeaderCommit: 5,
	})
	if err != nil {
		t.Fatalf("batch2 HandleAppendEntries: %v", err)
	}
	if !res.Success {
		t.Fatal("batch2 was rejected")
	}

	// Non-contiguous batch must be rejected before persisting any entry in the batch.
	res, err = f.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "a",
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Index: 6, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("gap1"), Value: []byte("x")}},
			{Index: 8, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("gap2"), Value: []byte("y")}},
		},
	})
	if err != nil {
		t.Fatalf("non-contiguous HandleAppendEntries error: %v", err)
	}
	if res.Success {
		t.Fatal("expected non-contiguous batch to be rejected")
	}
	if err := f.CloseLog(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify all 5 entries survived durably and the rejected batch did not.
	reopened := openFollower()
	defer reopened.CloseLog()
	got := reopened.Snapshot().Log
	if len(got) != 5 {
		t.Fatalf("reopened log len = %d, want 5: %+v", len(got), got)
	}
	wantKeys := []string{"", "k1", "k2", "k1", "k3"}
	wantVals := []string{"", "v1", "v2", "", "v3"}
	wantOps := []byte{proto.OpNoop, proto.OpSet, proto.OpSet, proto.OpDel, proto.OpSet}
	for i := range got {
		if got[i].Index != uint64(i+1) || got[i].Term != 1 || got[i].Command.Op != wantOps[i] ||
			string(got[i].Command.Key) != wantKeys[i] || string(got[i].Command.Value) != wantVals[i] {
			t.Fatalf("entry[%d] = %+v, want index=%d term=1 op=%d key=%q val=%q",
				i, got[i], i+1, wantOps[i], wantKeys[i], wantVals[i])
		}
	}
	if got[1].Command.ClientID != 10 || got[1].Command.Seq != 1 || got[3].Command.ClientID != 10 || got[3].Command.Seq != 3 {
		t.Fatalf("client metadata lost across reopen: %+v", got)
	}
}

func TestHandleAppendEntriesConflictTruncateAndBatchDurable(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state")
	logPath := filepath.Join(dir, "raft.log")

	openFollower := func() *Node {
		t.Helper()
		n, err := OpenNode("b", statePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.OpenLog(logPath); err != nil {
			t.Fatal(err)
		}
		return n
	}

	f := openFollower()

	// Seed term 1 entries: indices 1, 2, 3, 4.
	res, err := f.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "old-leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("a"), Value: []byte("1")}},
			{Index: 2, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("b"), Value: []byte("2")}},
			{Index: 3, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("c"), Value: []byte("stale3")}},
			{Index: 4, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("d"), Value: []byte("stale4")}},
		},
	})
	if err != nil || !res.Success {
		t.Fatalf("seed append: res=%+v err=%v", res, err)
	}

	// New leader in term 2 sends index 2 (matching), then conflicting index 3 + replacement batch 3, 4, 5.
	res, err = f.HandleAppendEntries(AppendEntriesArgs{
		Term:         2,
		LeaderID:     "new-leader",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Index: 2, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("b"), Value: []byte("2")}},
			{Index: 3, Term: 2, Command: proto.Command{Op: proto.OpSet, Key: []byte("c"), Value: []byte("new3")}},
			{Index: 4, Term: 2, Command: proto.Command{Op: proto.OpSet, Key: []byte("d"), Value: []byte("new4")}},
			{Index: 5, Term: 2, Command: proto.Command{Op: proto.OpSet, Key: []byte("e"), Value: []byte("new5")}},
		},
		LeaderCommit: 5,
	})
	if err != nil || !res.Success {
		t.Fatalf("conflict replacement append: res=%+v err=%v", res, err)
	}
	if err := f.CloseLog(); err != nil {
		t.Fatal(err)
	}

	reopened := openFollower()
	defer reopened.CloseLog()
	got := reopened.Snapshot().Log
	if len(got) != 5 {
		t.Fatalf("reopened log len = %d, want 5: %+v", len(got), got)
	}
	want := []struct {
		index uint64
		term  uint64
		key   string
		val   string
	}{
		{1, 1, "a", "1"},
		{2, 1, "b", "2"},
		{3, 2, "c", "new3"},
		{4, 2, "d", "new4"},
		{5, 2, "e", "new5"},
	}
	for i, w := range want {
		if got[i].Index != w.index || got[i].Term != w.term || string(got[i].Command.Key) != w.key || string(got[i].Command.Value) != w.val {
			t.Fatalf("entry[%d] = %+v, want %+v", i, got[i], w)
		}
	}
}
