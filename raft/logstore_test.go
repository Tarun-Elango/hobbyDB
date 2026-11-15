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
