package raft_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"kvStore/proto"
	"kvStore/raft"
	"kvStore/server"
	"kvStore/store"
)

func electSingleNode(t *testing.T, n *raft.Node) {
	t.Helper()
	// Use OpenNode + single-node election via Run/RequestVote or direct election.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go n.Run(ctx, func(context.Context, string, raft.RequestVoteArgs) (raft.RequestVoteResult, error) {
		return raft.RequestVoteResult{}, nil
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().Role == raft.Leader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("single node never became leader")
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func TestAutoSnapshotCompactRestartAndDedupPreserved(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "node.state")
	snapPath := filepath.Join(dir, "node.snap")
	logPath := filepath.Join(dir, "node.log")

	open := func() (*raft.Node, *store.Store[string, []byte], *server.StateMachine) {
		st := store.NewStore[string, []byte]()
		sm := server.NewStateMachine(st)
		n, err := raft.OpenNode("a", statePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.OpenSnapshot(snapPath, sm); err != nil {
			t.Fatal(err)
		}
		if err := n.OpenLog(logPath); err != nil {
			t.Fatal(err)
		}
		if err := n.SetPeers(nil); err != nil {
			t.Fatal(err)
		}
		n.SetSnapshotConfig(10, 0, 2)
		return n, st, sm
	}

	n1, st1, sm1 := open()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go n1.RunApply(ctx, sm1.Apply)
	electSingleNode(t, n1)

	// Propose 25 writes from client 42 (seqs 1..25) plus overwrites and deletes.
	for i := 1; i <= 25; i++ {
		cmd := proto.Command{
			Op:       proto.OpSet,
			Key:      []byte(fmt.Sprintf("k-%d", i%10)),
			Value:    []byte(fmt.Sprintf("v-%d", i)),
			ClientID: 42,
			Seq:      uint64(i),
		}
		if _, err := n1.Propose(ctx, cmd); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	// Wait for background snapshot & log compaction to finish.
	waitFor(t, 2*time.Second, "snapshot and compaction", func() bool {
		snap := n1.Snapshot()
		return snap.SnapshotIndex >= 20 && len(snap.Log) <= 10
	})

	// WaitLeaderRead must still work right after compaction.
	if err := n1.WaitLeaderRead(ctx); err != nil {
		t.Fatalf("WaitLeaderRead after compaction: %v", err)
	}
	if _, ok := st1.Get("k-5"); !ok {
		t.Fatal("missing key k-5 before restart")
	}

	cancel()
	if err := n1.CloseLog(); err != nil {
		t.Fatal(err)
	}

	// Reopen from snapshot + compacted WAL.
	n2, st2, sm2 := open()
	defer n2.CloseLog()

	snap2 := n2.Snapshot()
	if snap2.SnapshotIndex < 20 {
		t.Fatalf("restored SnapshotIndex = %d, want >= 20", snap2.SnapshotIndex)
	}
	if snap2.LastApplied != snap2.SnapshotIndex {
		t.Fatalf("restored LastApplied = %d, want SnapshotIndex %d", snap2.LastApplied, snap2.SnapshotIndex)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	go n2.RunApply(ctx2, sm2.Apply)
	electSingleNode(t, n2)

	// Wait for post-snapshot tail + leader noop to apply.
	waitFor(t, 2*time.Second, "post-restart apply", func() bool {
		v, ok := st2.Get("k-5")
		return ok && string(v) == "v-25"
	})

	// Retry an already-applied write from client 42 (seq 1) AFTER snapshot+restart:
	// dedup table from snapshot must skip it so k-1 stays "v-21", not "v-1"!
	if _, err := n2.Propose(ctx2, proto.Command{
		Op:       proto.OpSet,
		Key:      []byte("k-1"),
		Value:    []byte("v-1-stale-retry"),
		ClientID: 42,
		Seq:      1,
	}); err != nil {
		t.Fatalf("propose retry: %v", err)
	}
	if v, _ := st2.Get("k-1"); string(v) != "v-21" {
		t.Fatalf("k-1 = %q, want v-21 (snapshot must preserve client dedup table)", v)
	}
}

func TestOpenSnapshotRejectsAheadOfStateAndCorruption(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "node.state")
	snapPath := filepath.Join(dir, "node.snap")
	logPath := filepath.Join(dir, "node.log")

	st := store.NewStore[string, []byte]()
	sm := server.NewStateMachine(st)
	n, err := raft.OpenNode("a", statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.OpenSnapshot(snapPath, sm); err != nil {
		t.Fatal(err)
	}
	if err := n.OpenLog(logPath); err != nil {
		t.Fatal(err)
	}
	_ = n.SetPeers(nil)
	n.SetSnapshotConfig(5, 0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	go n.RunApply(ctx, sm.Apply)
	electSingleNode(t, n)

	for i := 1; i <= 8; i++ {
		if _, err := n.Propose(ctx, proto.Command{
			Op:    proto.OpSet,
			Key:   []byte(fmt.Sprintf("k%d", i)),
			Value: []byte("v"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 2*time.Second, "snapshot written", func() bool {
		return n.Snapshot().SnapshotIndex >= 5
	})
	cancel()
	_ = n.CloseLog()

	// Deleting state file resets term to 0 -> OpenSnapshot must reject with ErrLogAheadOfState.
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	n2, err := raft.OpenNode("a", statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := n2.OpenSnapshot(snapPath, server.NewStateMachine(store.NewStore[string, []byte]())); !errors.Is(err, raft.ErrLogAheadOfState) {
		t.Fatalf("OpenSnapshot err = %v, want ErrLogAheadOfState", err)
	}

	// Restore state file, corrupt snapshot file -> must reject with ErrCorruptSnapshot.
	if err := os.WriteFile(statePath, stateBytes, 0600); err != nil {
		t.Fatal(err)
	}
	snapBytes, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	snapBytes[len(snapBytes)-1] ^= 0xFF
	if err := os.WriteFile(snapPath, snapBytes, 0600); err != nil {
		t.Fatal(err)
	}
	n3, err := raft.OpenNode("a", statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := n3.OpenSnapshot(snapPath, server.NewStateMachine(store.NewStore[string, []byte]())); !errors.Is(err, raft.ErrCorruptSnapshot) {
		t.Fatalf("OpenSnapshot corrupt err = %v, want ErrCorruptSnapshot", err)
	}
}

func TestLaggingFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	// Leader + follower "b" form a 2-node majority (with "c" offline).
	// Leader compacts its entire log (trailingLogs = 0). Then wiped follower "c"
	// joins (with divergent uncommitted entries!) and must catch up via
	// InstallSnapshot, discard its divergent log, restore store + dedup table,
	// and apply subsequent writes normally.
	type member struct {
		id   string
		node *raft.Node
		st   *store.Store[string, []byte]
		sm   *server.StateMachine
	}

	dir := t.TempDir()
	ids := []string{"a", "b", "c"}
	members := make(map[string]*member, 3)

	for _, id := range ids {
		st := store.NewStore[string, []byte]()
		sm := server.NewStateMachine(st)
		n, err := raft.OpenNode(id, filepath.Join(dir, id+".state"))
		if err != nil {
			t.Fatal(err)
		}
		if err := n.OpenSnapshot(filepath.Join(dir, id+".snap"), sm); err != nil {
			t.Fatal(err)
		}
		if err := n.OpenLog(filepath.Join(dir, id+".log")); err != nil {
			t.Fatal(err)
		}
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		if err := n.SetPeers(peers); err != nil {
			t.Fatal(err)
		}
		n.SetSnapshotConfig(8, 0, 0) // compact everything once >= 8 entries applied
		members[id] = &member{id: id, node: n, st: st, sm: sm}
		t.Cleanup(func() { _ = n.CloseLog() })
	}

	// Seed follower "c" with divergent uncommitted entries from term 1 before
	// it goes offline, to test Raft §7 full-log discard + WAL ResetAfter.
	if res, err := members["c"].node.HandleAppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "old",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []raft.LogEntry{
			{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("bad"), Value: []byte("divergent")}},
			{Index: 2, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("bad2"), Value: []byte("divergent2")}},
		},
	}); err != nil || !res.Success {
		t.Fatalf("seed divergent c: res=%+v err=%v", res, err)
	}

	var cOnline atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	voteRPC := func(from string) raft.VoteRPC {
		return func(_ context.Context, peer string, args raft.RequestVoteArgs) (raft.RequestVoteResult, error) {
			if peer == "c" && !cOnline.Load() {
				return raft.RequestVoteResult{}, errors.New("offline")
			}
			return members[peer].node.HandleRequestVote(args)
		}
	}
	appendRPC := func(_ context.Context, peer string, args raft.AppendEntriesArgs) (raft.AppendEntriesResult, error) {
		if peer == "c" && !cOnline.Load() {
			return raft.AppendEntriesResult{}, errors.New("offline")
		}
		return members[peer].node.HandleAppendEntries(args)
	}
	snapRPC := func(_ context.Context, peer string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotResult, error) {
		if peer == "c" && !cOnline.Load() {
			return raft.InstallSnapshotResult{}, errors.New("offline")
		}
		// Exercise duplicate-chunk idempotency on the first chunk.
		if args.Offset == 0 {
			if _, err := members[peer].node.HandleInstallSnapshot(args); err != nil {
				return raft.InstallSnapshotResult{}, err
			}
		}
		return members[peer].node.HandleInstallSnapshot(args)
	}
	hbRPC := func(_ context.Context, peer string, args raft.HeartbeatArgs) (raft.HeartbeatResult, error) {
		if peer == "c" && !cOnline.Load() {
			return raft.HeartbeatResult{}, errors.New("offline")
		}
		_ = members[peer].node.ObserverLeader(args.Term, args.LeaderID)
		return raft.HeartbeatResult{Term: members[peer].node.Status().CurrentTerm}, nil
	}

	for _, id := range []string{"a", "b"} {
		m := members[id]
		m.node.SetInstallSnapshotRPC(snapRPC)
		go m.node.Run(ctx, voteRPC(id))
		go m.node.RunHeartbeats(ctx, hbRPC)
		go m.node.RunReplication(ctx, appendRPC)
		go m.node.RunApply(ctx, m.sm.Apply)
	}

	// Wait for leader among a, b.
	var leader *member
	waitFor(t, 3*time.Second, "leader elected", func() bool {
		for _, id := range []string{"a", "b"} {
			if members[id].node.Status().Role == raft.Leader {
				leader = members[id]
				return true
			}
		}
		return false
	})

	// Propose 15 entries so leader compacts its log past index 1.
	for i := 1; i <= 15; i++ {
		if _, err := leader.node.Propose(ctx, proto.Command{
			Op:       proto.OpSet,
			Key:      []byte(fmt.Sprintf("k-%d", i)),
			Value:    []byte(fmt.Sprintf("val-%d", i)),
			ClientID: 7,
			Seq:      uint64(i),
		}); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	waitFor(t, 2*time.Second, "leader compacted log", func() bool {
		snap := leader.node.Snapshot()
		return snap.SnapshotIndex >= 10
	})

	// Bring follower "c" online and start its apply loop.
	cOnline.Store(true)
	go members["c"].node.RunApply(ctx, members["c"].sm.Apply)

	// Propose one more write so leader wakes peer replication immediately.
	if _, err := leader.node.Propose(ctx, proto.Command{
		Op:       proto.OpSet,
		Key:      []byte("post-snap"),
		Value:    []byte("ok"),
		ClientID: 7,
		Seq:      16,
	}); err != nil {
		t.Fatalf("propose post-snap: %v", err)
	}

	// Verify "c" installed the snapshot, dropped its divergent keys, and applied post-snap.
	waitFor(t, 5*time.Second, "follower c caught up via InstallSnapshot", func() bool {
		v, ok := members["c"].st.Get("post-snap")
		return ok && string(v) == "ok"
	})

	if members["c"].node.Snapshot().SnapshotIndex < 10 {
		t.Fatalf("c SnapshotIndex = %d, want >= 10", members["c"].node.Snapshot().SnapshotIndex)
	}
	if _, exists := members["c"].st.Get("bad"); exists {
		t.Fatal("follower c still has divergent key 'bad'")
	}
	for i := 1; i <= 15; i++ {
		v, ok := members["c"].st.Get(fmt.Sprintf("k-%d", i))
		if !ok || string(v) != fmt.Sprintf("val-%d", i) {
			t.Fatalf("c[%s] = %q (%v), want val-%d", fmt.Sprintf("k-%d", i), v, ok, i)
		}
	}

	// Verify stale AppendEntries with PrevLogIndex < SnapshotIndex is handled safely on c.
	staleRes, err := members["c"].node.HandleAppendEntries(raft.AppendEntriesArgs{
		Term:         leader.node.Status().CurrentTerm,
		LeaderID:     leader.id,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []raft.LogEntry{
			{Index: 2, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("stale"), Value: []byte("no")}},
		},
		LeaderCommit: 2,
	})
	if err != nil || !staleRes.Success {
		t.Fatalf("stale AppendEntries: res=%+v err=%v", staleRes, err)
	}
	if _, exists := members["c"].st.Get("stale"); exists {
		t.Fatal("stale AppendEntries modified snapshotted state")
	}

	// Verify RequestVote safety when log is empty after snapshot: candidate with
	// older lastLogIndex must be denied.
	voteRes, err := members["c"].node.HandleRequestVote(raft.RequestVoteArgs{
		Term:         leader.node.Status().CurrentTerm + 1,
		CandidateID:  "stale-cand",
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if voteRes.VoteGranted {
		t.Fatal("compacted follower must not grant vote to candidate behind its snapshot")
	}
}
