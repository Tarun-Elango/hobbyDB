package raft

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kvStore/proto"
	"kvStore/store"
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	st := store.NewStore[string, []byte]()
	go leader.RunApply(ctx, func(cmd proto.Command) {
		st.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
	})

	entry, err := leader.Propose(ctx, proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	snap := leader.Snapshot()
	if snap.CommitIndex != entry.Index || snap.LastApplied != entry.Index {
		t.Fatalf("commit=%d applied=%d, want %d", snap.CommitIndex, snap.LastApplied, entry.Index)
	}
	got, ok := st.Get("k")
	if !ok || string(got) != "v" {
		t.Fatalf("store = %q ok=%v", got, ok)
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

func TestApplyCommittedEntriesInOrder(t *testing.T) {
	node := NewNode("b")
	node.Log = []LogEntry{
		{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v1")}},
		{Index: 2, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v2")}},
		{Index: 3, Term: 1, Command: proto.Command{Op: proto.OpDel, Key: []byte("k")}},
	}
	node.CommitIndex = 2 // index 3 is in the log, but not committed, so it must stay unapplied

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := store.NewStore[string, []byte]()
	var mu sync.Mutex
	var applied []string
	go node.RunApply(ctx, func(cmd proto.Command) {
		mu.Lock()
		applied = append(applied, string(cmd.Value))
		mu.Unlock()
		switch cmd.Op {
		case proto.OpSet:
			st.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
		case proto.OpDel:
			st.Delete(string(cmd.Key))
		}
	})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if node.Snapshot().LastApplied == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := node.Snapshot().LastApplied; got != 2 {
		t.Fatalf("last applied = %d, want 2", got)
	}
	got, ok := st.Get("k")
	if !ok || string(got) != "v2" {
		t.Fatalf("store = %q ok=%v, want v2", got, ok)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(applied) != 2 || applied[0] != "v1" || applied[1] != "v2" {
		t.Fatalf("applied = %q, want [v1 v2]", applied)
	}
}

func TestProposeWaitsUntilMajorityThenApply(t *testing.T) {
	leader := newTestLeader(t, "a", 1, []string{"b", "c"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	st := store.NewStore[string, []byte]()
	go leader.RunApply(ctx, func(cmd proto.Command) {
		st.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := leader.Propose(ctx, proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")})
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("propose returned before a majority: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	if _, ok := st.Get("k"); ok {
		t.Fatal("store changed before commit")
	}

	// leader + peer b is a majority of 3.
	leader.handleAppendResult("b", 1, 1, 1, AppendEntriesResult{Term: 1, Success: true})

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	got, ok := st.Get("k")
	if !ok || string(got) != "v" {
		t.Fatalf("store = %q ok=%v", got, ok)
	}
	if applied := leader.Snapshot().LastApplied; applied != 1 {
		t.Fatalf("last applied = %d, want 1", applied)
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

// After the last write commits, the follower must learn the commit index
// without waiting for another write.
func TestFollowerLearnsCommitWithoutNewWrite(t *testing.T) {
	leader := NewNode("a")
	follower := NewNode("b")
	if err := leader.SetPeers([]string{"b"}); err != nil {
		t.Fatal(err)
	}
	leader.CurrentTerm, follower.CurrentTerm = 1, 1
	leader.Role = Leader
	leader.nextIndex = map[string]uint64{"b": 1}
	leader.matchIndex = map[string]uint64{"b": 0}

	rpc := func(ctx context.Context, peer string, args AppendEntriesArgs) (AppendEntriesResult, error) {
		return follower.HandleAppendEntries(args)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.RunReplication(ctx, rpc)
	go leader.RunApply(ctx, func(proto.Command) {})

	pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
	defer pcancel()
	if _, err := leader.Propose(pctx, proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if follower.Snapshot().CommitIndex == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("follower commit index = %d, want 1", follower.Snapshot().CommitIndex)
}

// A follower that restarts (CommitIndex back to 0) or loses its log after the
// leader already told it the commit index must catch up without a new write.
func TestRestartedFollowerCatchesUpWhileIdle(t *testing.T) {
	for _, tc := range []struct {
		name    string
		keepLog bool
	}{
		{name: "log kept, commit reset", keepLog: true},
		{name: "log wiped", keepLog: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := NewNode("a")
			if err := leader.SetPeers([]string{"b"}); err != nil {
				t.Fatal(err)
			}
			leader.CurrentTerm = 1
			leader.Role = Leader
			leader.nextIndex = map[string]uint64{"b": 1}
			leader.matchIndex = map[string]uint64{"b": 0}

			var mu sync.Mutex
			follower := NewNode("b")
			follower.CurrentTerm = 1
			current := func() *Node { mu.Lock(); defer mu.Unlock(); return follower }
			rpc := func(ctx context.Context, peer string, args AppendEntriesArgs) (AppendEntriesResult, error) {
				return current().HandleAppendEntries(args)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go leader.RunReplication(ctx, rpc)
			go leader.RunApply(ctx, func(proto.Command) {})

			pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
			defer pcancel()
			if _, err := leader.Propose(pctx, proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return current().Snapshot().CommitIndex == 1 })

			// restart the follower: same term, commit index lost, log maybe lost too
			restarted := NewNode("b")
			restarted.CurrentTerm = 1
			if tc.keepLog {
				restarted.Log = current().Snapshot().Log
			}
			mu.Lock()
			follower = restarted
			mu.Unlock()

			waitFor(t, func() bool {
				s := restarted.Snapshot()
				return s.CommitIndex == 1 && len(s.Log) == 1
			})
		})
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}

// A new leader must not serve reads until an entry from its own term has
// committed, a majority confirms leadership, and the store has applied up to it.
func TestWaitLeaderReadWaitsForCurrentTermApply(t *testing.T) {
	old := LogEntry{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")}}
	leader := newTestLeader(t, "a", 2, []string{"b"}, []LogEntry{old})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.RunApply(ctx, func(proto.Command) {})
	go leader.RunHeartbeats(ctx, func(ctx context.Context, peerID string, args HeartbeatArgs) (HeartbeatResult, error) {
		return HeartbeatResult{Term: args.Term}, nil
	})

	done := make(chan error, 1)
	go func() { done <- leader.WaitLeaderRead(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("read allowed before a current-term commit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// new term's no-op reaches a majority
	leader.mu.Lock()
	leader.Log = append(leader.Log, LogEntry{Index: 2, Term: 2, Command: proto.Command{Op: proto.OpNoop}})
	leader.matchIndex["b"] = 2
	leader.advanceCommitLocked()
	leader.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read still blocked after commit and apply")
	}
	if got := leader.Snapshot().LastApplied; got < 2 {
		t.Fatalf("read released with LastApplied = %d, want >= 2", got)
	}
}

func TestWaitLeaderReadReturnsNotLeaderOnStepDown(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b"}, []LogEntry{{Index: 1, Term: 1}})
	leader.statePath = filepath.Join(t.TempDir(), "a.raft.state")
	done := make(chan error, 1)
	go func() { done <- leader.WaitLeaderRead(context.Background()) }()
	time.Sleep(20 * time.Millisecond)

	// higher-term vote request deposes the leader (log not up to date: no vote)
	if _, err := leader.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "b"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != ErrNotLeader {
			t.Fatalf("err = %v, want ErrNotLeader", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read not released after step down")
	}
}

func TestWaitLeaderReadDeposedByHigherTermHeartbeat(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b", "c"}, []LogEntry{
		{Index: 1, Term: 2, Command: proto.Command{Op: proto.OpNoop}},
	})
	leader.statePath = filepath.Join(t.TempDir(), "a.raft.state")
	leader.mu.Lock()
	leader.setCommitLocked(1, 2)
	leader.setLastAppliedLocked(1)
	leader.mu.Unlock()

	var depose atomic.Bool
	var initialAcks atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.RunHeartbeats(ctx, func(ctx context.Context, peerID string, args HeartbeatArgs) (HeartbeatResult, error) {
		if depose.Load() {
			return HeartbeatResult{Term: 3}, nil
		}
		initialAcks.Add(1)
		return HeartbeatResult{Term: args.Term}, nil
	})
	waitFor(t, func() bool {
		leader.readMu.Lock()
		ready := leader.readStint != nil
		leader.readMu.Unlock()
		return ready && initialAcks.Load() >= 2
	})
	depose.Store(true)

	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	defer readCancel()
	if err := leader.WaitLeaderRead(readCtx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("WaitLeaderRead err = %v, want ErrNotLeader", err)
	}
	st := leader.Status()
	if st.Role != Follower || st.CurrentTerm != 3 {
		t.Fatalf("status = %+v, want Follower term 3", st)
	}
}

func TestWaitLeaderReadTimesOutWithoutQuorum(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b", "c"}, []LogEntry{
		{Index: 1, Term: 2, Command: proto.Command{Op: proto.OpNoop}},
	})
	leader.mu.Lock()
	leader.setCommitLocked(1, 2)
	leader.setLastAppliedLocked(1)
	leader.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.RunHeartbeats(ctx, func(ctx context.Context, peerID string, args HeartbeatArgs) (HeartbeatResult, error) {
		return HeartbeatResult{}, errors.New("partitioned")
	})
	waitFor(t, func() bool {
		leader.readMu.Lock()
		defer leader.readMu.Unlock()
		return leader.readStint != nil
	})

	readCtx, readCancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer readCancel()
	if err := leader.WaitLeaderRead(readCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitLeaderRead err = %v, want DeadlineExceeded", err)
	}
}

func TestWaitLeaderReadSucceedsWithMinorityDown(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b", "c", "d", "e"}, []LogEntry{
		{Index: 1, Term: 2, Command: proto.Command{Op: proto.OpNoop}},
	})
	leader.mu.Lock()
	leader.setCommitLocked(1, 2)
	leader.setLastAppliedLocked(1)
	leader.mu.Unlock()

	var deadCalls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.RunHeartbeats(ctx, func(ctx context.Context, peerID string, args HeartbeatArgs) (HeartbeatResult, error) {
		if peerID == "d" || peerID == "e" {
			deadCalls.Add(1)
			<-ctx.Done() // stalled/dead minority must not delay quorum reads
			return HeartbeatResult{}, ctx.Err()
		}
		return HeartbeatResult{Term: args.Term}, nil
	})
	waitFor(t, func() bool {
		leader.readMu.Lock()
		defer leader.readMu.Unlock()
		return leader.readStint != nil
	})

	// Multiple consecutive reads must succeed without piling up RPCs on dead peers.
	for i := 0; i < 5; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, time.Second)
		err := leader.WaitLeaderRead(readCtx)
		readCancel()
		if err != nil {
			t.Fatalf("read %d err = %v, want nil", i, err)
		}
	}
	if got := deadCalls.Load(); got > 2 {
		t.Fatalf("dead peer RPC calls = %d, want <= 2 (at most 1 in-flight per dead peer)", got)
	}
}

func TestWaitLeaderReadDoesNotCancelSlowHealthyPeer(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b", "c"}, []LogEntry{
		{Index: 1, Term: 2, Command: proto.Command{Op: proto.OpNoop}},
	})
	leader.mu.Lock()
	leader.setCommitLocked(1, 2)
	leader.setLastAppliedLocked(1)
	leader.mu.Unlock()

	var initialAcks atomic.Int32
	var slowMode atomic.Bool
	slowEntered := make(chan struct{})
	releaseSlow := make(chan struct{})
	slowDone := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.RunHeartbeats(ctx, func(ctx context.Context, peerID string, args HeartbeatArgs) (HeartbeatResult, error) {
		if !slowMode.Load() {
			initialAcks.Add(1)
			return HeartbeatResult{Term: args.Term}, nil
		}
		if peerID == "c" {
			select {
			case <-slowEntered:
			default:
				close(slowEntered)
			}
			select {
			case <-ctx.Done():
				slowDone <- ctx.Err()
				return HeartbeatResult{}, ctx.Err()
			case <-releaseSlow:
				slowDone <- nil
				return HeartbeatResult{Term: args.Term}, nil
			}
		}
		return HeartbeatResult{Term: args.Term}, nil
	})

	waitFor(t, func() bool {
		leader.readMu.Lock()
		ready := leader.readStint != nil
		leader.readMu.Unlock()
		return ready && initialAcks.Load() >= 2
	})
	slowMode.Store(true)

	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	defer readCancel()
	if err := leader.WaitLeaderRead(readCtx); err != nil {
		t.Fatalf("WaitLeaderRead err = %v, want nil", err)
	}

	// Wait until c entered the RPC (if not already) and confirm its context was
	// NOT canceled when quorum was reached via b.
	select {
	case <-slowEntered:
	case <-time.After(time.Second):
		t.Fatal("slow peer c never started read heartbeat")
	}
	close(releaseSlow)
	select {
	case err := <-slowDone:
		if err != nil {
			t.Fatalf("slow healthy peer RPC canceled after quorum: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow peer c did not finish")
	}
}

func TestWaitLeaderReadDoesNotBlockOnMu(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b", "c"}, []LogEntry{
		{Index: 1, Term: 2, Command: proto.Command{Op: proto.OpNoop}},
	})
	leader.mu.Lock()
	leader.setCommitLocked(1, 2)
	leader.setLastAppliedLocked(1)
	leader.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.RunHeartbeats(ctx, func(ctx context.Context, peerID string, args HeartbeatArgs) (HeartbeatResult, error) {
		return HeartbeatResult{Term: args.Term}, nil
	})
	waitFor(t, func() bool {
		leader.readMu.Lock()
		defer leader.readMu.Unlock()
		return leader.readStint != nil
	})

	// Hold n.mu.Lock() to simulate an in-flight WAL fsync in Propose.
	leader.mu.Lock()
	defer leader.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		readCtx, readCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer readCancel()
		done <- leader.WaitLeaderRead(readCtx)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitLeaderRead err = %v, want nil", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("WaitLeaderRead blocked while n.mu.Lock() was held")
	}
}

func TestWaitLeaderReadCoalescesWithoutReusingInFlightRound(t *testing.T) {
	leader := newTestLeader(t, "a", 2, []string{"b"}, []LogEntry{
		{Index: 1, Term: 2, Command: proto.Command{Op: proto.OpNoop}},
	})
	leader.mu.Lock()
	leader.setCommitLocked(1, 2)
	leader.setLastAppliedLocked(1)
	leader.mu.Unlock()

	var readPhase atomic.Bool
	var calls atomic.Int32
	initialDone := make(chan struct{})
	round1Started := make(chan struct{})
	releaseRound1 := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.RunHeartbeats(ctx, func(ctx context.Context, peerID string, args HeartbeatArgs) (HeartbeatResult, error) {
		if !readPhase.Load() {
			select {
			case <-initialDone:
			default:
				close(initialDone)
			}
			return HeartbeatResult{Term: args.Term}, nil
		}
		n := calls.Add(1)
		if n == 1 {
			close(round1Started)
			<-releaseRound1
		}
		return HeartbeatResult{Term: args.Term}, nil
	})

	<-initialDone
	waitFor(t, func() bool {
		leader.readMu.Lock()
		defer leader.readMu.Unlock()
		return leader.readStint != nil
	})
	readPhase.Store(true)

	err1 := make(chan error, 1)
	go func() { err1 <- leader.WaitLeaderRead(ctx) }()
	<-round1Started

	// Launch 4 readers while round 1 is already in flight. None may complete on
	// round 1, and all 4 must share a single round 2.
	const waiters = 4
	errNext := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() { errNext <- leader.WaitLeaderRead(ctx) }()
	}
	waitFor(t, func() bool {
		leader.readMu.Lock()
		defer leader.readMu.Unlock()
		return leader.readStint != nil && leader.readStint.pending != nil && leader.readStint.pending.waiters == waiters
	})

	close(releaseRound1)
	if err := <-err1; err != nil {
		t.Fatalf("round 1 err = %v", err)
	}
	for i := 0; i < waiters; i++ {
		if err := <-errNext; err != nil {
			t.Fatalf("coalesced reader %d err = %v", i, err)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("heartbeat rounds = %d, want 2 (1 initial + 1 coalesced)", got)
	}
}

func TestProposeGroupCommitCoalescesAndRespectsMaxBatchSize(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state")
	logPath := filepath.Join(dir, "raft.log")

	leader, err := OpenNode("a", statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.SetPersistentState(1, "a"); err != nil {
		t.Fatal(err)
	}
	if err := leader.OpenLog(logPath); err != nil {
		t.Fatal(err)
	}
	defer leader.CloseLog()
	leader.mu.Lock()
	leader.setMetaLocked(Leader, "a")
	leader.mu.Unlock()

	origLimit := proposeMaxBatchSize
	proposeMaxBatchSize = 4
	t.Cleanup(func() { proposeMaxBatchSize = origLimit })

	var batchMu sync.Mutex
	var batchSizes []int
	leader.onProposeBatch = func(n int) {
		batchMu.Lock()
		batchSizes = append(batchSizes, n)
		batchMu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	st := store.NewStore[string, []byte]()
	go leader.RunApply(ctx, func(cmd proto.Command) {
		st.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
	})

	// Hold n.mu to simulate an in-flight WAL fsync for proposal 0 while proposals 1..9 queue up.
	leader.mu.Lock()

	const total = 10 // 1 in-flight + 9 queued -> batches [1, 4, 4, 1]
	results := make(chan proposeResult, total)
	// Start proposal 0 first so it becomes the active flusher.
	go func() {
		e, err := leader.Propose(ctx, proto.Command{Op: proto.OpSet, Key: []byte("k0"), Value: []byte("v0")})
		results <- proposeResult{entry: e, err: err}
	}()
	waitFor(t, func() bool {
		leader.proposeMu.Lock()
		defer leader.proposeMu.Unlock()
		return leader.proposeFlushing
	})

	for i := 1; i < total; i++ {
		i := i
		go func() {
			k := []byte{byte('a' + i)}
			v := []byte{byte('A' + i)}
			e, err := leader.Propose(ctx, proto.Command{Op: proto.OpSet, Key: k, Value: v})
			results <- proposeResult{entry: e, err: err}
		}()
	}
	waitFor(t, func() bool {
		leader.proposeMu.Lock()
		defer leader.proposeMu.Unlock()
		return len(leader.proposeQueue) == total-1
	})

	// Release n.mu so the first flush and opportunistic drain batches run.
	leader.mu.Unlock()

	seenIndices := make(map[uint64]bool, total)
	for i := 0; i < total; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("propose %d failed: %v", i, res.err)
		}
		if seenIndices[res.entry.Index] {
			t.Fatalf("duplicate log index %d", res.entry.Index)
		}
		seenIndices[res.entry.Index] = true
	}

	batchMu.Lock()
	gotBatches := append([]int(nil), batchSizes...)
	batchMu.Unlock()
	wantBatches := []int{1, 4, 4, 1}
	if len(gotBatches) != len(wantBatches) {
		t.Fatalf("batch sizes = %v, want %v", gotBatches, wantBatches)
	}
	for i := range wantBatches {
		if gotBatches[i] != wantBatches[i] {
			t.Fatalf("batch sizes = %v, want %v", gotBatches, wantBatches)
		}
	}

	// Verify WAL durability across reopen.
	if err := leader.CloseLog(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNode("a", statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.OpenLog(logPath); err != nil {
		t.Fatal(err)
	}
	defer reopened.CloseLog()
	if got := len(reopened.Snapshot().Log); got != total {
		t.Fatalf("reopened log len = %d, want %d", got, total)
	}
}

func TestProposeGroupCommitSkipsCanceledQueuedRequests(t *testing.T) {
	leader := NewNode("a")
	leader.mu.Lock()
	leader.setMetaLocked(Leader, "a")
	leader.CurrentTerm = 1
	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go leader.RunApply(ctx, func(proto.Command) {})

	leader.mu.Lock()
	res0 := make(chan error, 1)
	go func() {
		_, err := leader.Propose(ctx, proto.Command{Op: proto.OpSet, Key: []byte("k0"), Value: []byte("v0")})
		res0 <- err
	}()
	waitFor(t, func() bool {
		leader.proposeMu.Lock()
		defer leader.proposeMu.Unlock()
		return leader.proposeFlushing
	})

	cancelCtx, cancelReq := context.WithCancel(ctx)
	resCanceled := make(chan error, 1)
	go func() {
		_, err := leader.Propose(cancelCtx, proto.Command{Op: proto.OpSet, Key: []byte("drop"), Value: []byte("x")})
		resCanceled <- err
	}()
	resLive := make(chan error, 1)
	go func() {
		_, err := leader.Propose(ctx, proto.Command{Op: proto.OpSet, Key: []byte("keep"), Value: []byte("y")})
		resLive <- err
	}()
	waitFor(t, func() bool {
		leader.proposeMu.Lock()
		defer leader.proposeMu.Unlock()
		return len(leader.proposeQueue) == 2
	})

	// Cancel one queued proposal before the flush runs.
	cancelReq()
	if err := <-resCanceled; !errors.Is(err, context.Canceled) {
		leader.mu.Unlock()
		t.Fatalf("canceled propose err = %v, want context.Canceled", err)
	}

	leader.mu.Unlock()

	if err := <-res0; err != nil {
		t.Fatalf("propose 0 err = %v", err)
	}
	if err := <-resLive; err != nil {
		t.Fatalf("live propose err = %v", err)
	}

	log := leader.Snapshot().Log
	if len(log) != 2 {
		t.Fatalf("log len = %d, want 2 (canceled queued proposal should not be logged): %+v", len(log), log)
	}
	if string(log[1].Command.Key) != "keep" {
		t.Fatalf("log[1].Key = %q, want keep", log[1].Command.Key)
	}
}

func TestProposeGroupCommitFailsQueuedOnStepDown(t *testing.T) {
	leader := NewNode("a")
	leader.mu.Lock()
	leader.setMetaLocked(Leader, "a")
	leader.CurrentTerm = 1
	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	leader.mu.Lock()
	errCh := make(chan error, 3)
	go func() {
		_, err := leader.Propose(ctx, proto.Command{Op: proto.OpSet, Key: []byte("k0"), Value: []byte("v0")})
		errCh <- err
	}()
	waitFor(t, func() bool {
		leader.proposeMu.Lock()
		defer leader.proposeMu.Unlock()
		return leader.proposeFlushing
	})
	for i := 1; i < 3; i++ {
		go func() {
			_, err := leader.Propose(ctx, proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")})
			errCh <- err
		}()
	}
	waitFor(t, func() bool {
		leader.proposeMu.Lock()
		defer leader.proposeMu.Unlock()
		return len(leader.proposeQueue) == 2
	})

	// Step down while proposals are waiting to flush.
	leader.setMetaLocked(Follower, "b")
	leader.mu.Unlock()

	for i := 0; i < 3; i++ {
		if err := <-errCh; !errors.Is(err, ErrNotLeader) {
			t.Fatalf("propose %d err = %v, want ErrNotLeader", i, err)
		}
	}
	if got := len(leader.Snapshot().Log); got != 0 {
		t.Fatalf("log len = %d, want 0", got)
	}
}

type orderedMockPipeline struct {
	mu      sync.Mutex
	respCh  chan AppendPipelineResponse
	onSend  func(ctx context.Context, args AppendEntriesArgs, reply func(AppendEntriesResult, error)) error
	onReset func()
}

func newOrderedMockPipeline(onSend func(ctx context.Context, args AppendEntriesArgs, reply func(AppendEntriesResult, error)) error) *orderedMockPipeline {
	return &orderedMockPipeline{
		respCh: make(chan AppendPipelineResponse, 64),
		onSend: onSend,
	}
}

func (p *orderedMockPipeline) Send(ctx context.Context, args AppendEntriesArgs, meta any) error {
	return p.onSend(ctx, args, func(res AppendEntriesResult, err error) {
		p.respCh <- AppendPipelineResponse{Meta: meta, Res: res, Err: err}
	})
}

func (p *orderedMockPipeline) Responses() <-chan AppendPipelineResponse {
	return p.respCh
}

func (p *orderedMockPipeline) Reset() {
	p.mu.Lock()
	fn := p.onReset
	p.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func TestPipelinedReplicationProbeThenMaxInFlight(t *testing.T) {
	origBatch := appendEntriesBatchLimit
	origMax := appendMaxInFlight
	appendEntriesBatchLimit = 2
	appendMaxInFlight = 4
	t.Cleanup(func() {
		appendEntriesBatchLimit = origBatch
		appendMaxInFlight = origMax
	})

	entries := make([]LogEntry, 12) // 6 batches of 2
	for i := range entries {
		entries[i] = LogEntry{
			Index:   uint64(i + 1),
			Term:    1,
			Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")},
		}
	}
	leader := newTestLeader(t, "a", 1, []string{"b"}, entries)
	leader.nextIndex["b"] = 1
	follower := NewNode("b")
	follower.CurrentTerm = 1

	var (
		mu             sync.Mutex
		inFlight       int
		maxObserved    int
		probeChecked   bool
		pendingReplies []func()
		probeArrived   = make(chan struct{}, 1)
		fourInFlight   = make(chan struct{}, 1)
	)

	pipe := newOrderedMockPipeline(func(ctx context.Context, args AppendEntriesArgs, reply func(AppendEntriesResult, error)) error {
		mu.Lock()
		inFlight++
		cur := inFlight
		if cur > maxObserved {
			maxObserved = cur
		}
		isProbe := !probeChecked
		if isProbe {
			select {
			case probeArrived <- struct{}{}:
			default:
			}
			pendingReplies = append(pendingReplies, func() {
				res, err := follower.HandleAppendEntries(args)
				mu.Lock()
				inFlight--
				mu.Unlock()
				reply(res, err)
			})
			mu.Unlock()
			return nil
		}
		if cur == 4 {
			select {
			case fourInFlight <- struct{}{}:
			default:
			}
		}
		// Hold batches 2..5 until 4 are simultaneously in flight, then release cleanly.
		if cur <= 4 && len(args.Entries) > 0 && args.Entries[0].Index <= 9 {
			pendingReplies = append(pendingReplies, func() {
				res, err := follower.HandleAppendEntries(args)
				mu.Lock()
				inFlight--
				mu.Unlock()
				reply(res, err)
			})
			mu.Unlock()
			return nil
		}
		mu.Unlock()

		res, err := follower.HandleAppendEntries(args)
		mu.Lock()
		inFlight--
		mu.Unlock()
		reply(res, err)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	repDone := make(chan struct{})
	go func() {
		defer close(repDone)
		leader.RunReplicationPipelined(ctx, func(peerID string) (AppendPipeline, error) {
			return pipe, nil
		})
	}()
	defer func() {
		cancel()
		<-repDone
	}()

	// 1) Wait for initial probe; while it is in flight, no second batch may be sent.
	select {
	case <-probeArrived:
	case <-time.After(time.Second):
		t.Fatal("initial probe not sent")
	}
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	if inFlight != 1 {
		mu.Unlock()
		t.Fatalf("in-flight during probe = %d, want 1", inFlight)
	}
	probeChecked = true
	probeReply := pendingReplies[0]
	pendingReplies = pendingReplies[1:]
	mu.Unlock()

	// 2) Complete probe -> leader should enter modeReplicate and fill 4 in-flight slots.
	probeReply()
	select {
	case <-fourInFlight:
	case <-time.After(time.Second):
		mu.Lock()
		got := maxObserved
		mu.Unlock()
		t.Fatalf("expected 4 concurrent in-flight batches after probe, maxObserved = %d", got)
	}

	// Verify we never exceed appendMaxInFlight.
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if maxObserved != 4 {
		mu.Unlock()
		t.Fatalf("maxObserved = %d, want 4", maxObserved)
	}
	toRelease := append([]func(){}, pendingReplies...)
	pendingReplies = nil
	mu.Unlock()

	// Release held batches in FIFO order.
	for _, fn := range toRelease {
		fn()
	}

	waitFor(t, func() bool {
		s := follower.Snapshot()
		return len(s.Log) == 12 && s.CommitIndex == 12
	})
}

func TestPipelinedReplicationUpdatesSentCommitOnNonEmptyAppend(t *testing.T) {
	origBatch := appendEntriesBatchLimit
	origMax := appendMaxInFlight
	origRefresh := commitRefreshInterval
	appendEntriesBatchLimit = 2
	appendMaxInFlight = 4
	commitRefreshInterval = 10 * time.Second // keep idle refresh out of the way
	t.Cleanup(func() {
		appendEntriesBatchLimit = origBatch
		appendMaxInFlight = origMax
		commitRefreshInterval = origRefresh
	})

	// Leader has 4 entries, and CommitIndex is ALREADY 4 (e.g. committed via another peer).
	entries := make([]LogEntry, 4)
	for i := range entries {
		entries[i] = LogEntry{
			Index:   uint64(i + 1),
			Term:    1,
			Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")},
		}
	}
	leader := newTestLeader(t, "a", 1, []string{"b", "c"}, entries)
	leader.mu.Lock()
	leader.nextIndex["b"] = 1
	leader.matchIndex["b"] = 0
	leader.nextIndex["c"] = 5
	leader.matchIndex["c"] = 4
	leader.setCommitLocked(4, 1)
	leader.mu.Unlock()

	followerB := NewNode("b")
	followerB.CurrentTerm = 1

	var (
		mu           sync.Mutex
		emptyToB     int
		nonEmptyToB  int
	)

	ctx, cancel := context.WithCancel(context.Background())
	repDone := make(chan struct{})
	go func() {
		defer close(repDone)
		leader.RunReplicationPipelined(ctx, func(peerID string) (AppendPipeline, error) {
			return newOrderedMockPipeline(func(ctx context.Context, args AppendEntriesArgs, reply func(AppendEntriesResult, error)) error {
				if peerID == "c" {
					reply(AppendEntriesResult{Term: 1, Success: true}, nil)
					return nil
				}
				mu.Lock()
				if len(args.Entries) == 0 {
					emptyToB++
				} else {
					nonEmptyToB++
				}
				mu.Unlock()
				res, err := followerB.HandleAppendEntries(args)
				reply(res, err)
				return nil
			}), nil
		})
	}()
	defer func() {
		cancel()
		<-repDone
	}()

	// Peer b catches up in 2 non-empty batches:
	//   batch 1: [1..2] with LeaderCommit=4 -> effectiveCommit = min(4,2) = 2
	//   batch 2: [3..4] with LeaderCommit=4 -> effectiveCommit = min(4,4) = 4
	// Because sentCommit is updated on non-empty AppendEntries, NO empty commit-only
	// RPC should be sent to b once batch 2 succeeds.
	waitFor(t, func() bool {
		s := followerB.Snapshot()
		return len(s.Log) == 4 && s.CommitIndex == 4
	})
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	gotEmpty := emptyToB
	gotNonEmpty := nonEmptyToB
	mu.Unlock()
	if gotNonEmpty != 2 {
		t.Fatalf("non-empty RPCs to b = %d, want 2", gotNonEmpty)
	}
	if gotEmpty != 0 {
		t.Fatalf("empty commit-only RPCs to b = %d, want 0 (non-empty AppendEntries already carried CommitIndex=4)", gotEmpty)
	}
}

func TestPipelinedReplicationMismatchRewindsOnceAndIgnoresStaleReplies(t *testing.T) {
	origBatch := appendEntriesBatchLimit
	origMax := appendMaxInFlight
	appendEntriesBatchLimit = 2
	appendMaxInFlight = 4
	t.Cleanup(func() {
		appendEntriesBatchLimit = origBatch
		appendMaxInFlight = origMax
	})

	entries := make([]LogEntry, 10)
	for i := range entries {
		entries[i] = LogEntry{
			Index:   uint64(i + 1),
			Term:    1,
			Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")},
		}
	}
	leader := newTestLeader(t, "a", 1, []string{"b"}, entries)
	leader.nextIndex["b"] = 1

	follower := NewNode("b")
	follower.CurrentTerm = 1

	var (
		mu             sync.Mutex
		wipedOnce      bool
		held           []func()
		threeHeld      = make(chan struct{}, 1)
		rejectedFirst  bool
		probeAfterWipe bool
		maxAfterWipe   int
		curAfterWipe   int
	)

	pipe := newOrderedMockPipeline(func(ctx context.Context, args AppendEntriesArgs, reply func(AppendEntriesResult, error)) error {
		mu.Lock()
		// Let probe [1..2] succeed so leader enters modeReplicate.
		if !wipedOnce {
			if len(args.Entries) > 0 && args.Entries[0].Index == 1 {
				wipedOnce = true
				mu.Unlock()
				res, err := follower.HandleAppendEntries(args)
				reply(res, err)
				return nil
			}
		}

		// Simulate follower wiping its log right after probe [1..2] succeeded.
		// Hold the 3 pipelined batches [3..4], [5..6], [7..8] so all 3 are in flight
		// when batch [3..4] fails.
		if !rejectedFirst && len(args.Entries) > 0 && args.Entries[0].Index >= 3 {
			held = append(held, func() {
				res, err := follower.HandleAppendEntries(args)
				reply(res, err)
			})
			if len(held) == 3 {
				follower = NewNode("b")
				follower.CurrentTerm = 1
				rejectedFirst = true
				select {
				case threeHeld <- struct{}{}:
				default:
				}
			}
			mu.Unlock()
			return nil
		}

		// After the mismatch rewind, track in-flight concurrency during recovery.
		if probeAfterWipe {
			curAfterWipe++
			if curAfterWipe > maxAfterWipe {
				maxAfterWipe = curAfterWipe
			}
		}
		mu.Unlock()

		res, err := follower.HandleAppendEntries(args)
		mu.Lock()
		if probeAfterWipe {
			curAfterWipe--
		}
		mu.Unlock()
		reply(res, err)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	repDone := make(chan struct{})
	go func() {
		defer close(repDone)
		leader.RunReplicationPipelined(ctx, func(peerID string) (AppendPipeline, error) {
			return pipe, nil
		})
	}()
	defer func() {
		cancel()
		<-repDone
	}()

	select {
	case <-threeHeld:
	case <-time.After(time.Second):
		t.Fatal("expected 3 pipelined batches held before wipe")
	}

	mu.Lock()
	toFail := append([]func(){}, held...)
	held = nil
	probeAfterWipe = true
	mu.Unlock()

	// All 3 fail against the wiped follower; only the first should rewind nextIndex
	// (the remaining 2 are stale generation and must be ignored).
	for _, fn := range toFail {
		fn()
	}

	waitFor(t, func() bool {
		mu.Lock()
		f := follower
		mu.Unlock()
		s := f.Snapshot()
		return len(s.Log) == 10 && s.CommitIndex == 10
	})
}

func TestPipelinedReplicationHigherTermOnStaleReplyStepsDown(t *testing.T) {
	origBatch := appendEntriesBatchLimit
	origMax := appendMaxInFlight
	appendEntriesBatchLimit = 1
	appendMaxInFlight = 2
	t.Cleanup(func() {
		appendEntriesBatchLimit = origBatch
		appendMaxInFlight = origMax
	})

	entries := []LogEntry{
		{Index: 1, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k1"), Value: []byte("v1")}},
		{Index: 2, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k2"), Value: []byte("v2")}},
		{Index: 3, Term: 1, Command: proto.Command{Op: proto.OpSet, Key: []byte("k3"), Value: []byte("v3")}},
	}
	leader := newTestLeader(t, "a", 1, []string{"b"}, entries)
	leader.statePath = filepath.Join(t.TempDir(), "a.raft.state")
	leader.nextIndex["b"] = 1

	var (
		mu       sync.Mutex
		calls    int
		reply2   func(AppendEntriesResult, error)
		reply3   func(AppendEntriesResult, error)
		twoReady = make(chan struct{}, 1)
	)

	pipe := newOrderedMockPipeline(func(ctx context.Context, args AppendEntriesArgs, reply func(AppendEntriesResult, error)) error {
		mu.Lock()
		calls++
		n := calls
		if n == 1 {
			mu.Unlock()
			reply(AppendEntriesResult{Term: 1, Success: true}, nil)
			return nil
		}
		if n == 2 {
			reply2 = reply
			mu.Unlock()
			return nil
		}
		if n == 3 {
			reply3 = reply
			select {
			case twoReady <- struct{}{}:
			default:
			}
			mu.Unlock()
			return nil
		}
		mu.Unlock()
		reply(AppendEntriesResult{Term: 1, Success: false}, nil)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	repDone := make(chan struct{})
	go func() {
		defer close(repDone)
		leader.RunReplicationPipelined(ctx, func(peerID string) (AppendPipeline, error) {
			return pipe, nil
		})
	}()
	defer func() {
		cancel()
		<-repDone
	}()

	select {
	case <-twoReady:
	case <-time.After(time.Second):
		t.Fatal("expected 2 pipelined calls in flight")
	}

	// Reject #2 (bumps generation), then send a higher term on stale #3.
	reply2(AppendEntriesResult{Term: 1, Success: false}, nil)
	time.Sleep(15 * time.Millisecond)
	reply3(AppendEntriesResult{Term: 5, Success: false}, nil)

	waitFor(t, func() bool {
		st := leader.Status()
		return st.Role == Follower && st.CurrentTerm == 5
	})
}

func TestPipelinedReplicationTransportErrorRewindsAndRecovers(t *testing.T) {
	origBatch := appendEntriesBatchLimit
	origMax := appendMaxInFlight
	origBackoff := appendRetryBackoff
	appendEntriesBatchLimit = 2
	appendMaxInFlight = 4
	appendRetryBackoff = 10 * time.Millisecond
	t.Cleanup(func() {
		appendEntriesBatchLimit = origBatch
		appendMaxInFlight = origMax
		appendRetryBackoff = origBackoff
	})

	entries := make([]LogEntry, 8)
	for i := range entries {
		entries[i] = LogEntry{
			Index:   uint64(i + 1),
			Term:    1,
			Command: proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")},
		}
	}
	leader := newTestLeader(t, "a", 1, []string{"b"}, entries)
	leader.nextIndex["b"] = 1
	follower := NewNode("b")
	follower.CurrentTerm = 1

	var (
		mu      sync.Mutex
		dropped bool
	)

	pipe := newOrderedMockPipeline(func(ctx context.Context, args AppendEntriesArgs, reply func(AppendEntriesResult, error)) error {
		mu.Lock()
		shouldDrop := !dropped && len(args.Entries) > 0 && args.Entries[0].Index == 3
		if shouldDrop {
			dropped = true
		}
		mu.Unlock()
		if shouldDrop {
			reply(AppendEntriesResult{}, errors.New("connection reset"))
			return nil
		}
		res, err := follower.HandleAppendEntries(args)
		reply(res, err)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	repDone := make(chan struct{})
	go func() {
		defer close(repDone)
		leader.RunReplicationPipelined(ctx, func(peerID string) (AppendPipeline, error) {
			return pipe, nil
		})
	}()
	defer func() {
		cancel()
		<-repDone
	}()

	waitFor(t, func() bool {
		s := follower.Snapshot()
		return len(s.Log) == 8 && s.CommitIndex == 8
	})
}
