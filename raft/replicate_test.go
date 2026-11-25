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
