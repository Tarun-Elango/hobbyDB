package raft

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// While n.mu.Lock() is held (simulating a blocked WAL fsync in Propose or
// HandleAppendEntries), Status(), steady-state ObserverLeader, and stale-term
// ObserverLeader must return immediately without blocking on n.mu.
func TestStatusAndSteadyStateObserverLeaderDoNotBlockOnMu(t *testing.T) {
	node, err := OpenNode("b", filepath.Join(t.TempDir(), "b.state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := node.SetPersistentState(3, ""); err != nil {
		t.Fatal(err)
	}
	if err := node.ObserverLeader(3, "a"); err != nil {
		t.Fatal(err)
	}
	// Drain any poke from setup.
	select {
	case <-node.electionReset:
	default:
	}

	// Hold n.mu.Lock() to simulate an in-flight WAL fsync.
	node.mu.Lock()
	muHeld := true
	defer func() {
		if muHeld {
			node.mu.Unlock()
		}
	}()

	// 1. Status() must return immediately while n.mu is held.
	statusDone := make(chan NodeStatus, 1)
	go func() {
		statusDone <- node.Status()
	}()
	select {
	case st := <-statusDone:
		if st.Role != Follower || st.CurrentTerm != 3 || st.LeaderID != "a" {
			t.Fatalf("Status() = %+v, want Follower term=3 leader=a", st)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Status() blocked while n.mu.Lock() was held")
	}

	// 2. Stale-term ObserverLeader must return immediately without poking electionReset or changing LeaderID.
	staleDone := make(chan error, 1)
	go func() {
		staleDone <- node.ObserverLeader(2, "stale-leader")
	}()
	select {
	case err := <-staleDone:
		if err != nil {
			t.Fatalf("stale ObserverLeader error = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stale ObserverLeader blocked while n.mu.Lock() was held")
	}
	select {
	case <-node.electionReset:
		t.Fatal("stale ObserverLeader poked electionReset")
	default:
	}
	if got := node.Status().LeaderID; got != "a" {
		t.Fatalf("LeaderID after stale heartbeat = %q, want %q", got, "a")
	}

	// 3. Steady-state same-term ObserverLeader must return immediately, update LeaderID, and poke electionReset.
	obsDone := make(chan error, 1)
	go func() {
		obsDone <- node.ObserverLeader(3, "a-updated")
	}()
	select {
	case err := <-obsDone:
		if err != nil {
			t.Fatalf("steady-state ObserverLeader error = %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("steady-state ObserverLeader blocked while n.mu.Lock() was held")
	}

	select {
	case <-node.electionReset:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("steady-state ObserverLeader did not poke electionReset")
	}

	if st := node.Status(); st.Role != Follower || st.CurrentTerm != 3 || st.LeaderID != "a-updated" {
		t.Fatalf("Status() after ObserverLeader = %+v, want Follower term=3 leader=a-updated", st)
	}

	// Release n.mu and verify Snapshot() sees the fast-path LeaderID update cleanly.
	node.mu.Unlock()
	muHeld = false

	snap := node.Snapshot()
	if snap.Role != Follower || snap.CurrentTerm != 3 || snap.LeaderID != "a-updated" {
		t.Fatalf("Snapshot() = %+v, want Follower term=3 leader=a-updated", snap)
	}
}

// Higher-term ObserverLeader or ObserverLeader while Candidate/Leader must
// fall through to the full path: persist higher terms, transition to Follower,
// broadcast applyCond when stepping down from Leader, and poke electionReset.
func TestObserverLeaderTransitionsAndPersistence(t *testing.T) {
	t.Run("leader_steps_down_on_higher_term_persists_and_broadcasts_applyCond", func(t *testing.T) {
		statePath := filepath.Join(t.TempDir(), "a.state")
		leader, err := OpenNode("a", statePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := leader.SetPeers([]string{"b", "c"}); err != nil {
			t.Fatal(err)
		}
		if err := leader.SetPersistentState(2, "a"); err != nil {
			t.Fatal(err)
		}
		leader.mu.Lock()
		leader.setMetaLocked(Candidate, "")
		leader.mu.Unlock()
		leader.becomeLeader(2)

		// Drain any electionReset from setup.
		select {
		case <-leader.electionReset:
		default:
		}

		// Start a WaitLeaderRead blocked on applyCond (the no-op at term 2 is uncommitted).
		readErr := make(chan error, 1)
		go func() {
			readErr <- leader.WaitLeaderRead(context.Background())
		}()
		time.Sleep(20 * time.Millisecond)

		if err := leader.ObserverLeader(5, "b"); err != nil {
			t.Fatalf("ObserverLeader(5, b) = %v", err)
		}

		select {
		case err := <-readErr:
			if err != ErrNotLeader {
				t.Fatalf("WaitLeaderRead err = %v, want ErrNotLeader", err)
			}
		case <-time.After(time.Second):
			t.Fatal("WaitLeaderRead not woken by applyCond.Broadcast() on leader step-down")
		}

		select {
		case <-leader.electionReset:
		default:
			t.Fatal("leader step-down via ObserverLeader did not poke electionReset")
		}

		st := leader.Status()
		if st.Role != Follower || st.CurrentTerm != 5 || st.LeaderID != "b" {
			t.Fatalf("Status() = %+v, want Follower term=5 leader=b", st)
		}
		snap := leader.Snapshot()
		if snap.Role != Follower || snap.CurrentTerm != 5 || snap.LeaderID != "b" || snap.VotedFor != "" {
			t.Fatalf("Snapshot() = %+v, want Follower term=5 leader=b votedFor=''", snap)
		}

		// Verify the higher term was persisted to disk.
		reopened, err := OpenNode("a", statePath)
		if err != nil {
			t.Fatal(err)
		}
		if got := reopened.Status().CurrentTerm; got != 5 {
			t.Fatalf("reopened CurrentTerm = %d, want 5", got)
		}
	})

	t.Run("candidate_steps_down_on_same_term_heartbeat", func(t *testing.T) {
		statePath := filepath.Join(t.TempDir(), "b.state")
		cand, err := OpenNode("b", statePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := cand.SetPersistentState(4, "b"); err != nil {
			t.Fatal(err)
		}
		cand.mu.Lock()
		cand.setMetaLocked(Candidate, "")
		cand.mu.Unlock()

		select {
		case <-cand.electionReset:
		default:
		}

		if err := cand.ObserverLeader(4, "a"); err != nil {
			t.Fatalf("ObserverLeader(4, a) = %v", err)
		}

		st := cand.Status()
		if st.Role != Follower || st.CurrentTerm != 4 || st.LeaderID != "a" {
			t.Fatalf("Status() = %+v, want Follower term=4 leader=a", st)
		}
		select {
		case <-cand.electionReset:
		default:
			t.Fatal("candidate step-down via ObserverLeader did not poke electionReset")
		}
	})

	t.Run("follower_advances_term_on_higher_term_heartbeat", func(t *testing.T) {
		statePath := filepath.Join(t.TempDir(), "c.state")
		follower, err := OpenNode("c", statePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := follower.SetPersistentState(2, "a"); err != nil {
			t.Fatal(err)
		}

		if err := follower.ObserverLeader(6, "b"); err != nil {
			t.Fatalf("ObserverLeader(6, b) = %v", err)
		}

		st := follower.Status()
		if st.Role != Follower || st.CurrentTerm != 6 || st.LeaderID != "b" {
			t.Fatalf("Status() = %+v, want Follower term=6 leader=b", st)
		}
		reopened, err := OpenNode("c", statePath)
		if err != nil {
			t.Fatal(err)
		}
		if got := reopened.Status().CurrentTerm; got != 6 {
			t.Fatalf("reopened CurrentTerm = %d, want 6", got)
		}
	})
}

// Concurrent Status, Snapshot, and steady-state ObserverLeader must be race-free.
func TestConcurrentStatusSnapshotAndObserverLeader(t *testing.T) {
	node, err := OpenNode("b", filepath.Join(t.TempDir(), "b.state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := node.SetPersistentState(3, ""); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			leader := "a"
			if id%2 == 1 {
				leader = "c"
			}
			for j := 0; j < 200; j++ {
				_ = node.ObserverLeader(3, leader)
				_ = node.Status()
				_ = node.Snapshot()
			}
		}(i)
	}
	wg.Wait()
}

// Holding n.mu.Lock() longer than electionTimeoutMax (simulating a very slow
// WAL fsync >300ms) while periodic heartbeats arrive must never allow the
// follower's Run loop to bump CurrentTerm or send any RequestVote RPC.
func TestHeartbeatsPreventElectionWhileMuHeldLongerThanElectionTimeout(t *testing.T) {
	node, err := OpenNode("b", filepath.Join(t.TempDir(), "b.state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := node.SetPeers([]string{"a", "c"}); err != nil {
		t.Fatal(err)
	}
	if err := node.SetPersistentState(3, ""); err != nil {
		t.Fatal(err)
	}
	if err := node.ObserverLeader(3, "a"); err != nil {
		t.Fatal(err)
	}

	var voteCalls atomic.Int32
	voteRPC := func(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteResult, error) {
		voteCalls.Add(1)
		return RequestVoteResult{Term: args.Term, VoteGranted: false}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		node.Run(ctx, voteRPC)
	}()

	// Hold n.mu.Lock() longer than electionTimeoutMax while heartbeats continue arriving.
	node.mu.Lock()
	hbInterval := electionTimeoutMin / 3
	if hbInterval < 10*time.Millisecond {
		hbInterval = 10 * time.Millisecond
	}
	holdDuration := electionTimeoutMax + 150*time.Millisecond
	deadline := time.Now().Add(holdDuration)
	ticker := time.NewTicker(hbInterval)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if err := node.ObserverLeader(3, "a"); err != nil {
			node.mu.Unlock()
			cancel()
			<-runDone
			t.Fatalf("ObserverLeader during held mu: %v", err)
		}
		<-ticker.C
	}
	// Send one final heartbeat right before releasing mu and stopping Run.
	if err := node.ObserverLeader(3, "a"); err != nil {
		node.mu.Unlock()
		cancel()
		<-runDone
		t.Fatalf("final ObserverLeader: %v", err)
	}
	node.mu.Unlock()

	cancel()
	<-runDone

	if got := voteCalls.Load(); got != 0 {
		t.Fatalf("RequestVote RPCs sent = %d, want 0", got)
	}
	snap := node.Snapshot()
	if snap.CurrentTerm != 3 || snap.Role != Follower || snap.VotedFor != "" {
		t.Fatalf("Snapshot() = %+v, want Follower term=3 votedFor=''", snap)
	}
}

// If timer.C fires while n.mu.Lock() is held (so startElection blocks waiting
// for n.mu), and a valid heartbeat or AppendEntries arrives before n.mu is
// released, startElection must abort without bumping CurrentTerm or sending votes.
func TestStartElectionAbortsIfHeardWhileWaitingOnMu(t *testing.T) {
	t.Run("heartbeat_arrives_while_startElection_blocked_on_mu", func(t *testing.T) {
		node, err := OpenNode("b", filepath.Join(t.TempDir(), "b.state"))
		if err != nil {
			t.Fatal(err)
		}
		if err := node.SetPeers([]string{"a", "c"}); err != nil {
			t.Fatal(err)
		}
		if err := node.SetPersistentState(3, ""); err != nil {
			t.Fatal(err)
		}

		var voteCalls atomic.Int32
		voteRPC := func(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteResult, error) {
			voteCalls.Add(1)
			return RequestVoteResult{Term: args.Term, VoteGranted: false}, nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		runDone := make(chan struct{})

		// Hold n.mu.Lock() before starting Run so when timer.C fires, startElection
		// enters and blocks on n.mu.Lock().
		node.mu.Lock()
		go func() {
			defer close(runDone)
			node.Run(ctx, voteRPC)
		}()

		// Wait past electionTimeoutMax so timer.C is guaranteed to have fired.
		time.Sleep(electionTimeoutMax + 60*time.Millisecond)

		// Now deliver a valid heartbeat while n.mu is still held.
		if err := node.ObserverLeader(3, "a"); err != nil {
			node.mu.Unlock()
			cancel()
			<-runDone
			t.Fatalf("ObserverLeader: %v", err)
		}
		if !node.heardRecently() {
			node.mu.Unlock()
			cancel()
			<-runDone
			t.Fatal("expected heardRecently() == true after ObserverLeader")
		}

		// Release n.mu so the blocked startElection resumes and checks heardRecently().
		node.mu.Unlock()

		// Allow startElection to finish and return to Run's select loop (well under electionTimeoutMin).
		time.Sleep(30 * time.Millisecond)
		cancel()
		<-runDone

		if got := voteCalls.Load(); got != 0 {
			t.Fatalf("RequestVote RPCs sent = %d, want 0", got)
		}
		snap := node.Snapshot()
		if snap.CurrentTerm != 3 || snap.Role != Follower || snap.VotedFor != "" {
			t.Fatalf("Snapshot() = %+v, want Follower term=3 votedFor=''", snap)
		}
	})

	t.Run("appendEntries_arrives_while_startElection_blocked_on_mu", func(t *testing.T) {
		node, err := OpenNode("b", filepath.Join(t.TempDir(), "b.state"))
		if err != nil {
			t.Fatal(err)
		}
		if err := node.SetPeers([]string{"a", "c"}); err != nil {
			t.Fatal(err)
		}
		if err := node.SetPersistentState(3, ""); err != nil {
			t.Fatal(err)
		}

		var voteCalls atomic.Int32
		voteRPC := func(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteResult, error) {
			voteCalls.Add(1)
			return RequestVoteResult{Term: args.Term, VoteGranted: false}, nil
		}

		// Hold n.mu.Lock(), then block startElection on it.
		node.mu.Lock()
		electErr := make(chan error, 1)
		go func() {
			electErr <- node.startElection(context.Background(), voteRPC)
		}()
		// Give startElection a moment to reach n.mu.Lock().
		time.Sleep(20 * time.Millisecond)

		// Deliver AppendEntries while n.mu is still held; it must record lastHeard
		// before blocking on n.mu.Lock().
		type appendOutcome struct {
			res AppendEntriesResult
			err error
		}
		appendDone := make(chan appendOutcome, 1)
		go func() {
			res, err := node.HandleAppendEntries(AppendEntriesArgs{
				Term:         3,
				LeaderID:     "a",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
			})
			appendDone <- appendOutcome{res: res, err: err}
		}()

		// Wait until HandleAppendEntries has recorded lastHeard before unlocking n.mu.
		deadline := time.Now().Add(500 * time.Millisecond)
		for node.lastHeard.Load() == 0 {
			if time.Now().After(deadline) {
				node.mu.Unlock()
				t.Fatal("HandleAppendEntries did not update lastHeard while n.mu was held")
			}
			time.Sleep(time.Millisecond)
		}

		node.mu.Unlock()

		if err := <-electErr; err != nil {
			t.Fatalf("startElection returned error: %v", err)
		}
		out := <-appendDone
		if out.err != nil {
			t.Fatalf("HandleAppendEntries error: %v", out.err)
		}
		if !out.res.Success || out.res.Term != 3 {
			t.Fatalf("HandleAppendEntries result = %+v, want Term=3 Success=true", out.res)
		}
		if got := voteCalls.Load(); got != 0 {
			t.Fatalf("RequestVote RPCs sent = %d, want 0", got)
		}
		snap := node.Snapshot()
		if snap.CurrentTerm != 3 || snap.Role != Follower || snap.VotedFor != "" {
			t.Fatalf("Snapshot() = %+v, want Follower term=3 votedFor=''", snap)
		}
	})
}

// HandleAppendEntries must poke electionReset and update lastHeard for valid
// terms BEFORE acquiring n.mu (or doing WAL/state disk I/O), while stale-term
// AppendEntries must return immediately without touching electionReset/lastHeard.
func TestHandleAppendEntriesPokesElectionTimerBeforeAcquiringMu(t *testing.T) {
	node, err := OpenNode("b", filepath.Join(t.TempDir(), "b.state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := node.SetPersistentState(3, ""); err != nil {
		t.Fatal(err)
	}
	if node.lastHeard.Load() != 0 || node.heardRecently() {
		t.Fatalf("initial lastHeard=%d heardRecently=%v, want 0/false", node.lastHeard.Load(), node.heardRecently())
	}

	node.mu.Lock()
	muHeld := true
	defer func() {
		if muHeld {
			node.mu.Unlock()
		}
	}()

	// 1. Stale-term AppendEntries returns immediately and does not touch lastHeard/electionReset.
	staleDone := make(chan AppendEntriesResult, 1)
	go func() {
		res, err := node.HandleAppendEntries(AppendEntriesArgs{
			Term:     2,
			LeaderID: "stale",
		})
		if err != nil {
			t.Errorf("stale HandleAppendEntries error: %v", err)
		}
		staleDone <- res
	}()
	select {
	case res := <-staleDone:
		if res.Success || res.Term != 3 {
			t.Fatalf("stale AppendEntries result = %+v, want Term=3 Success=false", res)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stale HandleAppendEntries blocked while n.mu.Lock() was held")
	}
	if node.lastHeard.Load() != 0 || node.heardRecently() {
		t.Fatalf("stale HandleAppendEntries updated lastHeard=%d", node.lastHeard.Load())
	}
	select {
	case <-node.electionReset:
		t.Fatal("stale HandleAppendEntries poked electionReset")
	default:
	}

	// 2. Valid-term AppendEntries must poke electionReset and set lastHeard BEFORE acquiring n.mu.
	type appendOutcome struct {
		res AppendEntriesResult
		err error
	}
	validDone := make(chan appendOutcome, 1)
	go func() {
		res, err := node.HandleAppendEntries(AppendEntriesArgs{
			Term:         3,
			LeaderID:     "a",
			PrevLogIndex: 0,
			PrevLogTerm:  0,
		})
		validDone <- appendOutcome{res: res, err: err}
	}()

	select {
	case <-node.electionReset:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("HandleAppendEntries did not poke electionReset while n.mu was held")
	}
	if node.lastHeard.Load() == 0 || !node.heardRecently() {
		t.Fatalf("HandleAppendEntries did not update lastHeard before acquiring n.mu (lastHeard=%d)", node.lastHeard.Load())
	}
	select {
	case out := <-validDone:
		t.Fatalf("HandleAppendEntries returned while n.mu was still held: %+v", out)
	default:
	}

	// Release n.mu and verify HandleAppendEntries finishes cleanly.
	node.mu.Unlock()
	muHeld = false

	select {
	case out := <-validDone:
		if out.err != nil {
			t.Fatalf("HandleAppendEntries error: %v", out.err)
		}
		if !out.res.Success || out.res.Term != 3 {
			t.Fatalf("HandleAppendEntries result = %+v, want Term=3 Success=true", out.res)
		}
	case <-time.After(time.Second):
		t.Fatal("HandleAppendEntries did not finish after releasing n.mu")
	}
}
