package raft

import (
	"context"
	"errors"
	"time"

	"kvStore/proto"
)

var errReadNoQuorum = errors.New("quorum did not confirm leadership")

const readConfirmRetryBackoff = 20 * time.Millisecond

type readBatch struct {
	id      uint64
	term    uint64
	waiters int
	done    chan struct{}
	err     error
}

// heartbeatStintState coordinates ReadIndex confirmation rounds across one
// leader term's persistent per-peer heartbeat workers.
// Mutable fields are guarded by Node.readMu.
type heartbeatStintState struct {
	term   uint64
	quorum int
	peers  []string
	cancel context.CancelFunc
	wake   []chan struct{}

	closed      bool
	nextRoundID uint64
	active      *readBatch
	pending     *readBatch
	acks        int
	settled     int
	peerDone    []uint64
}

func newHeartbeatStintState(term uint64, peers []string, cancel context.CancelFunc) *heartbeatStintState {
	wakes := make([]chan struct{}, len(peers))
	for i := range wakes {
		wakes[i] = make(chan struct{}, 1)
	}
	return &heartbeatStintState{
		term:     term,
		quorum:   (len(peers)+1)/2 + 1,
		peers:    peers,
		cancel:   cancel,
		wake:     wakes,
		peerDone: make([]uint64, len(peers)),
	}
}

func (s *heartbeatStintState) wakeAllLocked() {
	for _, ch := range s.wake {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// RunApply copies committed log entries into the key-value store.
// It applies one entry at a time, in index order, and only entries at or below commitIndex.
// lastApplied moves forward only after that entry's command has been applied.
func (n *Node) RunApply(ctx context.Context, apply func(proto.Command)) {
	if apply == nil {
		return
	}

	// closing the caller context has to wake a Wait, same as a new commit.
	go func() {
		<-ctx.Done()
		n.mu.Lock()
		n.applyCond.Broadcast()
		n.mu.Unlock()
	}()

	for {
		n.mu.Lock()
		for n.pendingSnap == nil && n.LastApplied >= n.CommitIndex && ctx.Err() == nil {
			n.applyCond.Wait()
		}
		if ctx.Err() != nil {
			n.mu.Unlock()
			return
		}

		if n.pendingSnap != nil {
			ps := n.pendingSnap
			if ps.index <= n.LastApplied {
				n.pendingSnap = nil
				n.mu.Unlock()
				continue
			}
			n.mu.Unlock()
			n.applyPendingSnapshot(ps)
			continue
		}

		next := n.LastApplied + 1
		slice, ok := n.logSliceFromLocked(next)
		if !ok || len(slice) == 0 || slice[0].Index != next {
			// committed prefix is missing from this log; wait for a later update
			if ctx.Err() != nil {
				n.mu.Unlock()
				return
			}
			n.applyCond.Wait()
			n.mu.Unlock()
			continue
		}
		lastIdx, _ := n.lastLogLocked()
		end := n.CommitIndex
		if end > lastIdx {
			end = lastIdx
		}
		applyLimit := uint64(proposeMaxBatchSize)
		if applyLimit < 256 {
			applyLimit = 256
		}
		if end-next+1 > applyLimit {
			end = next + applyLimit - 1
		}
		count := int(end - next + 1)
		entries := make([]LogEntry, 0, count)
		for i := 0; i < count && i < len(slice); i++ {
			e := slice[i]
			if e.Index != next+uint64(i) {
				break
			}
			entries = append(entries, cloneLogEntry(e))
		}
		n.mu.Unlock()

		for _, entry := range entries {
			apply(entry.Command)
		}

		lastApplied := entries[len(entries)-1].Index
		n.mu.Lock()
		if n.LastApplied < lastApplied {
			n.setLastAppliedLocked(lastApplied)
		}
		n.applyCond.Broadcast()

		var (
			startSnap bool
			snapIdx   uint64
			snapTerm  uint64
			snapPath  string
			sm        Snapshotter
		)
		if n.shouldSnapshotLocked() {
			if term, ok := n.logTermAtLocked(n.LastApplied); ok {
				n.snapInFlight = true
				startSnap = true
				snapIdx = n.LastApplied
				snapTerm = term
				snapPath = n.snapPath
				sm = n.snapshotter
			}
		}
		n.mu.Unlock()

		if startSnap {
			writeFn, closeFn := sm.BeginSnapshot()
			go n.finishLocalSnapshot(snapIdx, snapTerm, snapPath, writeFn, closeFn)
		}
	}
}

// WaitLeaderRead implements Raft §8 ReadIndex so leader reads are linearizable
// even if this leader was stalled or partitioned while a new leader took over:
//  1. Wait until an entry from the current term is committed (e.g. the leader's
//     no-op) and record readIndex = CommitIndex.
//  2. Confirm with a majority via a fresh heartbeat round (started after
//     recording readIndex) that this node is still the leader in that term.
//  3. Wait until the local state machine has applied at least readIndex.
//
// Returns ErrNotLeader if this node is not (or stops being) the leader.
func (n *Node) WaitLeaderRead(ctx context.Context) error {
	readIndex, term, err := n.waitCurrentTermCommit(ctx)
	if err != nil {
		return err
	}
	if err := n.confirmLeadership(ctx, term); err != nil {
		return err
	}
	return n.waitApplied(ctx, term, readIndex)
}

func (n *Node) waitCurrentTermCommit(ctx context.Context) (uint64, uint64, error) {
	// Fast path: in steady state a current-term entry is already committed,
	// so reading under metaMu avoids n.mu (which may be held across WAL fsync).
	n.metaMu.RLock()
	if n.Role != Leader {
		n.metaMu.RUnlock()
		return 0, 0, ErrNotLeader
	}
	if n.CommitIndex > 0 && n.commitTerm == n.CurrentTerm {
		readIndex, term := n.CommitIndex, n.CurrentTerm
		n.metaMu.RUnlock()
		return readIndex, term, nil
	}
	n.metaMu.RUnlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	// Re-check under the exclusive lock before spawning the cancel watcher.
	if n.Role != Leader {
		return 0, 0, ErrNotLeader
	}
	if term, ok := n.logTermAtLocked(n.CommitIndex); ok && term == n.CurrentTerm {
		return n.CommitIndex, n.CurrentTerm, nil
	}
	if ctx.Err() != nil {
		return 0, 0, ctx.Err()
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() { // ctx cancel has to wake the wait below
		select {
		case <-ctx.Done():
			n.mu.Lock()
			n.applyCond.Broadcast()
			n.mu.Unlock()
		case <-stop:
		}
	}()

	for {
		if n.Role != Leader {
			return 0, 0, ErrNotLeader
		}
		if term, ok := n.logTermAtLocked(n.CommitIndex); ok && term == n.CurrentTerm {
			return n.CommitIndex, n.CurrentTerm, nil
		}
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		n.applyCond.Wait()
	}
}

func (n *Node) waitApplied(ctx context.Context, term, readIndex uint64) error {
	// Fast path: usually LastApplied has already reached readIndex; reading
	// under metaMu avoids n.mu (which may be held across WAL fsync).
	n.metaMu.RLock()
	if n.Role != Leader || n.CurrentTerm != term {
		n.metaMu.RUnlock()
		return ErrNotLeader
	}
	if n.LastApplied >= readIndex {
		n.metaMu.RUnlock()
		return nil
	}
	n.metaMu.RUnlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Role != Leader || n.CurrentTerm != term {
		return ErrNotLeader
	}
	if n.LastApplied >= readIndex {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() { // ctx cancel has to wake the wait below
		select {
		case <-ctx.Done():
			n.mu.Lock()
			n.applyCond.Broadcast()
			n.mu.Unlock()
		case <-stop:
		}
	}()

	for {
		if n.Role != Leader || n.CurrentTerm != term {
			return ErrNotLeader
		}
		if n.LastApplied >= readIndex {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n.applyCond.Wait()
	}
}

// confirmLeadership verifies that a majority of the cluster still recognizes
// this node as leader in `term` by completing a fresh heartbeat round started
// after the caller recorded readIndex. Concurrent readers share the next round.
func (n *Node) confirmLeadership(ctx context.Context, term uint64) error {
	if len(n.cachedPeers()) == 0 {
		st := n.Status()
		if st.Role != Leader || st.CurrentTerm != term {
			return ErrNotLeader
		}
		return nil
	}

	for {
		st := n.Status()
		if st.Role != Leader || st.CurrentTerm != term {
			return ErrNotLeader
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		batch, stale, ok := n.enqueueReadRound(term)
		if stale {
			return ErrNotLeader
		}
		if !ok {
			if !waitBackoff(ctx, readConfirmRetryBackoff) {
				return ctx.Err()
			}
			continue
		}

		select {
		case <-batch.done:
			if batch.err == nil {
				return nil
			}
			if errors.Is(batch.err, ErrNotLeader) {
				return ErrNotLeader
			}
			if !waitBackoff(ctx, readConfirmRetryBackoff) {
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func waitBackoff(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (n *Node) registerReadStint(stint *heartbeatStintState) {
	n.readMu.Lock()
	n.readStint = stint
	n.readMu.Unlock()
}

// enqueueReadRound attaches the caller to a heartbeat round that has NOT yet
// started (starting one immediately if idle, or joining pending to run right
// after the active round finishes).
func (n *Node) enqueueReadRound(term uint64) (batch *readBatch, stale bool, ok bool) {
	n.readMu.Lock()
	defer n.readMu.Unlock()

	stint := n.readStint
	if stint == nil || stint.closed {
		return nil, false, false
	}
	if stint.term > term {
		return nil, true, false
	}
	if stint.term < term {
		return nil, false, false
	}
	if stint.active == nil {
		stint.nextRoundID++
		b := &readBatch{id: stint.nextRoundID, term: term, waiters: 1, done: make(chan struct{})}
		stint.active = b
		stint.acks = 1 // count self
		stint.settled = 0
		stint.wakeAllLocked()
		return b, false, true
	}
	if stint.pending == nil {
		stint.pending = &readBatch{term: term, done: make(chan struct{})}
	}
	stint.pending.waiters++
	return stint.pending, false, true
}

// nextPeerReadRound returns the active round ID if this peer has not yet
// attempted it, or 0 if there is no new read round to send.
func (n *Node) nextPeerReadRound(stint *heartbeatStintState, lastAttempted uint64) uint64 {
	n.readMu.Lock()
	defer n.readMu.Unlock()

	if stint.closed || stint.active == nil || stint.active.id <= lastAttempted {
		return 0
	}
	return stint.active.id
}

// reportHeartbeatOutcome records one peer's result for roundID without
// canceling other in-flight peer RPCs when quorum is reached.
func (n *Node) reportHeartbeatOutcome(
	stint *heartbeatStintState,
	peerIdx int,
	roundID uint64,
	res HeartbeatResult,
	err error,
) {
	n.readMu.Lock()
	defer n.readMu.Unlock()

	if stint.closed || stint.active == nil || stint.active.id != roundID {
		return
	}
	if stint.peerDone[peerIdx] >= roundID {
		return
	}
	stint.peerDone[peerIdx] = roundID
	stint.settled++

	if err == nil && res.Term == stint.term {
		stint.acks++
		if stint.acks >= stint.quorum {
			n.finishActiveAndPromoteLocked(stint, nil)
			return
		}
	}

	remaining := len(stint.peers) - stint.settled
	if stint.acks+remaining < stint.quorum {
		n.finishActiveAndPromoteLocked(stint, errReadNoQuorum)
	}
}

func (n *Node) finishActiveAndPromoteLocked(stint *heartbeatStintState, err error) {
	if stint.active != nil {
		stint.active.err = err
		close(stint.active.done)
		stint.active = nil
	}
	if stint.closed || stint.pending == nil {
		return
	}
	stint.nextRoundID++
	b := stint.pending
	stint.pending = nil
	b.id = stint.nextRoundID
	stint.active = b
	stint.acks = 1 // count self
	stint.settled = 0
	stint.wakeAllLocked()
}

func (n *Node) abortReadStint(stint *heartbeatStintState, err error) {
	n.readMu.Lock()
	n.abortReadStintLocked(stint, err)
	n.readMu.Unlock()
}

func (n *Node) abortReadStintLocked(stint *heartbeatStintState, err error) {
	if stint == nil {
		return
	}
	stint.closed = true
	if stint.cancel != nil {
		stint.cancel()
	}
	if stint.active != nil {
		stint.active.err = err
		close(stint.active.done)
		stint.active = nil
	}
	if stint.pending != nil {
		stint.pending.err = err
		close(stint.pending.done)
		stint.pending = nil
	}
	if n.readStint == stint {
		n.readStint = nil
	}
}

// cancelReadRound aborts any in-flight or queued leadership confirmation when
// this node stops being leader.
func (n *Node) cancelReadRound() {
	n.readMu.Lock()
	n.abortReadStintLocked(n.readStint, ErrNotLeader)
	n.readMu.Unlock()
}
