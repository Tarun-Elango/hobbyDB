package raft

import (
	"context"

	"kvStore/proto"
)

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
		for n.LastApplied >= n.CommitIndex && ctx.Err() == nil {
			n.applyCond.Wait()
		}
		if ctx.Err() != nil {
			n.mu.Unlock()
			return
		}

		next := n.LastApplied + 1
		if next == 0 || next > uint64(len(n.Log)) || n.Log[next-1].Index != next {
			// committed prefix is missing from this log; wait for a later update
			if ctx.Err() != nil {
				n.mu.Unlock()
				return
			}
			n.applyCond.Wait()
			n.mu.Unlock()
			continue
		}
		entry := cloneLogEntry(n.Log[next-1])
		n.mu.Unlock()

		apply(entry.Command)

		n.mu.Lock()
		if n.LastApplied < entry.Index {
			n.LastApplied = entry.Index
		}
		n.applyCond.Broadcast()
		n.mu.Unlock()
	}
}

// WaitLeaderRead blocks until this leader's store is safe to read: an entry
// from the current term is committed (so CommitIndex covers every write any
// earlier leader acknowledged), and the store has applied up to that point.
// A freshly elected or restarted leader starts with an empty or partial store
// until its no-op commits; reading before then can miss committed writes.
// Returns ErrNotLeader if this node is not (or stops being) the leader.
func (n *Node) WaitLeaderRead(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

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

	var (
		readIndex uint64
		ready     bool // readIndex is fixed once a current-term entry has committed
	)
	for {
		if n.Role != Leader {
			return ErrNotLeader
		}
		if !ready {
			if term, ok := n.logTermAtLocked(n.CommitIndex); ok && term == n.CurrentTerm {
				readIndex, ready = n.CommitIndex, true
			}
		}
		if ready && n.LastApplied >= readIndex {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n.applyCond.Wait()
	}
}
