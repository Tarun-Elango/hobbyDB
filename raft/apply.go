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
