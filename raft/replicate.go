package raft

import (
	"context"
	"fmt"
	"kvStore/proto"
	"sync"
	"time"
)

const (
	appendEntriesBatchLimit = 64
	maxAppendEntriesPayload = 64<<20 - 2
	appendRetryBackoff      = 50 * time.Millisecond
)

// AppendEntries RPC: leader copies log entries to peers and reports its commit index.

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64 // leader's commitIndex; follower may advance up to the matched prefix
}

type AppendEntriesResult struct {
	Term    uint64
	Success bool
}

type AppendEntriesRPC func(ctx context.Context, peerId string, args AppendEntriesArgs) (AppendEntriesResult, error)

// ---- shared (leader + follower log helpers)

func (n *Node) logTermAtLocked(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index > uint64(len(n.Log)) {
		return 0, false
	}
	entry := n.Log[index-1]
	if entry.Index != index {
		return 0, false
	}
	return entry.Term, true
}

func logEntryWireSize(entry LogEntry) int {
	return 8 + 8 + 1 + 4 + len(entry.Command.Key) + 4 + len(entry.Command.Value)
}

func cloneLogEntry(entry LogEntry) LogEntry {
	return LogEntry{
		Index: entry.Index,
		Term:  entry.Term,
		Command: proto.Command{
			Op:    entry.Command.Op,
			Key:   append([]byte(nil), entry.Command.Key...),
			Value: append([]byte(nil), entry.Command.Value...),
		},
	}
}

// ---- leader funcs

// records the command in the leader's log.
// commitIndex moves here only when this node is already a majority (a single-node cluster).
func (n *Node) Propose(cmd proto.Command) (LogEntry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Role != Leader {
		return LogEntry{}, fmt.Errorf("not leader")
	}

	lastIndex, _ := n.lastLogLocked()
	entry := LogEntry{
		Index: lastIndex + 1,
		Term:  n.CurrentTerm,
		Command: proto.Command{
			Op:    cmd.Op,
			Key:   append([]byte(nil), cmd.Key...),
			Value: append([]byte(nil), cmd.Value...),
		},
	}
	n.Log = append(n.Log, entry) // add new entry to log
	n.advanceCommitLocked()

	for _, ch := range n.peerWake {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return entry, nil
}

// start one goroutine per peer to send append entries requests
func (n *Node) RunReplication(ctx context.Context, rpc AppendEntriesRPC) {
	peers := n.peerIds()
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			n.replicatePeer(ctx, peer, rpc)
		}(peer)
	}
	wg.Wait()
}

// per peer, ask what to send, send it, and wait for response
func (n *Node) replicatePeer(ctx context.Context, peer string, rpc AppendEntriesRPC) {
	n.mu.RLock()
	wake := n.peerWake[peer]
	n.mu.RUnlock()
	if wake == nil || rpc == nil {
		return
	}

	for {
		args, term, lastSent, ok := n.nextAppend(peer)
		if ok {
			res, err := rpc(ctx, peer, args)
			if err == nil {
				n.handleAppendResult(peer, term, lastSent, len(args.Entries), res)
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if !n.waitPeer(ctx, wake, appendRetryBackoff) {
				return
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-wake:
		}
	}
}

func (n *Node) waitPeer(ctx context.Context, wake <-chan struct{}, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return true
	case <-timer.C:
		return true
	}
}

// build one AppendEntriesArgs for this peer; if caught up, return false
func (n *Node) nextAppend(peer string) (AppendEntriesArgs, uint64, uint64, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.Role != Leader {
		return AppendEntriesArgs{}, 0, 0, false
	}
	next, exists := n.nextIndex[peer]
	if !exists || next == 0 || next > uint64(len(n.Log)) {
		return AppendEntriesArgs{}, 0, 0, false
	}

	prevIndex := next - 1
	prevTerm, ok := n.logTermAtLocked(prevIndex)
	if !ok {
		return AppendEntriesArgs{}, 0, 0, false
	}

	header := 8 + 2 + len(n.ID) + 8 + 8 + 8 + 4 // term, id len, id, prev index, prev term, leader commit, count
	entries := make([]LogEntry, 0, appendEntriesBatchLimit)
	size := header
	for _, entry := range n.Log[next-1:] {
		entrySize := logEntryWireSize(entry)
		if len(entries) > 0 && (len(entries) >= appendEntriesBatchLimit || size+entrySize > maxAppendEntriesPayload) {
			break
		}
		entries = append(entries, cloneLogEntry(entry))
		size += entrySize
	}
	if len(entries) == 0 {
		return AppendEntriesArgs{}, 0, 0, false
	}

	lastSent := entries[len(entries)-1].Index
	return AppendEntriesArgs{
		Term:         n.CurrentTerm,
		LeaderID:     n.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.CommitIndex,
	}, n.CurrentTerm, lastSent, true
}

// read follower response: step down, decrement next index, or update match index and maybe commit
func (n *Node) handleAppendResult(peer string, term uint64, lastSent uint64, nEntries int, res AppendEntriesResult) {
	if res.Term > term {
		_ = n.stepDown(res.Term)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Role != Leader || n.CurrentTerm != term {
		return
	}
	if !res.Success {
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		return
	}
	if nEntries > 0 && lastSent > n.matchIndex[peer] {
		n.matchIndex[peer] = lastSent
		n.nextIndex[peer] = lastSent + 1
		n.advanceCommitLocked()
	}
}

// A write is committed once a majority of the cluster has it in the log.
// Count this leader as one copy. Only an entry from the current term may
// move commitIndex; every earlier entry commits with it.
func (n *Node) advanceCommitLocked() {
	if n.Role != Leader {
		return
	}
	for i := len(n.Log); i > int(n.CommitIndex); i-- {
		entry := n.Log[i-1]
		if entry.Index != uint64(i) || entry.Term != n.CurrentTerm {
			continue
		}
		if n.copiesAtLocked(entry.Index) >= n.quorumLocked() {
			n.CommitIndex = entry.Index
			return
		}
	}
}

// copies of this index: the leader, plus each peer whose matchIndex has reached it.
func (n *Node) copiesAtLocked(index uint64) int {
	count := 1
	for _, peer := range n.peers {
		if n.matchIndex[peer] >= index {
			count++
		}
	}
	return count
}

// ---- follower funcs -----

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) (AppendEntriesResult, error) {
	snap := n.Snapshot()
	if args.Term < snap.CurrentTerm {
		return AppendEntriesResult{Term: snap.CurrentTerm, Success: false}, nil
	}
	if args.Term > snap.CurrentTerm {
		if err := n.SetPersistentState(args.Term, ""); err != nil {
			return AppendEntriesResult{}, err
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.CurrentTerm != args.Term {
		return AppendEntriesResult{Term: n.CurrentTerm, Success: false}, nil
	}
	n.Role = Follower
	n.LeaderID = args.LeaderID

	prevTerm, ok := n.logTermAtLocked(args.PrevLogIndex)
	if !ok || prevTerm != args.PrevLogTerm {
		n.pokeElectionTimer()
		return AppendEntriesResult{Term: n.CurrentTerm, Success: false}, nil
	}

	for _, entry := range args.Entries {
		haveTerm, exists := n.logTermAtLocked(entry.Index)
		if exists && haveTerm != entry.Term {
			n.Log = n.Log[:entry.Index-1]
			exists = false
		}
		if exists {
			continue
		}
		if entry.Index != uint64(len(n.Log))+1 {
			return AppendEntriesResult{Term: n.CurrentTerm, Success: false}, nil
		}
		n.Log = append(n.Log, cloneLogEntry(entry))
	}

	matchedThrough := args.PrevLogIndex
	if len(args.Entries) > 0 {
		matchedThrough = args.Entries[len(args.Entries)-1].Index
	}
	n.advanceFollowerCommitLocked(args.LeaderCommit, matchedThrough)

	n.pokeElectionTimer()
	return AppendEntriesResult{Term: n.CurrentTerm, Success: true}, nil
}

// follower learns the leader's commit index, capped at the prefix this RPC matched.
func (n *Node) advanceFollowerCommitLocked(leaderCommit, matchedThrough uint64) {
	if leaderCommit <= n.CommitIndex {
		return
	}
	commit := leaderCommit
	if matchedThrough < commit {
		commit = matchedThrough
	}
	if commit > n.CommitIndex {
		n.CommitIndex = commit
	}
}
