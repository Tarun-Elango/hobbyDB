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

// leader to peer log copy
// leader sends etnry to each peer ( with index and term of entry before it)
// if peers logs don't match, leader walks back until logs meet ( just for that peer)

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
}

type AppendEntriesResult struct {
	Term    uint64
	Success bool
}

type AppendEntriesRPC func(ctx context.Context, peerId string, args AppendEntriesArgs) (AppendEntriesResult, error)

// records the command in leader's, and returns before any peer has the entry
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

	for _, ch := range n.peerWake {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return entry, nil
}

// senders
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

// sends previous index and term, then every entry from next index to last sent
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

	header := 8 + 2 + len(n.ID) + 8 + 8 + 4
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
	}, n.CurrentTerm, lastSent, true
}

func logEntryWireSize(entry LogEntry) int {
	return 8 + 8 + 1 + 4 + len(entry.Command.Key) + 4 + len(entry.Command.Value)
}
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

// steps down when the peer has a higher term
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
	}
}

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

	n.pokeElectionTimer()
	return AppendEntriesResult{Term: n.CurrentTerm, Success: true}, nil
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
