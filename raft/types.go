package raft

import (
	"kvStore/proto"
	"sync"
)

type Role uint8

const (
	Follower Role = iota
	Leader
	Candidate
)

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command proto.Command
}

// actual state
type Node struct {
	mu sync.RWMutex

	ID        string
	statePath string

	Role        Role
	CurrentTerm uint64
	VotedFor    string
	LeaderID    string

	CommitIndex uint64 // highest log index known to be committed
	LastApplied uint64 // highest committed index applied to the key-value store

	Log []LogEntry
}

// read only copy, thread safe
type Snapshot struct {
	ID          string
	Role        Role
	CurrentTerm uint64
	VotedFor    string
	LeaderID    string
	CommitIndex uint64
	LastApplied uint64
	Log         []LogEntry
}

// copy nodes current state and return as snapshot
func (n *Node) Snapshot() Snapshot {
	n.mu.RLock()
	defer n.mu.RUnlock()
	logCopy := append([]LogEntry(nil), n.Log...) // copies log entires to a new slice, change to orignal keep this unaffected

	return Snapshot{
		ID:          n.ID,
		Role:        n.Role,
		CurrentTerm: n.CurrentTerm,
		VotedFor:    n.VotedFor,
		LeaderID:    n.LeaderID,
		CommitIndex: n.CommitIndex,
		LastApplied: n.LastApplied,
		Log:         logCopy,
	}

}

func NewNode(id string) *Node {
	return &Node{
		ID:   id,
		Role: Follower,
		Log:  make([]LogEntry, 0),
	}
}
