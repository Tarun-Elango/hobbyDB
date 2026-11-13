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

// Node holds one cluster member's Raft state (follower, candidate, or leader).
type Node struct {
	mu sync.RWMutex

	// ---- identity ----
	ID        string
	statePath string // file path for saving state to disk

	// ---- all roles ----
	Role        Role
	CurrentTerm uint64
	Log         []LogEntry

	// ---- follower + candidate (elections) ----
	VotedFor      string
	LeaderID      string
	peers         []string
	electionReset chan struct{}

	// ---- leader (log replication) ----
	nextIndex  map[string]uint64        // next log index to send to each peer
	matchIndex map[string]uint64        // highest index known copied on each peer
	peerWake   map[string]chan struct{} // wake per-peer replication workers

	// ---- commit + apply to KV store ----
	CommitIndex uint64 // highest log index known to be committed ( global)
	LastApplied uint64 // highest committed index applied to the store ( purely local)
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
		ID:            id,
		Role:          Follower,
		Log:           make([]LogEntry, 0),
		electionReset: make(chan struct{}, 1),
	}
}

func (n *Node) peerIds() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return append([]string(nil), n.peers...)
}
