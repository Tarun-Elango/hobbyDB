package raft

import (
	"kvStore/proto"
	"kvStore/wal"
	"sync"
	"sync/atomic"
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
	// metaMu guards fast-path reads of Role, CurrentTerm, LeaderID, CommitIndex,
	// commitTerm, LastApplied, and peers, and is NEVER held across disk or network I/O.
	//
	// Strict lock order: always acquire mu before metaMu; never acquire mu while
	// holding metaMu.
	//   - Every writer to Role, CurrentTerm, CommitIndex, commitTerm, LastApplied,
	//     or peers holds both mu.Lock() and metaMu.Lock(), so readers holding either
	//     mu or metaMu observe them without races.
	//   - Writers to LeaderID hold at least metaMu.Lock() (ObserverLeader's steady-state
	//     fast path updates LeaderID under metaMu alone), so every reader of LeaderID
	//     must hold metaMu.
	metaMu sync.RWMutex

	// ---- identity ----
	ID        string
	statePath string   // file path for saving state to disk
	logWAL    *wal.Log // durable copy of Log; nil means the log is memory-only

	// ---- all roles ----
	Role        Role
	CurrentTerm uint64
	Log         []LogEntry

	// ---- follower + candidate (elections) ----
	VotedFor      string
	LeaderID      string
	peers         []string
	electionReset chan struct{}
	lastHeard     atomic.Int64  // UnixNano timestamp of last valid leader/vote message; 0 means never heard
	leaderWake    chan struct{} // poked by becomeLeader to start the heartbeat stint

	// ---- leader (log replication) ----
	nextIndex  map[string]uint64        // next log index to send to each peer
	matchIndex map[string]uint64        // highest index known copied on each peer
	peerWake   map[string]chan struct{} // wake per-peer replication workers

	// ---- commit + apply to KV store ----
	CommitIndex uint64     // highest log index known to be committed ( global)
	commitTerm  uint64     // log term at CommitIndex (mirrored under metaMu for lock-free-of-mu reads)
	LastApplied uint64     // highest committed index applied to the store ( purely local)
	applyCond   *sync.Cond // wakes the apply loop and anyone waiting for an entry to be applied

	// ---- leader read confirmation (ReadIndex) ----
	// Leaf lock: never acquire mu or metaMu while holding readMu, and never hold
	// readMu across network or disk I/O.
	readMu    sync.Mutex
	readStint *heartbeatStintState
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

// NodeStatus is a lightweight view of leadership and term metadata.
// Reading it acquires only metaMu (never mu) and never copies Log.
type NodeStatus struct {
	Role        Role
	CurrentTerm uint64
	LeaderID    string
}

// Status returns the node's role, term, and known leader without blocking on mu
// (which may be held across WAL fsync) or copying the log slice.
func (n *Node) Status() NodeStatus {
	n.metaMu.RLock()
	st := NodeStatus{
		Role:        n.Role,
		CurrentTerm: n.CurrentTerm,
		LeaderID:    n.LeaderID,
	}
	n.metaMu.RUnlock()
	return st
}

// setMetaLocked updates Role and LeaderID under metaMu.
// Caller must hold n.mu.Lock().
func (n *Node) setMetaLocked(role Role, leaderID string) {
	n.metaMu.Lock()
	n.Role = role
	n.LeaderID = leaderID
	n.metaMu.Unlock()
}

// setCommitLocked updates CommitIndex and its log term under metaMu.
// Caller must hold n.mu.Lock().
func (n *Node) setCommitLocked(index, term uint64) {
	n.metaMu.Lock()
	n.CommitIndex = index
	n.commitTerm = term
	n.metaMu.Unlock()
}

// setLastAppliedLocked updates LastApplied under metaMu.
// Caller must hold n.mu.Lock().
func (n *Node) setLastAppliedLocked(index uint64) {
	n.metaMu.Lock()
	n.LastApplied = index
	n.metaMu.Unlock()
}

// copy nodes current state and return as snapshot
func (n *Node) Snapshot() Snapshot {
	n.mu.RLock()
	defer n.mu.RUnlock()

	n.metaMu.RLock()
	role := n.Role
	term := n.CurrentTerm
	leaderID := n.LeaderID
	n.metaMu.RUnlock()

	logCopy := append([]LogEntry(nil), n.Log...) // copies log entires to a new slice, change to orignal keep this unaffected

	return Snapshot{
		ID:          n.ID,
		Role:        role,
		CurrentTerm: term,
		VotedFor:    n.VotedFor,
		LeaderID:    leaderID,
		CommitIndex: n.CommitIndex,
		LastApplied: n.LastApplied,
		Log:         logCopy,
	}

}

func NewNode(id string) *Node {
	n := &Node{
		ID:            id,
		Role:          Follower,
		Log:           make([]LogEntry, 0),
		electionReset: make(chan struct{}, 1),
		leaderWake:    make(chan struct{}, 1),
	}
	n.applyCond = sync.NewCond(&n.mu)
	return n
}

// cachedPeers returns the immutable peer slice without acquiring n.mu or allocating.
// Callers must not mutate the returned slice.
func (n *Node) cachedPeers() []string {
	n.metaMu.RLock()
	peers := n.peers
	n.metaMu.RUnlock()
	return peers
}

func (n *Node) peerIds() []string {
	return append([]string(nil), n.cachedPeers()...)
}
