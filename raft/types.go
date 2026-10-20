package raft

import "kvStore/proto"

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

type Node struct {
	ID string

	Role        Role
	CurrentTerm uint64
	VotedFor    string
	LeaderId    string

	CommitIndex uint64 // highest index raft considers safely commited
	LastApplied uint64 // highest this node commited

	Log []LogEntry
}

func NewNode(id string) *Node {
	return &Node{
		ID:   id,
		Role: Follower,
		Log:  make([]LogEntry, 0),
	}
}
