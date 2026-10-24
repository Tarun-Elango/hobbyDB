package raft

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

type HeartbeatArgs struct {
	Term     uint64
	LeaderID string
}

type HeartbeatResult struct {
	Term uint64
}

type HeartbeatRPC func(ctx context.Context, peerID string, args HeartbeatArgs) (HeartbeatResult, error)

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResult struct {
	Term        uint64
	VoteGranted bool
}

// one ask to one peer,
type VoteRPC func(ctx context.Context, peerId string, args RequestVoteArgs) (RequestVoteResult, error)

// generate a random election timeout between electionTimeoutMin and electionTimeoutMax
func randomElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int64N(int64(span)))
}

// get the last log index and term from the log
func (n *Node) lastLogLocked() (index, term uint64) {
	if len(n.Log) == 0 {
		return 0, 0
	}
	last := n.Log[len(n.Log)-1]
	return last.Index, last.Term
}

// true when candidate's log is at least as up-to-date as receiver's log
func LogIsUptoDate(candIndex, candTerm, localIndex, localTerm uint64) bool {
	if candTerm != localTerm {
		return candTerm > localTerm
	}
	return candIndex >= localIndex
}

func (n *Node) Run(ctx context.Context, rpc VoteRPC) {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.electionReset: // when the election channel is triggered, usually a valid heartbeat
			if !timer.Stop() { // if timer is not stopped
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(randomElectionTimeout())
		case <-timer.C:
			if n.Snapshot().Role != Leader {
				_ = n.startElection(ctx, rpc) // start a new election
			}
			timer.Reset(randomElectionTimeout())
		}
	}
}

// to tell the election timer to reset
func (n *Node) pokeElectionTimer() {
	select {
	case n.electionReset <- struct{}{}:
	default: // if the channel is full, do nothing and return
	}
}

// when this node learns about a new leader - usally during the append entries
// recored the new leader and reset the election timer
func (n *Node) ObserverLeader(term uint64, leaderId string) error {
	n.mu.Lock()
	current := n.CurrentTerm
	n.mu.Unlock()

	if term < current {
		return nil
	}

	if term > current {
		if err := n.SetPersistentState(term, ""); err != nil {
			return err
		}
	}

	n.mu.Lock()
	if n.CurrentTerm == term {
		n.Role = Follower

		n.LeaderID = leaderId

	}
	n.mu.Unlock()
	n.pokeElectionTimer()
	return nil
}

// become a candidate and count votes

func (n *Node) startElection(ctx context.Context, rpc VoteRPC) error {
	n.mu.Lock()
	if n.Role == Leader { // lol already a leader
		n.mu.Unlock()
		return nil
	}

	term := n.CurrentTerm + 1
	lastIndex, lastTerm := n.lastLogLocked()
	peers := append([]string(nil), n.peers...)
	need := n.quorumLocked()
	n.mu.Unlock()

	// disk first, new term, and our vote is for ourselves
	if err := n.SetPersistentState(term, n.ID); err != nil {
		return err
	}

	n.mu.Lock()
	if n.CurrentTerm != term || n.VotedFor != n.ID {
		n.mu.Unlock()
		return nil
	}
	n.Role = Candidate
	n.LeaderID = ""
	n.mu.Unlock()

	votes := 1
	if votes >= need {
		n.becomeLeader(term)
		return nil
	}

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.ID,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	for _, peer := range peers {
		if ctx.Err() != nil || !n.isCandidate(term) { // node is not a candidate
			return ctx.Err()
		}
		if rpc == nil {
			continue
		}

		res, err := rpc(ctx, peer, args)
		if err != nil {
			continue
		}

		if res.Term > term { // new leader detected, step down
			return n.stepDown(res.Term)
		}
		if res.VoteGranted {
			votes++
			if votes >= need {
				n.becomeLeader(term)
				return nil
			}
		}
	}

	return nil
}

// node asks itself, is it a candidate? double check
func (n *Node) isCandidate(term uint64) bool {
	snap := n.Snapshot()
	return snap.Role == Candidate && snap.CurrentTerm == term
}

// become a leader by updating the in memory state
func (n *Node) becomeLeader(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Role != Candidate || n.CurrentTerm != term {
		return
	}

	n.Role = Leader
	n.LeaderID = n.ID
}

func (n *Node) stepDown(term uint64) error {
	if err := n.SetPersistentState(term, ""); err != nil {
		return err
	}

	n.mu.Lock()
	if n.CurrentTerm == term {
		n.Role = Follower
		n.LeaderID = ""

	}
	n.mu.Unlock()
	n.pokeElectionTimer()
	return nil
}

// reciever side,
func (n *Node) HandleRequestVote(args RequestVoteArgs) (RequestVoteResult, error) {
	n.mu.Lock()
	current := n.CurrentTerm
	votedFor := n.VotedFor
	lastIndex, lastTerm := n.lastLogLocked()
	n.mu.Unlock()

	if args.Term < current { //stale
		return RequestVoteResult{Term: current, VoteGranted: false}, nil
	}

	upToDate := LogIsUptoDate(args.LastLogIndex, args.LastLogTerm, lastIndex, lastTerm)

	if args.Term > current { // we are behind, so accept their term
		vote := "" // incase their log is not up to date, we will vote ""
		if upToDate {
			vote = args.CandidateID
		}
		res, err := n.recordVote(args.Term, vote, args.CandidateID)
		if res.VoteGranted {
			n.pokeElectionTimer()
		}
		return res, err
	}

	if !upToDate || (votedFor != "" && votedFor != args.CandidateID) {
		return RequestVoteResult{Term: current, VoteGranted: false}, nil
	}
	res, err := n.recordVote(args.Term, args.CandidateID, args.CandidateID)
	if res.VoteGranted {
		n.pokeElectionTimer()
	}
	return res, err
}

// save my vote and tell candidate the result
func (n *Node) recordVote(term uint64, votedFor string, candidateId string) (RequestVoteResult, error) {

	if err := n.SetPersistentState(term, votedFor); err != nil {
		snap := n.Snapshot()
		return RequestVoteResult{
			Term:        snap.CurrentTerm,
			VoteGranted: snap.VotedFor == candidateId && snap.CurrentTerm == term, // if the vote is for the candidate and the term is the same
		}, nil
	}

	n.mu.Lock()
	if n.CurrentTerm == term {
		n.Role = Follower
		n.LeaderID = "" // dont knwo for sure, hence will update later
	}
	granted := n.CurrentTerm == term && n.VotedFor == candidateId // if the vote is for the candidate and the term is the same
	termNow := n.CurrentTerm
	n.mu.Unlock()
	return RequestVoteResult{
		Term:        termNow,
		VoteGranted: granted,
	}, nil
}
