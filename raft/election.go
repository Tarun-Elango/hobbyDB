package raft

import (
	"context"
	"math/rand/v2"
	"sync"
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

// run the election timer in its own goroutine
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

	var (
		mu         sync.Mutex
		votes      = 1
		higherTerm uint64
	)
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
	electCtx, electCancel := context.WithCancel(ctx)
	defer electCancel()

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			res, err := rpc(electCtx, peer, args)
			if err != nil || !n.isCandidate(term) {
				return
			}

			mu.Lock()
			defer mu.Unlock()

			if res.Term > term {
				if res.Term > higherTerm {
					higherTerm = res.Term

				}
				electCancel()
				return
			}
			if res.VoteGranted {
				votes++
				if votes >= need {
					electCancel()
				}
			}
		}(peer)
	}
	wg.Wait()

	if higherTerm > term {
		return n.stepDown(higherTerm)
	}

	if votes >= need {
		n.becomeLeader(term)
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

	lastIndex, _ := n.lastLogLocked()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))

	// for each peer, set the next index to the last index + 1 and the match index to 0
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0 // 0 cause we haven't replicated anything to this peer yet
	}

	for _, ch := range n.peerWake {
		select {
		case ch <- struct{}{}: // wake the peer's sender, by sending a empty struct
		default:
		}
	}

	// assume every peer if caught up, fail append will walk back until logs meet
}

func (n *Node) stepDown(term uint64) error {
	if err := n.SetPersistentState(term, ""); err != nil {
		return err
	}

	n.mu.Lock()
	if n.CurrentTerm == term {
		n.Role = Follower
		n.LeaderID = ""
		n.applyCond.Broadcast() // unblock Propose waiting to apply
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

// send heartbeats to peers in a goroutine
func (n *Node) RunHeartbeats(ctx context.Context, rpc HeartbeatRPC) {
	peers := n.peerIds()
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			ticker := time.NewTicker(electionTimeoutMin / 3)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					snap := n.Snapshot()
					if snap.Role != Leader || rpc == nil {
						continue
					}
					res, err := rpc(ctx, peer, HeartbeatArgs{
						Term:     snap.CurrentTerm,
						LeaderID: snap.LeaderID,
					})
					if err != nil {
						continue
					}
					if res.Term > snap.CurrentTerm {
						_ = n.stepDown(res.Term)
					}
				}
			}
		}(peer)
	}
	wg.Wait()
}
