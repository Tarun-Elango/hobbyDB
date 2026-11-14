package raft

import (
	"context"
	"kvStore/logx"
	"kvStore/proto"
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
			if n.Snapshot().Role == Leader {
				// leaders don't run elections: leave the timer stopped.
				// every leader -> follower transition pokes electionReset to re-arm it.
				continue
			}
			_ = n.startElection(ctx, rpc) // start a new election
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

	// A leader only commits entries from its own term. Append a no-op so that
	// entries from earlier terms (for example after a restart) commit with it.
	noop := LogEntry{Index: lastIndex + 1, Term: term, Command: proto.Command{Op: proto.OpNoop}}
	if err := n.persistAppendLocked(noop); err != nil {
		logx.Errorf("raft: persist leader no-op: %v\n", err)
	} else {
		n.Log = append(n.Log, noop)
		n.advanceCommitLocked() // single-node cluster commits immediately
	}

	for _, ch := range n.peerWake {
		select {
		case ch <- struct{}{}: // wake the peer's sender, by sending a empty struct
		default:
		}
	}

	select { // start the heartbeat stint for this term
	case n.leaderWake <- struct{}{}:
	default:
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
	wasLeader := n.Role == Leader
	if n.CurrentTerm == term {
		n.Role = Follower
		n.LeaderID = "" // dont knwo for sure, hence will update later
	}
	if wasLeader && n.Role != Leader {
		n.pokeElectionTimer() // re-arm the timer a leader leaves stopped
	}
	granted := n.CurrentTerm == term && n.VotedFor == candidateId // if the vote is for the candidate and the term is the same
	termNow := n.CurrentTerm
	n.mu.Unlock()
	return RequestVoteResult{
		Term:        termNow,
		VoteGranted: granted,
	}, nil
}

// send heartbeats to peers, only while this node is leader.
// idles until becomeLeader pokes leaderWake, runs one stint per leader term.
func (n *Node) RunHeartbeats(ctx context.Context, rpc HeartbeatRPC) {
	for {
		snap := n.Snapshot()
		if snap.Role == Leader && rpc != nil {
			n.heartbeatStint(ctx, rpc, snap.CurrentTerm)
			continue // maybe leader again in a newer term; re-check before idling
		}
		select {
		case <-ctx.Done():
			return
		case <-n.leaderWake:
		}
	}
}

// heartbeat every peer until ctx ends or this node stops leading `term`.
// the first heartbeat goes out immediately, then one per tick.
func (n *Node) heartbeatStint(ctx context.Context, rpc HeartbeatRPC, term uint64) {
	stintCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	leading := func() bool {
		s := n.Snapshot()
		return s.Role == Leader && s.CurrentTerm == term
	}

	var wg sync.WaitGroup
	for _, peer := range n.peerIds() {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			ticker := time.NewTicker(electionTimeoutMin / 3)
			defer ticker.Stop()

			for {
				if !leading() {
					return
				}
				res, err := rpc(stintCtx, peer, HeartbeatArgs{Term: term, LeaderID: n.ID})
				if err == nil && res.Term > term {
					_ = n.stepDown(res.Term)
					return
				}
				select {
				case <-stintCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}(peer)
	}
	wg.Wait()

	// no peers (single node): nothing to send, so wait out the leadership instead of spinning
	for leading() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(electionTimeoutMin / 3):
		}
	}
}
