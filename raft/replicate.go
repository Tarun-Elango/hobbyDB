package raft

import (
	"context"
	"errors"
	"fmt"
	"kvStore/config"
	"kvStore/proto"
	"sync"
	"time"
)

var (
	// ErrNotLeader means this node cannot accept the write.
	ErrNotLeader = errors.New("not leader")
	// ErrLostLeadership means this node stepped down before the write was applied.
	ErrLostLeadership = errors.New("lost leadership before apply")
)

var (
	appendEntriesBatchLimit = config.Int("raft.append.entries.batch.limit")
	maxAppendEntriesPayload = config.Int("raft.append.entries.max.payload")
	appendMaxInFlight       = mustPositiveInt("raft.append.max.inflight", config.Int("raft.append.max.inflight"))
	appendMaxInFlightBytes  = mustPositiveInt("raft.append.max.inflight.bytes", config.Int("raft.append.max.inflight.bytes"))
	appendRetryBackoff      = config.Duration("raft.append.retry.backoff.ms")
	proposeMaxBatchSize     = mustPositiveInt("raft.propose.max.batch.size", config.Int("raft.propose.max.batch.size"))
	// how often an idle leader re-sends its commit index to a caught-up peer
	commitRefreshInterval = electionTimeoutMin
)

func mustPositiveInt(key string, n int) int {
	if n <= 0 {
		panic(fmt.Sprintf("config: %s must be positive, got %d", key, n))
	}
	return n
}

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

// AppendPipelineResponse delivers the outcome of one pipelined AppendEntries RPC.
type AppendPipelineResponse struct {
	Meta any
	Res  AppendEntriesResult
	Err  error
}

// AppendPipeline sends AppendEntries RPCs to a single peer in FIFO order without
// waiting for each reply before sending the next.
//
// Contract: if Send returns nil, exactly one AppendPipelineResponse with that
// meta will be delivered on Responses() (or abandoned only when the pipeline is
// permanently closed). If Send returns a non-nil error, no response is delivered.
type AppendPipeline interface {
	Send(ctx context.Context, args AppendEntriesArgs, meta any) error
	Responses() <-chan AppendPipelineResponse
	Reset()
}

// AppendPipelineOpener returns a per-peer AppendPipeline.
type AppendPipelineOpener func(peerID string) (AppendPipeline, error)

// ---- shared (leader + follower log helpers)

func (n *Node) logTermAtLocked(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index == n.SnapshotIndex {
		return n.SnapshotTerm, true
	}
	if len(n.Log) == 0 {
		return 0, false
	}
	first := n.Log[0].Index
	if index < first {
		return 0, false
	}
	pos := index - first
	if pos >= uint64(len(n.Log)) {
		return 0, false
	}
	entry := n.Log[pos]
	if entry.Index != index {
		return 0, false
	}
	return entry.Term, true
}

func (n *Node) logSliceFromLocked(index uint64) ([]LogEntry, bool) {
	if index == 0 || len(n.Log) == 0 {
		return nil, false
	}
	first := n.Log[0].Index
	if index < first {
		return nil, false
	}
	pos := index - first
	if pos >= uint64(len(n.Log)) {
		return nil, false
	}
	return n.Log[pos:], true
}

// trimLogPrefixLocked drops all in-memory log entries with Index <= throughIndex
// and copies the remaining tail into a fresh slice so GC can reclaim old entries.
func (n *Node) trimLogPrefixLocked(throughIndex uint64) {
	if len(n.Log) == 0 {
		return
	}
	first := n.Log[0].Index
	if throughIndex < first {
		return
	}
	drop := throughIndex - first + 1
	if drop >= uint64(len(n.Log)) {
		n.Log = nil
		return
	}
	tail := make([]LogEntry, len(n.Log)-int(drop))
	copy(tail, n.Log[drop:])
	n.Log = tail
}

func logEntryWireSize(entry LogEntry) int {
	return 8 + 8 + 1 + 4 + len(entry.Command.Key) + 4 + len(entry.Command.Value) + 8 + 8 // + client id, seq
}

func cloneLogEntry(entry LogEntry) LogEntry {
	return LogEntry{
		Index: entry.Index,
		Term:  entry.Term,
		Command: proto.Command{
			Op:       entry.Command.Op,
			Key:      append([]byte(nil), entry.Command.Key...),
			Value:    append([]byte(nil), entry.Command.Value...),
			ClientID: entry.Command.ClientID,
			Seq:      entry.Command.Seq,
		},
	}
}

// ---- leader funcs

type proposeReq struct {
	ctx  context.Context
	cmd  proto.Command
	done chan proposeResult
}

type proposeResult struct {
	entry LogEntry
	err   error
}

// records the command in the leader's log using group commit (opportunistic drain).
// If no WAL persist is currently running, the first caller flushes immediately
// with zero extra wait; concurrent callers arriving while that persist+fsync is
// in flight queue up and are drained together (up to proposeMaxBatchSize) in a
// single WAL write + fsync as soon as the current flush finishes.
// returns only after that entry is committed by a majority and applied to the store.
// commitIndex moves inside this call only when this node is already a majority (a single-node cluster).
func (n *Node) Propose(ctx context.Context, cmd proto.Command) (LogEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return LogEntry{}, err
	}
	if n.Status().Role != Leader {
		return LogEntry{}, ErrNotLeader
	}

	req := &proposeReq{
		ctx: ctx,
		cmd: proto.Command{
			Op:       cmd.Op,
			Key:      append([]byte(nil), cmd.Key...),
			Value:    append([]byte(nil), cmd.Value...),
			ClientID: cmd.ClientID,
			Seq:      cmd.Seq,
		},
		done: make(chan proposeResult, 1),
	}

	var initialBatch []*proposeReq
	n.proposeMu.Lock()
	if !n.proposeFlushing {
		n.proposeFlushing = true
		initialBatch = []*proposeReq{req}
	} else {
		n.proposeQueue = append(n.proposeQueue, req)
	}
	n.proposeMu.Unlock()

	if initialBatch != nil {
		n.flushProposeBatch(initialBatch)
		n.proposeMu.Lock()
		if len(n.proposeQueue) > 0 {
			// Hand off draining the remaining queued proposals so this caller
			// proceeds directly to apply-wait without waiting on later batches.
			go n.drainProposeLoop()
		} else {
			n.proposeFlushing = false
		}
		n.proposeMu.Unlock()
	}

	var res proposeResult
	select {
	case res = <-req.done:
	case <-ctx.Done():
		return LogEntry{}, ctx.Err()
	}
	if res.err != nil {
		return LogEntry{}, res.err
	}
	return n.waitProposeApplied(ctx, res.entry)
}

func (n *Node) drainProposeLoop() {
	for {
		batch := n.popProposeBatch()
		if len(batch) == 0 {
			return
		}
		n.flushProposeBatch(batch)
	}
}

func (n *Node) popProposeBatch() []*proposeReq {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()

	if len(n.proposeQueue) == 0 {
		n.proposeFlushing = false
		return nil
	}

	limit := proposeMaxBatchSize
	if limit < 1 {
		limit = 1
	}
	if limit > len(n.proposeQueue) {
		limit = len(n.proposeQueue)
	}

	batch := make([]*proposeReq, limit)
	copy(batch, n.proposeQueue[:limit])
	remaining := copy(n.proposeQueue, n.proposeQueue[limit:])
	clear(n.proposeQueue[remaining:])
	n.proposeQueue = n.proposeQueue[:remaining]
	return batch
}

func (n *Node) flushProposeBatch(batch []*proposeReq) {
	live := batch[:0]
	for _, req := range batch {
		if err := req.ctx.Err(); err != nil {
			req.done <- proposeResult{err: err}
			continue
		}
		live = append(live, req)
	}
	if len(live) == 0 {
		return
	}

	n.mu.Lock()
	if n.Role != Leader {
		n.mu.Unlock()
		for _, req := range live {
			req.done <- proposeResult{err: ErrNotLeader}
		}
		return
	}

	lastIndex, _ := n.lastLogLocked()
	term := n.CurrentTerm
	entries := make([]LogEntry, len(live))
	for i, req := range live {
		entries[i] = LogEntry{
			Index:   lastIndex + uint64(i) + 1,
			Term:    term,
			Command: req.cmd,
		}
	}

	// Durable before anyone can count the entries: one WAL write and one fsync
	// for the entire batch.
	if err := n.persistAppendBatchLocked(entries); err != nil {
		n.mu.Unlock()
		for _, req := range live {
			req.done <- proposeResult{err: err}
		}
		return
	}
	if n.onProposeBatch != nil {
		n.onProposeBatch(len(entries))
	}

	n.Log = append(n.Log, entries...)
	n.advanceCommitLocked()

	for _, ch := range n.peerWake {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	n.mu.Unlock()

	for i, req := range live {
		req.done <- proposeResult{entry: entries[i]}
	}
}

func (n *Node) waitProposeApplied(ctx context.Context, entry LogEntry) (LogEntry, error) {
	// Fast path: if already applied while still leader in entry.Term, return
	// without acquiring n.mu (which may be held by the next batch's WAL fsync).
	n.metaMu.RLock()
	if n.Role != Leader || n.CurrentTerm != entry.Term {
		n.metaMu.RUnlock()
		return LogEntry{}, ErrLostLeadership
	}
	if n.LastApplied >= entry.Index {
		n.metaMu.RUnlock()
		return entry, nil
	}
	n.metaMu.RUnlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Role != Leader || n.CurrentTerm != entry.Term {
		return LogEntry{}, ErrLostLeadership
	}
	if n.LastApplied >= entry.Index {
		return entry, nil
	}
	if ctx.Err() != nil {
		return LogEntry{}, ctx.Err()
	}

	// ctx cancel has to wake this wait; applyCond alone only fires on commit/apply.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			n.mu.Lock()
			n.applyCond.Broadcast()
			n.mu.Unlock()
		case <-stop:
		}
	}()

	for n.LastApplied < entry.Index {
		if ctx.Err() != nil {
			return LogEntry{}, ctx.Err()
		}
		if n.Role != Leader || n.CurrentTerm != entry.Term {
			return LogEntry{}, ErrLostLeadership
		}
		n.applyCond.Wait()
	}
	return entry, nil
}

// start one goroutine per peer to send append entries requests
func (n *Node) RunReplication(ctx context.Context, rpc AppendEntriesRPC) {
	if rpc == nil {
		return
	}
	n.RunReplicationPipelined(ctx, func(peer string) (AppendPipeline, error) {
		return newRPCAppendPipeline(ctx, peer, rpc), nil
	})
}

// RunReplicationPipelined starts one pipelined replication worker per peer.
func (n *Node) RunReplicationPipelined(ctx context.Context, open AppendPipelineOpener) {
	if open == nil {
		return
	}
	peers := n.peerIds()
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			n.replicatePeerPipeline(ctx, peer, open)
		}(peer)
	}
	wg.Wait()
}

type rpcAppendPipeline struct {
	parentCtx context.Context
	peer      string
	rpc       AppendEntriesRPC
	respCh    chan AppendPipelineResponse

	mu          sync.Mutex
	epochCtx    context.Context
	epochCancel context.CancelFunc
}

func newRPCAppendPipeline(ctx context.Context, peer string, rpc AppendEntriesRPC) *rpcAppendPipeline {
	capSize := appendMaxInFlight * 2
	if capSize < 4 {
		capSize = 4
	}
	epochCtx, epochCancel := context.WithCancel(ctx)
	return &rpcAppendPipeline{
		parentCtx:   ctx,
		peer:        peer,
		rpc:         rpc,
		respCh:      make(chan AppendPipelineResponse, capSize),
		epochCtx:    epochCtx,
		epochCancel: epochCancel,
	}
}

func (p *rpcAppendPipeline) Send(ctx context.Context, args AppendEntriesArgs, meta any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	epochCtx := p.epochCtx
	p.mu.Unlock()

	go func() {
		res, err := p.rpc(epochCtx, p.peer, args)
		select {
		case p.respCh <- AppendPipelineResponse{Meta: meta, Res: res, Err: err}:
		case <-p.parentCtx.Done():
		}
	}()
	return nil
}

func (p *rpcAppendPipeline) Responses() <-chan AppendPipelineResponse {
	return p.respCh
}

func (p *rpcAppendPipeline) Reset() {
	p.mu.Lock()
	p.epochCancel()
	p.epochCtx, p.epochCancel = context.WithCancel(p.parentCtx)
	p.mu.Unlock()
}

type replicateMode uint8

const (
	modeProbe replicateMode = iota
	modeReplicate
)

type inflightAppend struct {
	seq             uint64
	gen             uint64
	term            uint64
	prevLogIndex    uint64
	lastSent        uint64
	nEntries        int
	wireBytes       int
	effectiveCommit uint64 // min(args.LeaderCommit, matchedThrough)
}

func oldestActiveStart(active []inflightAppend) (uint64, bool) {
	var minStart uint64
	found := false
	for _, inf := range active {
		if inf.nEntries == 0 {
			continue
		}
		start := inf.prevLogIndex + 1
		if !found || start < minStart {
			minStart = start
			found = true
		}
	}
	return minStart, found
}

func knownCommitForTerm(sentCommit, sentTerm uint64, active []inflightAppend, term uint64) uint64 {
	var known uint64
	if sentTerm == term {
		known = sentCommit
	}
	for _, inf := range active {
		if inf.term == term && inf.effectiveCommit > known {
			known = inf.effectiveCommit
		}
	}
	return known
}

// per peer: keep up to appendMaxInFlight AppendEntries RPCs in flight once
// log matching is confirmed (modeReplicate), dropping back to single-RPC
// probing (modeProbe) on leadership start, log mismatch, or transport error.
func (n *Node) replicatePeerPipeline(ctx context.Context, peer string, open AppendPipelineOpener) {
	n.mu.RLock()
	wake := n.peerWake[peer]
	n.mu.RUnlock()
	if wake == nil || open == nil {
		return
	}

	pipeline, err := open(peer)
	if err != nil || pipeline == nil {
		return
	}
	defer pipeline.Reset()

	var (
		mode          = modeProbe
		activeTerm    uint64
		sendNext      uint64
		gen           uint64
		nextSeq       uint64
		active        []inflightAppend
		activeBytes   int
		totalInFlight int

		// highest commit index (and term) confirmed acknowledged by this peer
		// through any successful AppendEntries (empty or non-empty)
		sentCommit uint64
		sentTerm   uint64

		backoffUntil time.Time
	)

	resetPipelineState := func(newTerm uint64, newNext uint64) {
		activeTerm = newTerm
		sendNext = newNext
		gen++
		mode = modeProbe
		active = active[:0]
		activeBytes = 0
		sentCommit = 0
		sentTerm = 0
		backoffUntil = time.Time{}
		pipeline.Reset()
	}

	rewindOnTransportError := func() {
		if start, ok := oldestActiveStart(active); ok {
			sendNext = start
		}
		gen++
		mode = modeProbe
		active = active[:0]
		activeBytes = 0
		backoffUntil = time.Now().Add(appendRetryBackoff)
		pipeline.Reset()
	}

	removeActive := func(seq uint64) bool {
		for i, inf := range active {
			if inf.seq == seq {
				activeBytes -= inf.wireBytes
				if activeBytes < 0 {
					activeBytes = 0
				}
				copy(active[i:], active[i+1:])
				active = active[:len(active)-1]
				return true
			}
		}
		return false
	}

	handleResponse := func(resp AppendPipelineResponse) {
		if totalInFlight > 0 {
			totalInFlight--
		}
		inf, ok := resp.Meta.(inflightAppend)
		if !ok {
			return
		}
		// Always check for a higher term, even on a stale generation reply.
		if resp.Err == nil && resp.Res.Term > inf.term {
			_ = n.stepDown(resp.Res.Term)
			if inf.gen == gen {
				resetPipelineState(0, 0)
			}
			return
		}
		if inf.gen != gen || inf.term != activeTerm {
			return
		}
		if resp.Err != nil {
			rewindOnTransportError()
			return
		}
		if !resp.Res.Success {
			newNext := n.handleAppendMismatch(peer, inf.term, inf.prevLogIndex)
			gen++
			mode = modeProbe
			active = active[:0]
			activeBytes = 0
			sentCommit = 0
			if newNext > 0 {
				sendNext = newNext
			}
			return
		}

		// Successful AppendEntries (empty or non-empty).
		removeActive(inf.seq)
		if sentTerm != inf.term {
			sentTerm = inf.term
			sentCommit = 0
		}
		if inf.effectiveCommit > sentCommit {
			sentCommit = inf.effectiveCommit
		}
		if inf.nEntries > 0 {
			n.handleAppendResult(peer, inf.term, inf.lastSent, inf.nEntries, resp.Res)
			if inf.lastSent+1 > sendNext {
				sendNext = inf.lastSent + 1
			}
		}
		mode = modeReplicate
	}

	respCh := pipeline.Responses()

	for {
		if ctx.Err() != nil {
			return
		}

		// Sync with current leadership and term.
		role, term, nextIdx := n.peerLeaderState(peer)
		if role != Leader {
			if activeTerm != 0 {
				resetPipelineState(0, 0)
			}
			select {
			case <-ctx.Done():
				return
			case resp := <-respCh:
				handleResponse(resp)
			case <-wake:
			}
			continue
		}
		if term != activeTerm {
			resetPipelineState(term, nextIdx)
		}

		maxTotal := appendMaxInFlight
		if maxTotal < 1 {
			maxTotal = 1
		}
		window := 1
		if mode == modeReplicate {
			window = maxTotal
		}

		inBackoff := !backoffUntil.IsZero() && time.Now().Before(backoffUntil)
		if !inBackoff {
			backoffUntil = time.Time{}
			for len(active) < window && totalInFlight < maxTotal {
				if len(active) > 0 && activeBytes >= appendMaxInFlightBytes {
					break
				}
				args, sendTerm, lastSent, wireBytes, action := n.nextAppendAt(peer, sendNext)
				if action == appendNeedSnapshot {
					if n.getInstallSnapshotRPC() == nil {
						break
					}
					// Drain/reset pipelined AppendEntries before streaming snapshot.
					gen++
					mode = modeProbe
					active = active[:0]
					activeBytes = 0
					pipeline.Reset()

					snapIdx, sentOK, steppedDown := n.sendSnapshotToPeer(ctx, peer, activeTerm)
					if steppedDown {
						resetPipelineState(0, 0)
						break
					}
					if !sentOK {
						backoffUntil = time.Now().Add(appendRetryBackoff)
						break
					}
					sendNext = snapIdx + 1
					sentCommit = snapIdx
					sentTerm = activeTerm
					mode = modeReplicate
					continue
				}
				if action == appendNone {
					var ok bool
					knownCommit := knownCommitForTerm(sentCommit, sentTerm, active, activeTerm)
					args, sendTerm, _, ok = n.nextCommitUpdate(peer, knownCommit, activeTerm)
					if !ok {
						break
					}
					wireBytes = 0
					lastSent = args.PrevLogIndex
				}
				if sendTerm != activeTerm {
					break
				}

				matchedThrough := args.PrevLogIndex
				if len(args.Entries) > 0 {
					matchedThrough = lastSent
				}
				effectiveCommit := args.LeaderCommit
				if matchedThrough < effectiveCommit {
					effectiveCommit = matchedThrough
				}

				nextSeq++
				inf := inflightAppend{
					seq:             nextSeq,
					gen:             gen,
					term:            sendTerm,
					prevLogIndex:    args.PrevLogIndex,
					lastSent:        lastSent,
					nEntries:        len(args.Entries),
					wireBytes:       wireBytes,
					effectiveCommit: effectiveCommit,
				}
				if err := pipeline.Send(ctx, args, inf); err != nil {
					if ctx.Err() != nil {
						return
					}
					rewindOnTransportError()
					break
				}
				totalInFlight++
				active = append(active, inf)
				activeBytes += wireBytes
				if len(args.Entries) > 0 {
					sendNext = lastSent + 1
				}
			}
		}

		// Re-check backoff state after dispatch attempts.
		if !backoffUntil.IsZero() {
			delay := time.Until(backoffUntil)
			if delay <= 0 {
				backoffUntil = time.Time{}
				continue
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case resp := <-respCh:
				timer.Stop()
				handleResponse(resp)
			case <-timer.C:
				backoffUntil = time.Time{}
			}
			continue
		}

		// If requests are in flight, wait for responses (and wake only if we have
		// spare pipeline capacity so a full window doesn't busy-spin on wakes).
		if len(active) > 0 {
			canSendMore := len(active) < window && totalInFlight < maxTotal && activeBytes < appendMaxInFlightBytes
			var wakeCh <-chan struct{}
			if canSendMore {
				wakeCh = wake
			}
			select {
			case <-ctx.Done():
				return
			case resp := <-respCh:
				handleResponse(resp)
			case <-wakeCh:
			}
			continue
		}

		// If stale replies are still occupying all slots, drain before idling.
		if totalInFlight >= maxTotal {
			select {
			case <-ctx.Done():
				return
			case resp := <-respCh:
				handleResponse(resp)
			}
			continue
		}

		// Idle. Heartbeats do not carry the commit index, so a follower that
		// restarted (CommitIndex back to 0, or its log wiped) would otherwise
		// stay behind until the next write. Re-send the commit index now and
		// then; a wiped follower fails the prev-log check and nextIndex walks back.
		refresh := time.NewTimer(commitRefreshInterval)
		select {
		case <-ctx.Done():
			refresh.Stop()
			return
		case resp := <-respCh:
			refresh.Stop()
			handleResponse(resp)
		case <-wake:
			refresh.Stop()
		case <-refresh.C:
			sentCommit = 0
		}
	}
}

func (n *Node) peerLeaderState(peer string) (Role, uint64, uint64) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.Role != Leader {
		return n.Role, n.CurrentTerm, 0
	}
	return n.Role, n.CurrentTerm, n.nextIndex[peer]
}

type appendAction uint8

const (
	appendNone appendAction = iota
	appendSend
	appendNeedSnapshot
)

// build one AppendEntriesArgs for this peer starting at next; reports whether
// the peer is caught up (appendNone) or requires InstallSnapshot (appendNeedSnapshot).
func (n *Node) nextAppendAt(peer string, next uint64) (AppendEntriesArgs, uint64, uint64, int, appendAction) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.Role != Leader {
		return AppendEntriesArgs{}, 0, 0, 0, appendNone
	}
	if next == 0 {
		if n.SnapshotIndex > 0 {
			return AppendEntriesArgs{}, n.CurrentTerm, 0, 0, appendNeedSnapshot
		}
		return AppendEntriesArgs{}, 0, 0, 0, appendNone
	}
	lastIdx, _ := n.lastLogLocked()
	if next > lastIdx {
		return AppendEntriesArgs{}, 0, 0, 0, appendNone
	}

	prevIndex := next - 1
	prevTerm, ok := n.logTermAtLocked(prevIndex)
	if !ok {
		return AppendEntriesArgs{}, n.CurrentTerm, 0, 0, appendNeedSnapshot
	}
	slice, ok := n.logSliceFromLocked(next)
	if !ok || len(slice) == 0 {
		return AppendEntriesArgs{}, n.CurrentTerm, 0, 0, appendNeedSnapshot
	}

	header := 8 + 2 + len(n.ID) + 8 + 8 + 8 + 4 // term, id len, id, prev index, prev term, leader commit, count
	entries := make([]LogEntry, 0, appendEntriesBatchLimit)
	size := header
	for _, entry := range slice {
		entrySize := logEntryWireSize(entry)
		if len(entries) > 0 && (len(entries) >= appendEntriesBatchLimit || size+entrySize > maxAppendEntriesPayload) {
			break
		}
		entries = append(entries, cloneLogEntry(entry))
		size += entrySize
	}
	if len(entries) == 0 {
		return AppendEntriesArgs{}, 0, 0, 0, appendNone
	}

	lastSent := entries[len(entries)-1].Index
	return AppendEntriesArgs{
		Term:         n.CurrentTerm,
		LeaderID:     n.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.CommitIndex,
	}, n.CurrentTerm, lastSent, size, appendSend
}

// build one AppendEntriesArgs for this peer; if caught up, return false
func (n *Node) nextAppend(peer string) (AppendEntriesArgs, uint64, uint64, bool) {
	n.mu.RLock()
	next := n.nextIndex[peer]
	n.mu.RUnlock()
	args, term, lastSent, _, action := n.nextAppendAt(peer, next)
	return args, term, lastSent, action == appendSend
}

// build an empty AppendEntries that only carries the leader's commit index.
// The previous-log check uses matchIndex, so the follower can only commit
// what it is known to hold. Returns the commit index this send tells the peer about.
func (n *Node) nextCommitUpdate(peer string, sentCommit, sentTerm uint64) (AppendEntriesArgs, uint64, uint64, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.Role != Leader {
		return AppendEntriesArgs{}, 0, 0, false
	}
	if sentTerm != n.CurrentTerm {
		sentCommit = 0 // told in an older term: this leader has not told the peer anything yet
	}
	match := n.matchIndex[peer]
	effective := n.CommitIndex
	if match < effective {
		effective = match
	}
	if effective == 0 || effective <= sentCommit {
		return AppendEntriesArgs{}, 0, 0, false
	}
	prevTerm, ok := n.logTermAtLocked(match)
	if !ok {
		return AppendEntriesArgs{}, 0, 0, false
	}
	return AppendEntriesArgs{
		Term:         n.CurrentTerm,
		LeaderID:     n.ID,
		PrevLogIndex: match,
		PrevLogTerm:  prevTerm,
		LeaderCommit: n.CommitIndex,
	}, n.CurrentTerm, effective, true
}

// handleAppendMismatch rewinds nextIndex[peer] after a rejected AppendEntries
// and returns the new nextIndex to probe from.
func (n *Node) handleAppendMismatch(peer string, term uint64, prevLogIndex uint64) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Role != Leader || n.CurrentTerm != term {
		return 0
	}
	next := n.nextIndex[peer]
	if prevLogIndex < next {
		next = prevLogIndex
	} else if next > 1 {
		next--
	}
	if next < 1 {
		next = 1
	}
	n.nextIndex[peer] = next
	return next
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
	if nEntries > 0 {
		// move nextIndex past what was accepted (without moving backward on
		// out-of-order replies), even when matchIndex is already higher (a peer
		// that lost its log after an earlier match): otherwise the same batch is
		// resent forever.
		if lastSent+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = lastSent + 1
		}
		if lastSent > n.matchIndex[peer] {
			n.matchIndex[peer] = lastSent
			n.advanceCommitLocked()
		}
	}
}

// A write is committed once a majority of the cluster has it in the log.
// Count this leader as one copy. Only an entry from the current term may
// move commitIndex; every earlier entry commits with it.
func (n *Node) advanceCommitLocked() {
	if n.Role != Leader {
		return
	}
	prev := n.CommitIndex
	for i := len(n.Log) - 1; i >= 0; i-- {
		entry := n.Log[i]
		if entry.Index <= n.CommitIndex {
			break
		}
		if entry.Term != n.CurrentTerm {
			continue
		}
		if n.copiesAtLocked(entry.Index) >= n.quorumLocked() {
			n.setCommitLocked(entry.Index, entry.Term)
			if n.CommitIndex > prev {
				n.applyCond.Broadcast()
				// tell followers the new commit index even if they have no new entries
				for _, ch := range n.peerWake {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
			}
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
	st := n.Status()
	if args.Term < st.CurrentTerm {
		return AppendEntriesResult{Term: st.CurrentTerm, Success: false}, nil
	}
	// Reset the election timer before any disk I/O or lock acquisition so a slow
	// WAL fsync or SetPersistentState cannot trigger a spurious election.
	n.pokeElectionTimer()
	if args.Term > st.CurrentTerm {
		if err := n.SetPersistentState(args.Term, ""); err != nil {
			return AppendEntriesResult{}, err
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.CurrentTerm != args.Term {
		return AppendEntriesResult{Term: n.CurrentTerm, Success: false}, nil
	}
	wasLeader := n.Role == Leader
	n.setMetaLocked(Follower, args.LeaderID)
	if wasLeader {
		n.cancelReadRound()
		n.applyCond.Broadcast()
		n.pokeElectionTimer() // re-arm the timer a leader leaves stopped
	}

	entries := args.Entries
	if args.PrevLogIndex < n.SnapshotIndex {
		// Stale or overlapping AppendEntries whose prefix is already covered by
		// our committed snapshot. Never reject or truncate snapshotted entries.
		if len(entries) == 0 {
			n.advanceFollowerCommitLocked(args.LeaderCommit, args.PrevLogIndex)
			n.pokeElectionTimer()
			return AppendEntriesResult{Term: n.CurrentTerm, Success: true}, nil
		}
		lastEntry := entries[len(entries)-1]
		if lastEntry.Index <= n.SnapshotIndex {
			n.advanceFollowerCommitLocked(args.LeaderCommit, lastEntry.Index)
			n.pokeElectionTimer()
			return AppendEntriesResult{Term: n.CurrentTerm, Success: true}, nil
		}
		skip := n.SnapshotIndex - args.PrevLogIndex
		if skip == 0 || skip > uint64(len(entries)) {
			n.pokeElectionTimer()
			return AppendEntriesResult{Term: n.CurrentTerm, Success: false}, nil
		}
		snapEntry := entries[skip-1]
		if snapEntry.Index != n.SnapshotIndex || snapEntry.Term != n.SnapshotTerm {
			n.pokeElectionTimer()
			return AppendEntriesResult{Term: n.CurrentTerm, Success: false}, nil
		}
		entries = entries[skip:]
	} else {
		prevTerm, ok := n.logTermAtLocked(args.PrevLogIndex)
		if !ok || prevTerm != args.PrevLogTerm {
			n.pokeElectionTimer()
			return AppendEntriesResult{Term: n.CurrentTerm, Success: false}, nil
		}
	}

	var toAppend []LogEntry
	for _, entry := range entries {
		if entry.Index <= n.SnapshotIndex {
			continue
		}
		if len(toAppend) == 0 {
			haveTerm, exists := n.logTermAtLocked(entry.Index)
			if exists && haveTerm != entry.Term {
				if err := n.persistTruncateLocked(entry.Index); err != nil {
					return AppendEntriesResult{}, err
				}
				first := n.Log[0].Index
				n.Log = n.Log[:entry.Index-first]
				exists = false
			}
			if exists {
				continue
			}
		}
		lastIdx, _ := n.lastLogLocked()
		if entry.Index != lastIdx+1+uint64(len(toAppend)) {
			return AppendEntriesResult{Term: n.CurrentTerm, Success: false}, nil
		}
		toAppend = append(toAppend, entry)
	}
	if len(toAppend) > 0 {
		if err := n.persistAppendBatchLocked(toAppend); err != nil {
			return AppendEntriesResult{}, err
		}
		for _, entry := range toAppend {
			n.Log = append(n.Log, cloneLogEntry(entry))
		}
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
		commitTerm, _ := n.logTermAtLocked(commit)
		n.setCommitLocked(commit, commitTerm)
		n.applyCond.Broadcast()
	}
}
