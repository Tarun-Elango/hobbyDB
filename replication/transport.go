package replication

// Peer TCP transport for Raft RPCs (RequestVote, AppendEntries, heartbeat).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"kvStore/config"
	"kvStore/logx"
	"kvStore/raft"
	"net"
	"sync"
	"syscall"
	"time"
)

var (
	rpcAttemptTimeout       = config.Duration("replication.rpc.attempt.timeout.ms")
	rpcAttempts             = config.Int("replication.rpc.attempts")
	heartbeatAttemptTimeout = config.Duration("replication.heartbeat.attempt.timeout.ms")
	heartbeatAttempts       = config.Int("replication.heartbeat.attempts")
	appendAttemptTimeout    = config.Duration("replication.append.attempt.timeout.ms")
)

const (
	rpcRetryPause       = 20 * time.Millisecond
	heartbeatRetryPause = 5 * time.Millisecond
)

var (
	errPeerClientClosed = errors.New("replication: peer client is closed")
	errPipelineReset    = errors.New("replication: append pipeline reset")
)

type TCPPeerClient struct {
	addr string

	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

func NewTCPPeerClient(addr string) *TCPPeerClient {
	// dial later or lazily
	return &TCPPeerClient{
		addr: addr,
	}
}

// shared helper: do one rpc to the follower over a reused tcp connection
func (c *TCPPeerClient) roundTrip(
	ctx context.Context,
	write func(net.Conn) error,
	read func(net.Conn) error,
) error {
	if c == nil {
		return errors.New("replication: nil TCP peer client")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errPeerClientClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if c.conn == nil {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.addr)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("replication: dial follower %s: %w", c.addr, err)
		}
		c.conn = conn
	}

	conn := c.conn
	stopCancelWatcher := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer func() {
		if !stopCancelWatcher() {
			c.invalidateLocked(conn)
		}
	}()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			c.invalidateLocked(conn)
			return err
		}
	}
	defer conn.SetDeadline(time.Time{})

	if err := write(conn); err != nil {
		c.invalidateLocked(conn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("replication: write to %s: %w", c.addr, err)
	}

	if err := read(conn); err != nil {
		c.invalidateLocked(conn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("replication: read from %s: %w", c.addr, err)
	}
	return ctx.Err()
}

// makes current connection unusable
// to recover from broken connection
func (c *TCPPeerClient) invalidateLocked(conn net.Conn) {
	if c.conn == conn {
		c.conn = nil
	}

	_ = conn.Close()
}

// Close closes the TCP connection used for replication.
func (c *TCPPeerClient) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil
	return err
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isStaleConnError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

func (c *TCPPeerClient) RequestVote(
	ctx context.Context,
	args raft.RequestVoteArgs,
) (raft.RequestVoteResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var last error
	for attempts := 0; attempts < rpcAttempts; attempts++ {
		attemptCtx, cancel := context.WithTimeout(ctx, rpcAttemptTimeout)
		var res raft.RequestVoteResult
		err := c.roundTrip(attemptCtx,
			func(conn net.Conn) error { return EncodeRequestVote(conn, args) },
			func(conn net.Conn) error {
				frame, err := readFrame(conn)
				if err != nil {
					return err
				}
				if frame.messageType != messageRequestVoteRes {
					return fmt.Errorf("replication: unexpected frame type: %d", frame.messageType)
				}
				res, err = DecodeRequestVoteResultPayload(frame.payload)
				return err
			},
		)
		cancel()
		if err == nil {
			return res, nil
		}
		last = err
		if errors.Is(err, errPeerClientClosed) {
			return raft.RequestVoteResult{}, err
		}
		if ctx.Err() != nil {
			return raft.RequestVoteResult{}, ctx.Err()
		}
		if attempts+1 < rpcAttempts {
			if err := sleepWithContext(ctx, rpcRetryPause); err != nil {
				return raft.RequestVoteResult{}, err
			}
		}
	}
	return raft.RequestVoteResult{}, last
}

func (c *TCPPeerClient) Heartbeat(
	ctx context.Context,
	args raft.HeartbeatArgs,
) (raft.HeartbeatResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var last error
	for attempts := 0; attempts < heartbeatAttempts; attempts++ {
		attemptCtx, cancel := context.WithTimeout(ctx, heartbeatAttemptTimeout)
		var res raft.HeartbeatResult
		err := c.roundTrip(attemptCtx,
			func(conn net.Conn) error { return EncodeHeartbeat(conn, args) },
			func(conn net.Conn) error {
				frame, err := readFrame(conn)
				if err != nil {
					return err
				}
				if frame.messageType != messageHeartbeatRes {
					return fmt.Errorf("replication: unexpected frame type: %d", frame.messageType)
				}
				res, err = DecodeHeartbeatResultPayload(frame.payload)
				return err
			},
		)
		cancel()

		if err == nil {
			return res, nil
		}
		last = err
		if errors.Is(err, errPeerClientClosed) {
			return raft.HeartbeatResult{}, err
		}
		if ctx.Err() != nil {
			return raft.HeartbeatResult{}, ctx.Err()
		}
		if attempts+1 < heartbeatAttempts && !isStaleConnError(err) {
			if err := sleepWithContext(ctx, heartbeatRetryPause); err != nil {
				return raft.HeartbeatResult{}, err
			}
		}
	}
	return raft.HeartbeatResult{}, last
}

// sends append entries request from leader to follower
func (c *TCPPeerClient) AppendEntries(
	ctx context.Context,
	args raft.AppendEntriesArgs,
) (raft.AppendEntriesResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var last error
	for attempts := 0; attempts < rpcAttempts; attempts++ {
		attemptCtx, cancel := context.WithTimeout(ctx, appendAttemptTimeout)
		var res raft.AppendEntriesResult
		err := c.roundTrip(attemptCtx,
			func(conn net.Conn) error { return EncodeRaftAppendEntries(conn, args) },
			func(conn net.Conn) error {
				frame, err := readFrame(conn)
				if err != nil {
					return err
				}
				if frame.messageType != messageRaftAppendEntriesRes {
					return fmt.Errorf("replication: unexpected frame type: %d", frame.messageType)
				}
				res, err = DecodeRaftAppendEntriesResultPayload(frame.payload)
				return err
			},
		)
		cancel()
		if err == nil {
			return res, nil
		}
		last = err
		if errors.Is(err, errPeerClientClosed) {
			return raft.AppendEntriesResult{}, err
		}
		if ctx.Err() != nil {
			return raft.AppendEntriesResult{}, ctx.Err()
		}
		if attempts+1 < rpcAttempts {
			if err := sleepWithContext(ctx, rpcRetryPause); err != nil {
				return raft.AppendEntriesResult{}, err
			}
		}
	}
	return raft.AppendEntriesResult{}, last
}

const defaultPipelineRespBuffer = 64

type pendingAppend struct {
	meta     any
	deadline time.Time
}

type tcpAppendConn struct {
	conn      net.Conn
	wake      chan struct{}
	queue     []pendingAppend
	broken    bool
	failErr   error
	closeOnce sync.Once
}

func (tc *tcpAppendConn) close() {
	tc.closeOnce.Do(func() {
		_ = tc.conn.Close()
	})
}

func (tc *tcpAppendConn) signalWake() {
	select {
	case tc.wake <- struct{}{}:
	default:
	}
}

// TCPAppendPipeline pipelines AppendEntries frames over a single persistent TCP
// connection so the follower processes batches in strict FIFO order while
// overlapping network RTT with follower disk fsyncs.
type TCPAppendPipeline struct {
	addr    string
	respCh  chan raft.AppendPipelineResponse
	closeCh chan struct{}

	// Lock order: writeMu -> mu. readLoop, Reset, and Close only acquire mu.
	writeMu sync.Mutex
	mu      sync.Mutex
	active  *tcpAppendConn
	closed  bool
}

func NewTCPAppendPipeline(addr string) *TCPAppendPipeline {
	return &TCPAppendPipeline{
		addr:    addr,
		respCh:  make(chan raft.AppendPipelineResponse, defaultPipelineRespBuffer),
		closeCh: make(chan struct{}),
	}
}

func (p *TCPAppendPipeline) Responses() <-chan raft.AppendPipelineResponse {
	return p.respCh
}

// Send encodes and writes args onto the peer's single TCP connection in exact
// call order without waiting for the follower's reply. If Send returns nil,
// exactly one AppendPipelineResponse for meta will arrive on Responses()
// (unless Close is called).
func (p *TCPAppendPipeline) Send(ctx context.Context, args raft.AppendEntriesArgs, meta any) error {
	if p == nil {
		return errors.New("replication: nil TCP append pipeline")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var frameBuf bytes.Buffer
	if err := EncodeRaftAppendEntries(&frameBuf, args); err != nil {
		return err
	}
	frameBytes := frameBuf.Bytes()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	tc, err := p.ensureConn(ctx)
	if err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errPeerClientClosed
	}
	if p.active != tc || tc.broken {
		// Connection broke between ensureConn and enqueue; dial once fresh.
		p.mu.Unlock()
		tc, err = p.ensureConn(ctx)
		if err != nil {
			return err
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return errPeerClientClosed
		}
		if p.active != tc || tc.broken {
			p.mu.Unlock()
			return errPipelineReset
		}
	}
	tc.queue = append(tc.queue, pendingAppend{
		meta:     meta,
		deadline: time.Now().Add(appendAttemptTimeout),
	})
	tc.signalWake()
	p.mu.Unlock()

	// From here on, readLoop owns the queued entry and guarantees a response or
	// error on respCh.
	stopCancel := context.AfterFunc(ctx, tc.close)
	defer stopCancel()

	if err := tc.conn.SetWriteDeadline(time.Now().Add(appendAttemptTimeout)); err != nil {
		p.failConn(tc, err)
		return nil
	}
	if err := writeFull(tc.conn, frameBytes); err != nil {
		if ctx.Err() != nil {
			p.failConn(tc, ctx.Err())
		} else {
			p.failConn(tc, fmt.Errorf("replication: write to %s: %w", p.addr, err))
		}
	}
	return nil
}

func (p *TCPAppendPipeline) ensureConn(ctx context.Context) (*tcpAppendConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPeerClientClosed
	}
	if p.active != nil && !p.active.broken {
		tc := p.active
		p.mu.Unlock()
		return tc, nil
	}
	p.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, appendAttemptTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", p.addr)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("replication: dial follower %s: %w", p.addr, err)
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = conn.Close()
		return nil, errPeerClientClosed
	}
	if p.active != nil && !p.active.broken {
		tc := p.active
		p.mu.Unlock()
		_ = conn.Close()
		return tc, nil
	}
	tc := &tcpAppendConn{
		conn: conn,
		wake: make(chan struct{}, 1),
	}
	p.active = tc
	p.mu.Unlock()

	go p.readLoop(tc)
	return tc, nil
}

func (p *TCPAppendPipeline) failConn(tc *tcpAppendConn, err error) {
	p.mu.Lock()
	if !tc.broken {
		tc.broken = true
		tc.failErr = err
	}
	if p.active == tc {
		p.active = nil
	}
	tc.signalWake()
	p.mu.Unlock()
	tc.close()
}

func (p *TCPAppendPipeline) deliver(resp raft.AppendPipelineResponse) bool {
	select {
	case p.respCh <- resp:
		return true
	case <-p.closeCh:
		return false
	}
}

func (p *TCPAppendPipeline) readLoop(tc *tcpAppendConn) {
	defer tc.close()

	for {
		p.mu.Lock()
		for len(tc.queue) == 0 && !tc.broken && !p.closed {
			p.mu.Unlock()
			select {
			case <-tc.wake:
			case <-p.closeCh:
				return
			}
			p.mu.Lock()
		}
		if tc.broken || p.closed {
			remaining := tc.queue
			tc.queue = nil
			failErr := tc.failErr
			if p.active == tc {
				p.active = nil
			}
			p.mu.Unlock()
			if failErr == nil {
				if p.isClosed() {
					failErr = errPeerClientClosed
				} else {
					failErr = errPipelineReset
				}
			}
			for _, item := range remaining {
				if !p.deliver(raft.AppendPipelineResponse{Meta: item.meta, Err: failErr}) {
					return
				}
			}
			return
		}
		item := tc.queue[0]
		tc.queue = tc.queue[1:]
		p.mu.Unlock()

		var (
			res raft.AppendEntriesResult
			err error
		)
		if err = tc.conn.SetReadDeadline(item.deadline); err == nil {
			var frame replicationFrame
			frame, err = readFrame(tc.conn)
			if err == nil {
				if frame.messageType != messageRaftAppendEntriesRes {
					err = fmt.Errorf("replication: unexpected frame type: %d", frame.messageType)
				} else {
					res, err = DecodeRaftAppendEntriesResultPayload(frame.payload)
				}
			} else {
				err = fmt.Errorf("replication: read from %s: %w", p.addr, err)
			}
		}

		if err != nil {
			p.mu.Lock()
			if !tc.broken {
				tc.broken = true
				tc.failErr = err
			}
			failErr := tc.failErr
			remaining := tc.queue
			tc.queue = nil
			if p.active == tc {
				p.active = nil
			}
			p.mu.Unlock()
			tc.close()

			if !p.deliver(raft.AppendPipelineResponse{Meta: item.meta, Err: failErr}) {
				return
			}
			for _, rem := range remaining {
				if !p.deliver(raft.AppendPipelineResponse{Meta: rem.meta, Err: failErr}) {
					return
				}
			}
			return
		}

		if !p.deliver(raft.AppendPipelineResponse{Meta: item.meta, Res: res}) {
			return
		}
	}
}

func (p *TCPAppendPipeline) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// Reset closes the current TCP connection (if any) and fails any in-flight
// requests on it so the next Send dials fresh.
func (p *TCPAppendPipeline) Reset() {
	if p == nil {
		return
	}
	p.mu.Lock()
	tc := p.active
	if tc != nil {
		if !tc.broken {
			tc.broken = true
			tc.failErr = errPipelineReset
		}
		p.active = nil
		tc.signalWake()
	}
	p.mu.Unlock()
	if tc != nil {
		tc.close()
	}
}

// Close permanently shuts down the pipeline and its TCP connection.
func (p *TCPAppendPipeline) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.closeCh)
	tc := p.active
	if tc != nil {
		tc.broken = true
		tc.failErr = errPeerClientClosed
		p.active = nil
		tc.signalWake()
	}
	p.mu.Unlock()
	if tc != nil {
		tc.close()
	}
	return nil
}

// replication server owns replication listener and all peer connections
type ReplicationServer struct {
	listener net.Listener
	raftNode *raft.Node

	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	closed      bool

	wg         sync.WaitGroup
	closedOnce sync.Once
}

// NewReplicationServer binds the replication listener immediately.
// Binding errors are returned to the caller instead of being printed and ignored.
func NewReplicationServer(
	ctx context.Context,
	addr string,
	raftNode *raft.Node,
) (*ReplicationServer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if raftNode == nil {
		return nil, errors.New("replication: raft node cannot be nil")
	}
	if addr == "" {
		return nil, errors.New("replication: listen address cannot be empty")
	}

	// listne on port
	logx.Printf("creating replication server listening on %s \n", addr)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf(
			"replication: listen on %s: %w",
			addr,
			err,
		)
	}

	serverCtx, cancel := context.WithCancel(ctx) // child context

	server := &ReplicationServer{
		listener:    listener,
		raftNode:    raftNode,
		ctx:         serverCtx,
		cancel:      cancel,
		connections: make(map[net.Conn]struct{}),
	}

	// do nothing for this server until its context is canceled then close server
	go func() {
		<-serverCtx.Done() // done sends a receive only channel
		_ = server.Close()
	}()
	return server, nil

}

// accepts replication connections until close / context cancel
func (s *ReplicationServer) Serve() error {
	if s == nil {
		return errors.New("replication: nil server")
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// if follower shutting down
			if s.isClosed() ||
				errors.Is(err, net.ErrClosed) ||
				s.ctx.Err() != nil {
				return nil
			}

			// wait 100ms
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				timer := time.NewTimer(100 * time.Millisecond)

				select {
				case <-s.ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
					continue
				}
			}

			_ = s.Close()
			return fmt.Errorf("replication: accept connection: %w", err)
		}

		s.mu.Lock()

		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}

		s.connections[conn] = struct{}{}
		s.wg.Add(1)

		s.mu.Unlock()

		go s.serveConnection(conn) // spawn a new goroutine to serve the connection
	}
}

func (s *ReplicationServer) serveConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()

		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()

		s.wg.Done()
	}()

	serveReplicationConnection(s.ctx, conn, s.raftNode)
}

// stop accepting connections and close all peer connections
func (s *ReplicationServer) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closedOnce.Do(func() {
		s.cancel()

		s.mu.Lock()
		s.closed = true
		listener := s.listener
		connections := make([]net.Conn, 0, len(s.connections))
		for conn := range s.connections {
			connections = append(connections, conn)
		}
		s.mu.Unlock()

		if listener != nil {
			closeErr = listener.Close()
		}
		// Closing connections interrupts blocked JSON reads/writes.
		for _, conn := range connections {
			_ = conn.Close()
		}

		s.wg.Wait()
	})
	return closeErr
}

func (s *ReplicationServer) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// follower side loop for 1 tcp connection ( handle 1 tcp conn)
// listen for request from the same leader conn
func serveReplicationConnection(
	ctx context.Context,
	conn net.Conn,
	raftNode *raft.Node,
) {
	if conn == nil {
		return
	}
	defer conn.Close()

	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		frame, err := readFrame(conn)
		if err != nil {
			return
		}
		if frame.protocolVersion != messageProtocol {
			return
		}
		switch frame.messageType {
		case messageRequestVote:
			if raftNode == nil {
				return
			}
			args, err := DecodeRequestVotePayload(frame.payload)
			if err != nil {
				return
			}
			res, _ := raftNode.HandleRequestVote(args)
			if err := EncodeRequestVoteResult(conn, res); err != nil {
				return
			}
		case messageRaftAppendEntries:
			if raftNode == nil {
				return
			}
			args, err := DecodeRaftAppendEntriesPayload(frame.payload)
			if err != nil {
				return
			}
			res, err := raftNode.HandleAppendEntries(args)
			if err != nil {
				return
			}
			if err := EncodeRaftAppendEntriesResult(conn, res); err != nil {
				return
			}
		case messageHeartbeat:
			if raftNode == nil {
				return
			}
			args, err := DecodeHeartbeatPayload(frame.payload)
			if err != nil {
				return
			}
			_ = raftNode.ObserverLeader(args.Term, args.LeaderID)
			if err := EncodeHeartbeatResult(conn, raft.HeartbeatResult{
				Term: raftNode.Status().CurrentTerm,
			}); err != nil {
				return
			}
		default:
			return
		}
	}
}
