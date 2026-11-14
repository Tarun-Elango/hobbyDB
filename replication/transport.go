package replication

// Peer TCP transport for Raft RPCs (RequestVote, AppendEntries, heartbeat).

import (
	"context"
	"errors"
	"fmt"
	"kvStore/logx"
	"kvStore/raft"
	"net"
	"sync"
	"time"
)

const (
	rpcAttemptTimeout = 80 * time.Millisecond
	rpcAttempts       = 3
)

type PeerClient interface {
	Append(ctx context.Context, req AppendRequest) (AppendResponse, error)
	Close() error
}

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
		return errors.New("replication: peer client is closed")
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
	defer stopCancelWatcher()

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

// sends replication request from leader to follower
// lock client
// check if client closed expired
// lazy connect c.conn == nil
// register context cancellation -AfterFunc
// apply context deadline
// encode and send appendrequest
// decode appendresponse
// if fails call invalidateLocked
// send resp to leader
func (c *TCPPeerClient) Append(ctx context.Context, req AppendRequest) (AppendResponse, error) {
	if c == nil {
		return AppendResponse{}, errors.New("replication: nil TCP peer client")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return AppendResponse{}, errors.New("replication: peer client is closed")
	}
	if err := ctx.Err(); err != nil {
		return AppendResponse{}, err
	}

	if c.conn == nil {
		dialer := net.Dialer{}

		// c is follower, this func called by leader
		conn, err := dialer.DialContext(ctx, "tcp", c.addr)
		if err != nil {
			if ctx.Err() != nil {
				return AppendResponse{}, ctx.Err()
			}
			return AppendResponse{}, fmt.Errorf(
				"replication: dial follower %s: %w",
				c.addr,
				err,
			)
		}
		c.conn = conn
	}

	conn := c.conn

	stopCancelWatcher := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stopCancelWatcher()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			c.invalidateLocked(conn)
			return AppendResponse{}, err
		}
	}

	defer conn.SetDeadline(time.Time{})

	if err := EncodeAppendRequest(conn, req); err != nil { // req to json, then writes to tcp conn
		c.invalidateLocked(conn)

		if ctx.Err() != nil {
			return AppendResponse{}, ctx.Err()
		}
		return AppendResponse{}, fmt.Errorf(
			"replication: write append request to %s: %w",
			c.addr,
			err,
		)
	}

	resp, err := DecodeAppendResponse(conn)

	if err != nil {
		c.invalidateLocked(conn)

		if ctx.Err() != nil {
			return AppendResponse{}, ctx.Err()
		}
		return AppendResponse{}, fmt.Errorf(
			"replication: read append response from %s: %w",
			c.addr,
			err,
		)
	}

	if err := ctx.Err(); err != nil {
		return AppendResponse{}, err
	}

	return resp, nil

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

func (c *TCPPeerClient) RequestVote(
	ctx context.Context,
	args raft.RequestVoteArgs,
) (raft.RequestVoteResult, error) {
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
		if ctx.Err() != nil {
			return raft.RequestVoteResult{}, ctx.Err()
		}
		time.Sleep(20 * time.Millisecond)
	}
	return raft.RequestVoteResult{}, last
}

func (c *TCPPeerClient) Heartbeat(
	ctx context.Context,
	args raft.HeartbeatArgs,
) (raft.HeartbeatResult, error) {
	var last error
	for attempts := 0; attempts < rpcAttempts; attempts++ {
		attemptCtx, cancel := context.WithTimeout(ctx, rpcAttemptTimeout)
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
		if ctx.Err() != nil {
			return raft.HeartbeatResult{}, ctx.Err()
		}
		time.Sleep(20 * time.Millisecond)
	}
	return raft.HeartbeatResult{}, last
}

// sends append entries request from leader to follower
func (c *TCPPeerClient) AppendEntries(
	ctx context.Context,
	args raft.AppendEntriesArgs,
) (raft.AppendEntriesResult, error) {
	var last error
	for attempts := 0; attempts < rpcAttempts; attempts++ {
		attemptCtx, cancel := context.WithTimeout(ctx, rpcAttemptTimeout)
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
		if ctx.Err() != nil {
			return raft.AppendEntriesResult{}, ctx.Err()
		}
		time.Sleep(20 * time.Millisecond)
	}
	return raft.AppendEntriesResult{}, last
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

		go s.serveConnection(conn)
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
		case messageAppendRequest:
			return
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
				Term: raftNode.Snapshot().CurrentTerm,
			}); err != nil {
				return
			}
		default:
			return
		}
	}
}
