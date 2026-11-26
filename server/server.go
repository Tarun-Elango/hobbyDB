package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"kvStore/config"
	"kvStore/logx"
	"kvStore/proto"
	"kvStore/raft"
	"kvStore/store"
	"net"
	"strconv"
	"sync"
	"time"
)

// How long a client write waits for Raft to copy it to a majority and apply it.
var raftProposeTimeout = config.Duration("server.raft.propose.timeout.ms")

// Where reads are served: "any" node or only the "leader".
var readFrom = mustReadFrom(config.String("server.read.from"))

func mustReadFrom(v string) string {
	if v != "any" && v != "leader" {
		panic(fmt.Sprintf("config: server.read.from must be \"any\" or \"leader\", got %q", v))
	}
	return v
}

// Config wires the client server to its Raft node.
type Config struct {
	RaftNode *raft.Node // required: leadership and writes go through Raft
	// ClientAddrs maps a Raft node id to the address clients dial.
	// A follower uses it to redirect writes to the current leader.
	ClientAddrs map[string]string
}

// Server answers client requests for one cluster member. Reads come from the
// store (filled by Raft's apply loop); writes are proposed to Raft.
type Server struct {
	store *store.Store[string, []byte]

	listener    net.Listener          // lets shutdown() close it, cause shutdown can be called from different routine,
	connections map[net.Conn]struct{} // clientConn1: {}, clientConn2: {}

	mu           sync.Mutex     // prevet race while accept/handler/shutdown - lock for shared server data
	shuttingDown bool           // prevent accepting/starting while shutting down
	handlers     sync.WaitGroup // lets shutdown() wait for all goroutines to finish
	shutDownOnce sync.Once      // helps ensure shutdown only happens once, sync.once guarantees once execution - used in shutdown

	raftNode    *raft.Node
	clientAddrs map[string]string
}

// New builds a server over st. cfg.RaftNode must be set.
func New(st *store.Store[string, []byte], cfg Config) *Server {
	if cfg.RaftNode == nil {
		panic("server: Config.RaftNode is required")
	}
	return &Server{
		store:       st,
		connections: make(map[net.Conn]struct{}),
		raftNode:    cfg.RaftNode,
		clientAddrs: copyClientAddrs(cfg.ClientAddrs),
	}
}

func copyClientAddrs(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for id, addr := range src {
		out[id] = addr
	}
	return out
}

// listen for connections, accept, handle
func (s *Server) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	// safely save the tcp listener
	// //prevetn shutdown from listening when changes are being made
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	stopWatcher := make(chan struct{}) // to help stop the below routine
	defer close(stopWatcher)

	// go routine to watch ctx.done(), call shutdown when we see it
	go func() {
		select {
		case <-ctx.Done(): // when ctrl + c di psressed or sigter,
			s.shutdown()
		case <-stopWatcher:
		}
	}()

	// for each accepted connection, we set variables and serve
	for {
		conn, err := ln.Accept() // pauses here till client connects
		if err != nil {
			// Shutdown closes the listener, which makes Accept return.
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				s.shutdown() // waits for active handlers before returning
				return nil
			}
			return err
		}

		s.mu.Lock()
		if s.shuttingDown { //reject new
			s.mu.Unlock()
			conn.Close()
			continue
		}

		s.connections[conn] = struct{}{} // a set, using map with empty value
		s.handlers.Add(1)                // counter
		s.mu.Unlock()
		logx.Printf("client connected: %s\n", conn.RemoteAddr())
		go s.serveConn(ctx, conn) // new routine serve each client conn
	}
}

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer func() {
		// cleanup variables from struct - when a client disconnets
		_ = conn.Close() // close connection
		s.mu.Lock()
		delete(s.connections, conn) // delete from pa
		s.mu.Unlock()
		s.handlers.Done() //decrememnt counter by 1
		logx.Printf("Disconnected %s \n", conn.RemoteAddr())
	}()
	if err := s.handleConn(ctx, conn); err != nil && ctx.Err() == nil {
		logx.Printf("connection %s error: %v\n", conn.RemoteAddr(), err)
	}
}

func (s *Server) shutdown() {
	s.shutDownOnce.Do(func() { //run once
		s.mu.Lock()
		s.shuttingDown = true
		ln := s.listener
		connections := make([]net.Conn, 0, len(s.connections)) //empty slice type net.conn, capacity len(s.connections)
		for conn := range s.connections {
			connections = append(connections, conn) // pulls keys from s.connnections map and add to new slice
		}
		s.mu.Unlock()
		// makes accept return
		if ln != nil {
			_ = ln.Close()
		}

		deadline := time.Now()
		for _, conn := range connections {
			_ = conn.SetDeadline(deadline) // immediately stop any live conn, by forcing timeout error
		}
		s.handlers.Wait() // wait till all goroutine have decreemented till 0
	})
}

// loop forever, every iteration 1 command
// decode call dispatch, encode resp - written back to client
func (s *Server) handleConn(ctx context.Context, conn net.Conn) error {
	// add a small memory buffer around the tcp connection - need to flush to send
	// reason - read more effecient, bufio pulls more from conn than asked
	// write can be chunked before flushing together.
	reader := bufio.NewReader(conn) // buffer 1, not blocking , blocking only when read
	writer := bufio.NewWriter(conn) // buffer 2
	for {
		// when decoder asks for byte[0], bufio pull a larger block
		// if buffer empty reads more
		cmd, err := proto.DecodeCommand(reader) // bufio implements io.reader
		if err == io.EOF {
			return nil // clean close
		}
		if err != nil {
			return err
		}

		resp, err := s.dispatch(ctx, cmd)
		if err != nil {
			return err
		}
		if err := proto.EncodeResponse(writer, resp); err != nil {
			return err
		}

		// flush will send bytes to conn
		if err := writer.Flush(); err != nil {
			return err
		}
	}
}

// perform based on command, return response obj
func (s *Server) dispatch(ctx context.Context, cmd proto.Command) (proto.Response, error) {
	switch cmd.Op {
	case proto.OpGet:
		if resp, redirect := s.readRedirect(ctx); redirect {
			return resp, nil
		}
		val, ok := store.GetBytes(s.store, cmd.Key)
		if !ok {
			return proto.Response{Status: proto.StatusNotFound}, nil
		}
		return proto.Response{Status: proto.StatusOk, Value: val}, nil
	case proto.OpSet, proto.OpDel:
		// leadership comes from Raft's current state
		return s.proposeRaftWrite(ctx, cmd)

	case proto.OpPing:
		return proto.Response{Status: proto.StatusOk}, nil

	case proto.OpLen:
		if resp, redirect := s.readRedirect(ctx); redirect {
			return resp, nil
		}
		//	do operation (get len), and send response object back
		return proto.Response{Status: proto.StatusOk, Value: []byte(strconv.Itoa(s.store.Len()))}, nil
	default:
		// shouldn't happen — Decode already validates opcode
		return proto.Response{Status: proto.StatusError}, nil

	}
}

// readRedirect applies server.read.from=leader: a node that is not the leader
// answers NotLeader (with the leader's address when known) instead of reading
// its local store. It reports false when this node may serve the read.
//
// On the leader it also waits until the store has caught up with every
// committed write (see raft.WaitLeaderRead): a just-elected or restarted
// leader rebuilds its store only after its no-op commits.
func (s *Server) readRedirect(ctx context.Context) (proto.Response, bool) {
	if readFrom != "leader" {
		return proto.Response{}, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	readCtx, cancel := context.WithTimeout(ctx, raftProposeTimeout)
	defer cancel()
	err := s.raftNode.WaitLeaderRead(readCtx)
	switch {
	case err == nil:
		return proto.Response{}, false
	case errors.Is(err, raft.ErrNotLeader):
		return notLeaderResponse(s.leaderTarget(s.raftNode.Status().LeaderID)), true
	default:
		return proto.Response{
			Status: proto.StatusError,
			Value:  []byte("timed out waiting for the leader to catch up"),
		}, true
	}
}

// proposeRaftWrite appends the command on the leader and replies only after
// a majority has the entry and this node has applied it.
// A follower answers with the leader's client address when it knows one,
// otherwise the leader's node id.
func (s *Server) proposeRaftWrite(ctx context.Context, cmd proto.Command) (proto.Response, error) {
	st := s.raftNode.Status()
	if st.Role != raft.Leader {
		return notLeaderResponse(s.leaderTarget(st.LeaderID)), nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	proposeCtx, cancel := context.WithTimeout(ctx, raftProposeTimeout)
	defer cancel()

	_, err := s.raftNode.Propose(proposeCtx, cmd)
	if err == nil {
		return proto.Response{Status: proto.StatusOk}, nil
	}
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLostLeadership) {
		return notLeaderResponse(s.leaderTarget(s.raftNode.Status().LeaderID)), nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return proto.Response{
			Status: proto.StatusError,
			Value:  []byte("timed out waiting for the write to commit"),
		}, nil
	}
	return proto.Response{}, err
}

func notLeaderResponse(leaderID string) proto.Response {
	return proto.Response{
		Status: proto.StatusNotLeader,
		Value:  []byte(leaderID),
	}
}

// leaderTarget is the dial address for leaderID, or the id itself when this
// node was not given that member's client address.
func (s *Server) leaderTarget(leaderID string) string {
	if leaderID == "" {
		return ""
	}
	if addr := s.clientAddrs[leaderID]; addr != "" {
		return addr
	}
	return leaderID
}
