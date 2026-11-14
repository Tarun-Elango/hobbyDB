package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"kvStore/logx"
	"kvStore/proto"
	"kvStore/raft"
	"kvStore/replication"
	"kvStore/store"
	"kvStore/wal"
	"net"
	"strconv"
	"sync"
	"time"
)

// How long a client write waits for Raft to copy it to a majority and apply it.
const raftProposeTimeout = 5 * time.Second

// config from user input, to set the server state like role, addr
type Config struct {
	Role       replication.Role
	LeaderAddr string
	NextIndex  uint64

	LeaderReplicator *replication.Leader
	RaftNode         *raft.Node
	// ClientAddrs maps a Raft node id to the address clients dial.
	// A follower uses it to redirect writes to the current leader.
	ClientAddrs map[string]string
}

// the actual running server object with the store
// server will keep the store, contructor will create the struct,
// prevents every function receiving the store
type Server struct {
	store *store.Store[string, []byte]
	log   *wal.Log // pointer to wal.log ( has file descriptor and mutex)

	role       replication.Role
	leaderAddr string
	nextIndex  uint64

	leaderReplicator *replication.Leader

	writeMu  sync.Mutex //serialize index allocation
	writeErr error      // first WAL append/sync failure; protected by writeMu

	listener    net.Listener          // lets shutdown() close it, cause shutdown can be called from different routine,
	connections map[net.Conn]struct{} // clientConn1: {}, clientConn2: {}

	mu           sync.Mutex     // prevet race while accept/handler/shutdown - lock for shared server data
	shuttingDown bool           // prevent accepting/starting while shutting down
	handlers     sync.WaitGroup // lets shutdown() wait for all goroutines to finish
	shutDownOnce sync.Once      // helps ensure shutdown only happens once, sync.once guarantees once execution - used in shutdown
	logCloseOnce sync.Once

	raftNode    *raft.Node
	clientAddrs map[string]string
}

// constructor, takes store, returns server pointer
func New(s *store.Store[string, []byte], log *wal.Log) *Server {
	return &Server{
		store:       s,
		log:         log,
		nextIndex:   1,
		connections: make(map[net.Conn]struct{}), // make a set, we keep value as empty struct - no mem alloc
	}
}

func NewWithConfig(
	st *store.Store[string, []byte],
	log *wal.Log,
	cfg *Config,
) *Server {
	if cfg.NextIndex == 0 {
		cfg.NextIndex = 1
	}

	return &Server{
		store:              st,
		log:                log,
		role:               cfg.Role,
		leaderAddr:         cfg.LeaderAddr,
		nextIndex:          cfg.NextIndex,
		leaderReplicator: cfg.LeaderReplicator,
		connections:      make(map[net.Conn]struct{}),
		raftNode:           cfg.RaftNode,
		clientAddrs:        copyClientAddrs(cfg.ClientAddrs),
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

		if s.leaderReplicator != nil {
			s.leaderReplicator.Close()
		}

		s.logCloseOnce.Do(func() {
			if s.log != nil {
				_ = s.log.Close()
			}
		})
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
		val, ok := s.store.Get(string(cmd.Key))
		if !ok {
			return proto.Response{Status: proto.StatusNotFound}, nil
		}
		return proto.Response{Status: proto.StatusOk, Value: val}, nil
	case proto.OpSet, proto.OpDel:
		// Cluster nodes decide leadership from Raft, not from the startup role.
		if s.raftNode != nil {
			return s.proposeRaftWrite(ctx, cmd)
		}
		if s.role != replication.RoleLeader {
			return notLeaderResponse(s.leaderAddr), nil
		}

		return s.applyLeaderWrite(cmd)

	case proto.OpPing:
		return proto.Response{Status: proto.StatusOk}, nil

	case proto.OpLen:
		//	do operation (get len), and send response object back
		return proto.Response{Status: proto.StatusOk, Value: []byte(strconv.Itoa(s.store.Len()))}, nil
	default:
		// shouldn't happen — Decode already validates opcode
		return proto.Response{Status: proto.StatusError}, nil

	}
}

// proposeRaftWrite appends the command on the leader and replies only after
// a majority has the entry and this node has applied it.
// A follower answers with the leader's client address when it knows one,
// otherwise the leader's node id.
func (s *Server) proposeRaftWrite(ctx context.Context, cmd proto.Command) (proto.Response, error) {
	snap := s.raftNode.Snapshot()
	if snap.Role != raft.Leader {
		return notLeaderResponse(s.leaderTarget(snap.LeaderID)), nil
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
		return notLeaderResponse(s.leaderTarget(s.raftNode.Snapshot().LeaderID)), nil
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

// write pipeline for a standalone server that is not part of a Raft cluster.
func (s *Server) applyLeaderWrite(cmd proto.Command) (proto.Response, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// we had error set before, so return
	if s.writeErr != nil {
		return proto.Response{
			Status: proto.StatusError,
			Value:  []byte("writes are disabled after a WAL error: " + s.writeErr.Error()),
		}, nil
	}

	if s.log == nil {
		return proto.Response{
			Status: proto.StatusError,
			Value:  []byte("server WAL is unavailable"),
		}, nil
	}

	var term uint64
	if s.raftNode != nil {
		term = s.raftNode.Snapshot().CurrentTerm // current term from snapshot
	}

	entry := replication.Entry{
		Index: s.nextIndex,
		Term:  term,
		Command: proto.Command{
			Op:    cmd.Op,
			Key:   append([]byte(nil), cmd.Key...),
			Value: append([]byte(nil), cmd.Value...),
		},
	}

	record, err := recordFromEntry(entry)
	if err != nil {
		return proto.Response{
			Status: proto.StatusError,
			Value:  []byte(err.Error()),
		}, nil
	}

	//durable local write first.
	if err := s.log.Append(record); err != nil {
		// A failed append or sync may have left a partial record, or may have
		// persisted the record despite returning an error. Do not append behind
		// that uncertain state; require a restart and WAL replay first.
		s.writeErr = err
		return proto.Response{
			Status: proto.StatusError,
			Value:  []byte(err.Error()),
		}, nil
	}

	// apply after wal good
	switch cmd.Op {
	case proto.OpSet:
		s.store.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
	case proto.OpDel:
		s.store.Delete(string(cmd.Key))
	}

	s.nextIndex++

	// async replicate
	if s.leaderReplicator != nil {
		if err := s.leaderReplicator.Replicate(context.Background(), entry); err != nil {
			logx.Printf("replication enqueue error for index %d: %v\n", entry.Index, err)
		}
	}

	return proto.Response{Status: proto.StatusOk}, nil
}

func recordFromEntry(entry replication.Entry) (wal.Record, error) {
	record := wal.Record{
		Index: entry.Index,
		Term:  entry.Term, // need to make it through both leader and followers wal conversion
		Key:   append([]byte(nil), entry.Command.Key...),
		Value: append([]byte(nil), entry.Command.Value...),
	}
	switch entry.Command.Op {
	case proto.OpSet:
		record.Op = byte(wal.OpSet)
	case proto.OpDel:
		record.Op = byte(wal.OpDel)
	default:
		return wal.Record{}, fmt.Errorf(
			"unsupported replicated command op: %d",
			entry.Command.Op,
		)
	}

	return record, nil
}
