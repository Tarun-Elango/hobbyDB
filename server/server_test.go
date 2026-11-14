package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"kvStore/proto"
	"kvStore/raft"
	"kvStore/store"
	"kvStore/wal"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func newTestServer(t *testing.T) *Server {
	t.Helper() // tell its a helper func

	log, err := wal.Open(filepath.Join(t.TempDir(), "data/wal.log"))
	if err != nil {
		t.Fatalf("open test WAL: %v", err)
	}
	s := New(store.NewStore[string, []byte](), log)
	t.Cleanup(s.shutdown)
	return s
}

// TestDispatch verifies each command's store-facing behavior without requiring
// a TCP listener. It also covers the only response statuses dispatch can return.
func TestDispatch(t *testing.T) {
	s := newTestServer(t)

	response, err := s.dispatch(context.Background(), proto.Command{Op: proto.OpGet, Key: []byte("missing")})
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if response.Status != proto.StatusNotFound {
		t.Fatalf("missing GET status = %d, want %d", response.Status, proto.StatusNotFound)
	}

	// SET persists bytes, and GET returns those exact bytes.
	response, err = s.dispatch(context.Background(),
		proto.Command{
			Op:    proto.OpSet,
			Key:   []byte("language"),
			Value: []byte("go"),
		})
	if response.Status != proto.StatusOk {
		t.Fatalf("SET status = %d, want %d", response.Status, proto.StatusOk)
	}

	response, err = s.dispatch(context.Background(), proto.Command{Op: proto.OpLen, Key: []byte("language")})
	if response.Status != proto.StatusOk || string(response.Value) != "1" {
		t.Fatalf("LEN response = %#v, want OK with value %q", response, "1")
	}

	// DELETE removes the key; a later GET must report it as absent.
	response, err = s.dispatch(context.Background(), proto.Command{Op: proto.OpDel, Key: []byte("language")})
	if response.Status != proto.StatusOk {
		t.Fatalf("DEL status = %d, want %d", response.Status, proto.StatusOk)
	}
	response, err = s.dispatch(context.Background(), proto.Command{Op: proto.OpGet, Key: []byte("language")})
	if response.Status != proto.StatusNotFound {
		t.Fatalf("GET after DEL status = %d, want %d", response.Status, proto.StatusNotFound)
	}

	// PING has no store side effect and always acknowledges the client.
	response, err = s.dispatch(context.Background(), proto.Command{Op: proto.OpPing})
	if response.Status != proto.StatusOk {
		t.Fatalf("PING status = %d, want %d", response.Status, proto.StatusOk)
	}

	// LEN reflects the current number of keys, not the total number of writes.
	response, err = s.dispatch(context.Background(), proto.Command{Op: proto.OpLen})
	if response.Status != proto.StatusOk || string(response.Value) != "0" {
		t.Fatalf("LEN response = %#v, want OK with value %q", response, "0")
	}

	// unknown opcode
	response, err = s.dispatch(context.Background(), proto.Command{Op: 99})
	if response.Status != proto.StatusError {
		t.Fatalf("unknown opcode status = %d, want %d", response.Status, proto.StatusError)
	}
}

// TestHandleConnMultipleCommands verifies that one client connection can carry
// several encoded requests and receives responses in the same order.
func TestHanldeConnMultipleCommands(t *testing.T) {
	s := newTestServer(t)

	serverConn, clientConn := net.Pipe() // duplex connection
	defer clientConn.Close()

	done := make(chan error, 1) // buffered channel error type, 1 size

	// handle connection async
	// send error to done channel
	// closes after handling finishes
	go func() {
		done <- s.handleConn(context.Background(), serverConn)
		_ = serverConn.Close()
	}()

	writer := bufio.NewWriter(clientConn)
	reader := bufio.NewReader(clientConn)

	commands := []proto.Command{
		{Op: proto.OpSet, Key: []byte("answer"), Value: []byte("42")},
		{Op: proto.OpGet, Key: []byte("answer")},
		{Op: proto.OpLen},
	}

	wants := []proto.Response{
		{Status: proto.StatusOk},
		{Status: proto.StatusOk, Value: []byte("42")},
		{Status: proto.StatusOk, Value: []byte("1")},
	}

	for i, command := range commands {
		if err := proto.EncodeCommand(writer, command); err != nil {
			t.Fatalf("encode command %d: %v", i, err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("flush command %d: %v", i, err)
		}
		got, err := proto.DecodeResponse(reader)
		if err != nil {
			t.Fatalf("decode response %d: %v", i, err)
		}
		if got.Status != wants[i].Status || !bytes.Equal(got.Value, wants[i].Value) {
			t.Errorf("response %d = %#v, want %#v", i, got, wants[i])
		}
	}

	clientConn.Close()
	if err := <-done; err != nil {
		t.Fatalf("handleConn returned %v after clean close, want nil", err)
	}

}

// TestHandleConnRejectsMalformedRequest ensures an invalid frame ends handling
// instead of allowing the connection to continue with an undefined request.
func TestHandleConnRejectsMalformedRequest(t *testing.T) {
	s := newTestServer(t)

	serverConn, clientConn := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- s.handleConn(context.Background(), serverConn)
		_ = serverConn.Close()
	}()

	// not valid opcode
	if _, err := clientConn.Write([]byte("99")); err != nil {
		t.Fatalf("write malformed request: %v", err)
	}

	err := <-done
	if !errors.Is(err, proto.ErrUnknownOpcode) {
		t.Fatalf("handleConn error = %v, want ErrUnknownOpcode", err)
	}
}

// TestShutdownStopsActiveHandler verifies cancellation closes an in-flight
// connection and waits until its handler has finished cleanup.
func TestShutdownStopsActiveHandler(t *testing.T) {
	s := newTestServer(t)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	s.mu.Lock()
	s.connections[serverConn] = struct{}{}
	s.handlers.Add(1)
	s.mu.Unlock()

	go s.serveConn(context.Background(), serverConn) // go routine

	finished := make(chan struct{}) // used to send signal
	go func() {
		s.shutdown()
		close(finished)
	}()

	select {
	case <-finished: // unblocks when shutdown complete, and channel closed
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after closing the active connection")
	}

	s.mu.Lock()
	_, stillTracked := s.connections[serverConn]
	s.mu.Unlock()

	if stillTracked {
		t.Fatal("closed connection is still tracked by the server")
	}

	// peer must observe that its connection cannot be used anymore
	_, err := clientConn.Write([]byte{proto.OpPing})
	if err == nil {
		t.Fatal("write to connection after shutdown succeeded, want an error")
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write after shutdown error = %v, want io.ErrClosedPipe", err)
	}
}

func TestRaftFollowerWriteReturnsLeader(t *testing.T) {
	node := raft.NewNode("b")
	node.Role = raft.Follower
	node.LeaderID = "a"
	s := NewWithConfig(store.NewStore[string, []byte](), nil, &Config{RaftNode: node})

	response, err := s.dispatch(context.Background(), proto.Command{
		Op: proto.OpSet, Key: []byte("k"), Value: []byte("v"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != proto.StatusNotLeader || string(response.Value) != "a" {
		t.Fatalf("response = %#v, want NotLeader leader a", response)
	}
	if _, ok := s.store.Get("k"); ok {
		t.Fatal("follower stored the write")
	}
}

func TestRaftFollowerWriteReturnsLeaderAddress(t *testing.T) {
	node := raft.NewNode("b")
	node.Role = raft.Follower
	node.LeaderID = "a"
	s := NewWithConfig(store.NewStore[string, []byte](), nil, &Config{
		RaftNode:    node,
		ClientAddrs: map[string]string{"a": "127.0.0.1:9000"},
	})

	response, err := s.dispatch(context.Background(), proto.Command{
		Op: proto.OpSet, Key: []byte("k"), Value: []byte("v"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != proto.StatusNotLeader || string(response.Value) != "127.0.0.1:9000" {
		t.Fatalf("response = %#v, want NotLeader 127.0.0.1:9000", response)
	}
}

func TestRaftFollowerWriteLeaderUnknown(t *testing.T) {
	node := raft.NewNode("b")
	node.Role = raft.Candidate
	s := NewWithConfig(store.NewStore[string, []byte](), nil, &Config{RaftNode: node})

	response, err := s.dispatch(context.Background(), proto.Command{
		Op: proto.OpDel, Key: []byte("k"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != proto.StatusNotLeader || len(response.Value) != 0 {
		t.Fatalf("response = %#v, want NotLeader with no leader id", response)
	}
}

func TestRaftLeaderWriteRepliesAfterApply(t *testing.T) {
	node := raft.NewNode("a")
	node.Role = raft.Leader
	node.CurrentTerm = 1
	st := store.NewStore[string, []byte]()
	s := NewWithConfig(st, nil, &Config{RaftNode: node})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go node.RunApply(ctx, func(cmd proto.Command) {
		switch cmd.Op {
		case proto.OpSet:
			st.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
		case proto.OpDel:
			st.Delete(string(cmd.Key))
		}
	})

	response, err := s.dispatch(ctx, proto.Command{
		Op: proto.OpSet, Key: []byte("k"), Value: []byte("v"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != proto.StatusOk {
		t.Fatalf("SET status = %d, want OK", response.Status)
	}
	got, ok := st.Get("k")
	if !ok || string(got) != "v" {
		t.Fatalf("store = %q ok=%v, want v", got, ok)
	}

	response, err = s.dispatch(ctx, proto.Command{Op: proto.OpDel, Key: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != proto.StatusOk {
		t.Fatalf("DEL status = %d, want OK", response.Status)
	}
	if _, ok := st.Get("k"); ok {
		t.Fatal("key still present after DEL")
	}
}

func TestReadFromLeaderRedirectsFollowerReads(t *testing.T) {
	old := readFrom
	defer func() { readFrom = old }()

	node := raft.NewNode("b")
	node.Role = raft.Follower
	node.LeaderID = "a"
	s := NewWithConfig(store.NewStore[string, []byte](), nil, &Config{
		RaftNode:    node,
		ClientAddrs: map[string]string{"a": "127.0.0.1:9000"},
	})

	for _, op := range []byte{proto.OpGet, proto.OpLen} {
		readFrom = "any"
		resp, err := s.dispatch(context.Background(), proto.Command{Op: op, Key: []byte("k")})
		if err != nil || resp.Status == proto.StatusNotLeader {
			t.Fatalf("op %d with read from any = %#v, %v; want a local answer", op, resp, err)
		}

		readFrom = "leader"
		resp, err = s.dispatch(context.Background(), proto.Command{Op: op, Key: []byte("k")})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Status != proto.StatusNotLeader || string(resp.Value) != "127.0.0.1:9000" {
			t.Fatalf("op %d with read from leader = %#v, want NotLeader 127.0.0.1:9000", op, resp)
		}
	}

	// PING never redirects.
	resp, _ := s.dispatch(context.Background(), proto.Command{Op: proto.OpPing})
	if resp.Status != proto.StatusOk {
		t.Fatalf("ping = %#v, want OK", resp)
	}

	// The leader serves reads itself.
	node.Role = raft.Leader
	resp, _ = s.dispatch(context.Background(), proto.Command{Op: proto.OpGet, Key: []byte("k")})
	if resp.Status == proto.StatusNotLeader {
		t.Fatalf("leader redirected a read: %#v", resp)
	}
}
