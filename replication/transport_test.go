package replication

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kvStore/proto"
	"kvStore/raft"
)

func startFakePeerServer(t *testing.T, handle func(net.Conn)) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var (
		mu    sync.Mutex
		conns = make(map[net.Conn]struct{})
		wg    sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns[conn] = struct{}{}
			mu.Unlock()

			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer func() {
					_ = c.Close()
					mu.Lock()
					delete(conns, c)
					mu.Unlock()
				}()
				handle(c)
			}(conn)
		}
	}()

	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})

	return ln.Addr().String()
}

func TestPeerSetHeartbeatNotBlockedBySlowAppendEntries(t *testing.T) {
	appendStarted := make(chan struct{})
	releaseAppend := make(chan struct{})
	var startOnce sync.Once

	addr := startFakePeerServer(t, func(conn net.Conn) {
		for {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			switch frame.messageType {
			case messageRaftAppendEntries:
				args, err := DecodeRaftAppendEntriesPayload(frame.payload)
				if err != nil {
					return
				}
				startOnce.Do(func() { close(appendStarted) })
				<-releaseAppend
				if err := EncodeRaftAppendEntriesResult(conn, raft.AppendEntriesResult{
					Term:    args.Term,
					Success: true,
				}); err != nil {
					return
				}
			case messageHeartbeat:
				args, err := DecodeHeartbeatPayload(frame.payload)
				if err != nil {
					return
				}
				if err := EncodeHeartbeatResult(conn, raft.HeartbeatResult{
					Term: args.Term,
				}); err != nil {
					return
				}
			default:
				return
			}
		}
	})

	peerSet, err := NewPeerSet([]string{"p1=" + addr})
	if err != nil {
		t.Fatalf("NewPeerSet: %v", err)
	}
	defer peerSet.Close()

	ctx := context.Background()

	type appendOutcome struct {
		res raft.AppendEntriesResult
		err error
	}
	appendDone := make(chan appendOutcome, 1)
	go func() {
		res, err := peerSet.AppendEntries(ctx, "p1", raft.AppendEntriesArgs{
			Term:     7,
			LeaderID: "leader",
		})
		appendDone <- appendOutcome{res: res, err: err}
	}()

	select {
	case <-appendStarted:
	case <-time.After(time.Second):
		close(releaseAppend)
		t.Fatal("timed out waiting for AppendEntries to reach server")
	}

	// Heartbeat must complete quickly on its own TCPPeerClient while AppendEntries is still blocked.
	hbStart := time.Now()
	hbRes, hbErr := peerSet.Heartbeat(ctx, "p1", raft.HeartbeatArgs{
		Term:     7,
		LeaderID: "leader",
	})
	hbElapsed := time.Since(hbStart)

	close(releaseAppend)

	if hbErr != nil {
		t.Fatalf("Heartbeat failed while AppendEntries blocked: %v", hbErr)
	}
	if hbRes.Term != 7 {
		t.Fatalf("Heartbeat term = %d, want 7", hbRes.Term)
	}
	if hbElapsed >= 100*time.Millisecond {
		t.Fatalf("Heartbeat took %v while AppendEntries was blocked; want < 100ms", hbElapsed)
	}

	outcome := <-appendDone
	if outcome.err != nil {
		t.Fatalf("AppendEntries failed: %v", outcome.err)
	}
	if !outcome.res.Success || outcome.res.Term != 7 {
		t.Fatalf("AppendEntries result = %+v, want Term=7 Success=true", outcome.res)
	}

	if err := peerSet.Close(); err != nil {
		t.Fatalf("PeerSet.Close: %v", err)
	}
	if _, err := peerSet.Heartbeat(ctx, "p1", raft.HeartbeatArgs{Term: 7, LeaderID: "leader"}); !errors.Is(err, errPeerClientClosed) {
		t.Fatalf("Heartbeat after Close err = %v, want errPeerClientClosed", err)
	}
	if _, err := peerSet.AppendEntries(ctx, "p1", raft.AppendEntriesArgs{Term: 7, LeaderID: "leader"}); !errors.Is(err, errPeerClientClosed) {
		t.Fatalf("AppendEntries after Close err = %v, want errPeerClientClosed", err)
	}
}

func TestHeartbeatRetriesOnceOnDroppedConnection(t *testing.T) {
	t.Run("succeeds_on_second_attempt_after_drop", func(t *testing.T) {
		var attempts atomic.Int32
		addr := startFakePeerServer(t, func(conn net.Conn) {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			if frame.messageType != messageHeartbeat {
				return
			}
			args, err := DecodeHeartbeatPayload(frame.payload)
			if err != nil {
				return
			}
			n := attempts.Add(1)
			if n == 1 {
				// Simulate dropped connection on first attempt.
				return
			}
			_ = EncodeHeartbeatResult(conn, raft.HeartbeatResult{Term: args.Term})
		})

		client := NewTCPPeerClient(addr)
		defer client.Close()

		start := time.Now()
		res, err := client.Heartbeat(context.Background(), raft.HeartbeatArgs{
			Term:     9,
			LeaderID: "leader",
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("Heartbeat failed: %v", err)
		}
		if res.Term != 9 {
			t.Fatalf("Heartbeat term = %d, want 9", res.Term)
		}
		if got := attempts.Load(); got != 2 {
			t.Fatalf("attempts = %d, want 2", got)
		}
		if elapsed >= 100*time.Millisecond {
			t.Fatalf("Heartbeat retry took %v, want < 100ms", elapsed)
		}
	})

	t.Run("recovers_from_stale_cached_connection", func(t *testing.T) {
		firstClosed := make(chan struct{})
		var connCount atomic.Int32
		addr := startFakePeerServer(t, func(conn net.Conn) {
			id := connCount.Add(1)
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			if frame.messageType != messageHeartbeat {
				return
			}
			args, err := DecodeHeartbeatPayload(frame.payload)
			if err != nil {
				return
			}
			_ = EncodeHeartbeatResult(conn, raft.HeartbeatResult{Term: args.Term})
			if id == 1 {
				// Close server side right after replying so client caches a soon-to-be-stale conn.
				_ = conn.Close()
				close(firstClosed)
				return
			}
		})

		client := NewTCPPeerClient(addr)
		defer client.Close()

		if _, err := client.Heartbeat(context.Background(), raft.HeartbeatArgs{Term: 3, LeaderID: "leader"}); err != nil {
			t.Fatalf("warmup Heartbeat failed: %v", err)
		}
		<-firstClosed
		// Give the OS loopback a moment to deliver the FIN so the cached socket is stale.
		time.Sleep(10 * time.Millisecond)

		res, err := client.Heartbeat(context.Background(), raft.HeartbeatArgs{Term: 4, LeaderID: "leader"})
		if err != nil {
			t.Fatalf("Heartbeat on stale conn failed: %v", err)
		}
		if res.Term != 4 {
			t.Fatalf("Heartbeat term = %d, want 4", res.Term)
		}
		if got := connCount.Load(); got != 2 {
			t.Fatalf("server connection count = %d, want 2", got)
		}
	})

	t.Run("stops_after_two_attempts", func(t *testing.T) {
		var attempts atomic.Int32
		addr := startFakePeerServer(t, func(conn net.Conn) {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			if frame.messageType == messageHeartbeat {
				attempts.Add(1)
			}
			// Always drop without replying.
		})

		client := NewTCPPeerClient(addr)
		defer client.Close()

		start := time.Now()
		_, err := client.Heartbeat(context.Background(), raft.HeartbeatArgs{
			Term:     11,
			LeaderID: "leader",
		})
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("expected Heartbeat error when all attempts fail")
		}
		if got := attempts.Load(); got != 2 {
			t.Fatalf("attempts = %d, want 2", got)
		}
		if elapsed >= 140*time.Millisecond {
			t.Fatalf("failed Heartbeat took %v, want well under 150ms election timeout", elapsed)
		}
	})
}

func TestAppendEntriesAllowsSlowSync(t *testing.T) {
	var attempts atomic.Int32
	addr := startFakePeerServer(t, func(conn net.Conn) {
		for {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			if frame.messageType != messageRaftAppendEntries {
				return
			}
			args, err := DecodeRaftAppendEntriesPayload(frame.payload)
			if err != nil {
				return
			}
			attempts.Add(1)
			// Exceeds the old 80ms rpc timeout, well within the 500ms append timeout.
			time.Sleep(150 * time.Millisecond)
			if err := EncodeRaftAppendEntriesResult(conn, raft.AppendEntriesResult{
				Term:    args.Term,
				Success: true,
			}); err != nil {
				return
			}
		}
	})

	client := NewTCPPeerClient(addr)
	defer client.Close()

	res, err := client.AppendEntries(context.Background(), raft.AppendEntriesArgs{
		Term:     5,
		LeaderID: "leader",
	})
	if err != nil {
		t.Fatalf("AppendEntries failed on slow sync: %v", err)
	}
	if !res.Success || res.Term != 5 {
		t.Fatalf("AppendEntries result = %+v, want Term=5 Success=true", res)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 (should not abort and retry before 500ms)", got)
	}
}

func TestRetrySleepRespectsContextCancellation(t *testing.T) {
	t.Run("sleepWithContext_cancels_promptly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()

		start := time.Now()
		err := sleepWithContext(ctx, 5*time.Second)
		elapsed := time.Since(start)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if elapsed >= 200*time.Millisecond {
			t.Fatalf("sleepWithContext took %v after cancel, want prompt return", elapsed)
		}
	})

	t.Run("rpc_aborts_retry_on_cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var attempts atomic.Int32
		addr := startFakePeerServer(t, func(conn net.Conn) {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			attempts.Add(1)
			// Return wrong frame type so attempt 1 fails with a non-EOF error and enters retry sleep.
			_ = frame
			_ = writeFrame(conn, messageHeartbeatRes, nil)
			cancel()
		})

		client := NewTCPPeerClient(addr)
		defer client.Close()

		start := time.Now()
		_, err := client.RequestVote(ctx, raft.RequestVoteArgs{Term: 1, CandidateID: "c1"})
		elapsed := time.Since(start)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RequestVote err = %v, want context.Canceled", err)
		}
		if got := attempts.Load(); got != 1 {
			t.Fatalf("attempts = %d, want 1", got)
		}
		if elapsed >= 100*time.Millisecond {
			t.Fatalf("RequestVote took %v after cancel", elapsed)
		}
	})
}

func TestTCPAppendPipelineFIFOOverSingleConnection(t *testing.T) {
	const count = 6
	var connCount atomic.Int32
	allReceived := make(chan struct{})

	addr := startFakePeerServer(t, func(conn net.Conn) {
		connCount.Add(1)
		received := make([]raft.AppendEntriesArgs, 0, count)
		// Read ALL 6 frames BEFORE sending the first reply. This only succeeds
		// if the client genuinely pipelines requests without waiting for replies.
		for len(received) < count {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			if frame.messageType != messageRaftAppendEntries {
				return
			}
			args, err := DecodeRaftAppendEntriesPayload(frame.payload)
			if err != nil {
				return
			}
			received = append(received, args)
		}
		close(allReceived)
		for _, args := range received {
			if err := EncodeRaftAppendEntriesResult(conn, raft.AppendEntriesResult{
				Term:    args.Term,
				Success: args.PrevLogIndex%2 == 0,
			}); err != nil {
				return
			}
		}
	})

	pipe := NewTCPAppendPipeline(addr)
	defer pipe.Close()

	ctx := context.Background()
	for i := 0; i < count; i++ {
		args := raft.AppendEntriesArgs{
			Term:         uint64(10 + i),
			LeaderID:     "leader",
			PrevLogIndex: uint64(i),
			PrevLogTerm:  1,
		}
		if err := pipe.Send(ctx, args, i); err != nil {
			t.Fatalf("Send(%d): %v", i, err)
		}
	}

	select {
	case <-allReceived:
	case <-time.After(time.Second):
		t.Fatal("server did not receive all pipelined frames before replying")
	}

	for i := 0; i < count; i++ {
		select {
		case resp := <-pipe.Responses():
			if resp.Err != nil {
				t.Fatalf("resp %d err = %v", i, resp.Err)
			}
			if resp.Meta.(int) != i {
				t.Fatalf("resp %d meta = %v, want %d (FIFO order)", i, resp.Meta, i)
			}
			wantTerm := uint64(10 + i)
			wantSuccess := i%2 == 0
			if resp.Res.Term != wantTerm || resp.Res.Success != wantSuccess {
				t.Fatalf("resp %d = %+v, want Term=%d Success=%v", i, resp.Res, wantTerm, wantSuccess)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for resp %d", i)
		}
	}

	if got := connCount.Load(); got != 1 {
		t.Fatalf("TCP connection count = %d, want 1", got)
	}
}

func TestTCPAppendPipelineDropFailsInFlightAndRedials(t *testing.T) {
	const inFlightCount = 4
	var connCount atomic.Int32

	addr := startFakePeerServer(t, func(conn net.Conn) {
		id := connCount.Add(1)
		if id == 1 {
			// Read all 4 pipelined frames then drop the TCP connection abruptly.
			for i := 0; i < inFlightCount; i++ {
				if _, err := readFrame(conn); err != nil {
					return
				}
			}
			_ = conn.Close()
			return
		}
		// Second connection succeeds normally.
		for {
			frame, err := readFrame(conn)
			if err != nil {
				return
			}
			args, err := DecodeRaftAppendEntriesPayload(frame.payload)
			if err != nil {
				return
			}
			if err := EncodeRaftAppendEntriesResult(conn, raft.AppendEntriesResult{
				Term:    args.Term,
				Success: true,
			}); err != nil {
				return
			}
		}
	})

	pipe := NewTCPAppendPipeline(addr)
	defer pipe.Close()

	ctx := context.Background()
	for i := 0; i < inFlightCount; i++ {
		if err := pipe.Send(ctx, raft.AppendEntriesArgs{Term: 1, LeaderID: "leader"}, i); err != nil {
			t.Fatalf("Send(%d): %v", i, err)
		}
	}

	// All 4 in-flight sends must promptly fail in order without hanging.
	for i := 0; i < inFlightCount; i++ {
		select {
		case resp := <-pipe.Responses():
			if resp.Err == nil {
				t.Fatalf("resp %d err = nil, want drop error", i)
			}
			if resp.Meta.(int) != i {
				t.Fatalf("resp %d meta = %v, want %d", i, resp.Meta, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for error on in-flight %d", i)
		}
	}

	// Next Send must transparently redial and succeed.
	if err := pipe.Send(ctx, raft.AppendEntriesArgs{Term: 2, LeaderID: "leader"}, 99); err != nil {
		t.Fatalf("Send after drop: %v", err)
	}
	select {
	case resp := <-pipe.Responses():
		if resp.Err != nil || !resp.Res.Success || resp.Res.Term != 2 || resp.Meta.(int) != 99 {
			t.Fatalf("resp after redial = %+v, want Term=2 Success=true Meta=99", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reply after redial")
	}
	if got := connCount.Load(); got != 2 {
		t.Fatalf("TCP connection count = %d, want 2", got)
	}

	if err := pipe.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := pipe.Send(ctx, raft.AppendEntriesArgs{Term: 2, LeaderID: "leader"}, 100); !errors.Is(err, errPeerClientClosed) {
		t.Fatalf("Send after Close err = %v, want errPeerClientClosed", err)
	}
}

func TestPeerSetPipelinedReplicationEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	follower, err := raft.OpenNode("b", filepath.Join(dir, "b.state"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewReplicationServer(ctx, "127.0.0.1:0", follower)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	go srv.Serve()

	peerSet, err := NewPeerSet([]string{"b=" + srv.listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer peerSet.Close()

	leader, err := raft.OpenNode("a", filepath.Join(dir, "a.state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := leader.SetPeers([]string{"b"}); err != nil {
		t.Fatal(err)
	}
	// Elect leader via Run so nextIndex/matchIndex and the term no-op are initialized normally.
	electCtx, electCancel := context.WithCancel(ctx)
	go leader.Run(electCtx, func(ctx context.Context, peerID string, args raft.RequestVoteArgs) (raft.RequestVoteResult, error) {
		return raft.RequestVoteResult{Term: args.Term, VoteGranted: true}, nil
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if leader.Status().Role == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	electCancel()
	if leader.Status().Role != raft.Leader {
		t.Fatal("leader did not become leader")
	}

	go leader.RunReplicationPipelined(ctx, peerSet.AppendPipeline)
	go leader.RunApply(ctx, func(proto.Command) {})

	const totalWrites = 150
	var wg sync.WaitGroup
	errCh := make(chan error, totalWrites)
	for i := 0; i < totalWrites; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
			defer pcancel()
			_, err := leader.Propose(pctx, proto.Command{
				Op:    proto.OpSet,
				Key:   []byte("key"),
				Value: []byte("val"),
			})
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("Propose failed: %v", err)
	}

	// +1 for leader no-op entry appended on becomeLeader.
	wantLen := totalWrites + 1
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s := follower.Snapshot()
		if len(s.Log) == wantLen && s.CommitIndex == uint64(wantLen) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	s := follower.Snapshot()
	t.Fatalf("follower log len=%d commit=%d, want %d", len(s.Log), s.CommitIndex, wantLen)
}
