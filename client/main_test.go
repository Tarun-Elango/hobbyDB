package main

import (
	"bufio"
	"kvStore/proto"
	"net"
	"sync"
	"testing"
	"time"
)

func TestServerAddresses(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    []string
		wantErr bool
	}{
		{name: "default port", want: []string{"localhost:9000"}},
		{name: "custom port", args: []string{"--9010"}, want: []string{"localhost:9010"}},
		{name: "several ports", args: []string{"--9000", "--9010", "--9020"}, want: []string{"localhost:9000", "localhost:9010", "localhost:9020"}},
		{name: "host and port", args: []string{"--127.0.0.1:9000"}, want: []string{"127.0.0.1:9000"}},
		{name: "missing prefix", args: []string{"9010"}, wantErr: true},
		{name: "invalid port", args: []string{"--abc"}, wantErr: true},
		{name: "port out of range", args: []string{"--65536"}, wantErr: true},
		{name: "bare extra argument", args: []string{"--9000", "extra"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := serverAddresses(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("serverAddresses(%v) error = %v, want error: %v", tt.args, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("serverAddresses(%v) = %#v, want %#v", tt.args, got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("serverAddresses(%v) = %#v, want %#v", tt.args, got, tt.want)
				}
			}
		})
	}
}

func TestWriteFollowsLeaderAddress(t *testing.T) {
	leader := serveResponses(t, func(cmd proto.Command) proto.Response {
		return proto.Response{Status: proto.StatusOk}
	})
	defer leader.close()

	follower := serveResponses(t, func(cmd proto.Command) proto.Response {
		return proto.Response{Status: proto.StatusNotLeader, Value: []byte(leader.addr)}
	})
	defer follower.close()

	sess, err := connectAny([]string{follower.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	resp, err := sess.Do(proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != proto.StatusOk {
		t.Fatalf("status = %d, want OK", resp.Status)
	}
	if sess.current != leader.addr {
		t.Fatalf("current = %s, want leader %s", sess.current, leader.addr)
	}
}

func TestWriteFindsNextLeaderAfterCurrentDies(t *testing.T) {
	next := serveResponses(t, func(cmd proto.Command) proto.Response {
		return proto.Response{Status: proto.StatusOk}
	})
	defer next.close()

	old := serveResponses(t, nil) // accepts the write, then drops the connection
	defer old.close()
	sess, err := connectAny([]string{old.addr, next.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	resp, err := sess.Do(proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != proto.StatusOk {
		t.Fatalf("status = %d, want OK", resp.Status)
	}
	if sess.current != next.addr {
		t.Fatalf("current = %s, want next leader %s", sess.current, next.addr)
	}
}

func TestWriteTriesOtherMembersWhenLeaderIDIsNotAnAddress(t *testing.T) {
	leader := serveResponses(t, func(cmd proto.Command) proto.Response {
		return proto.Response{Status: proto.StatusOk}
	})
	defer leader.close()

	follower := serveResponses(t, func(cmd proto.Command) proto.Response {
		return proto.Response{Status: proto.StatusNotLeader, Value: []byte("node-a")}
	})
	defer follower.close()

	sess, err := connectAny([]string{follower.addr, leader.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	resp, err := sess.Do(proto.Command{Op: proto.OpDel, Key: []byte("k")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != proto.StatusOk {
		t.Fatalf("status = %d, want OK", resp.Status)
	}
}

type fakeServer struct {
	addr  string
	close func()
}

func serveResponses(t *testing.T, reply func(proto.Command) proto.Response) fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					return
				}
			}
			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				writer := bufio.NewWriter(conn)
				for {
					cmd, err := proto.DecodeCommand(reader)
					if err != nil || reply == nil {
						return
					}
					if err := proto.EncodeResponse(writer, reply(cmd)); err != nil {
						return
					}
					if err := writer.Flush(); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return fakeServer{
		addr: ln.Addr().String(),
		close: func() {
			select {
			case <-done:
			default:
				close(done)
			}
			_ = ln.Close()
		},
	}
}

// Every attempt of one write carries the same client id and seq; the next write gets seq+1.
func TestWriteRetryKeepsClientIDAndSeq(t *testing.T) {
	var seen []proto.Command
	var mu sync.Mutex
	record := func(cmd proto.Command) { mu.Lock(); seen = append(seen, cmd); mu.Unlock() }

	next := serveResponses(t, func(cmd proto.Command) proto.Response {
		record(cmd)
		return proto.Response{Status: proto.StatusOk}
	})
	defer next.close()
	dropped := serveResponses(t, func(cmd proto.Command) proto.Response {
		record(cmd)
		return proto.Response{Status: proto.StatusNotLeader, Value: []byte(next.addr)}
	})
	defer dropped.close()

	sess, err := connectAny([]string{dropped.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	if _, err := sess.Do(proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.Do(proto.Command{Op: proto.OpDel, Key: []byte("k")}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("saw %d commands, want 3", len(seen))
	}
	first, retry, second := seen[0], seen[1], seen[2]
	if first.ClientID == 0 || first.Seq != 1 || retry.ClientID != first.ClientID || retry.Seq != first.Seq {
		t.Fatalf("first %+v, retry %+v: want same non-zero id and seq 1", first, retry)
	}
	if second.ClientID != first.ClientID || second.Seq != 2 {
		t.Fatalf("second write = %+v, want same id and seq 2", second)
	}
}

// #7: when the redirect to the leader connects after the retry window has
// passed, the write is still sent there instead of ending as "leader unknown".
func TestRedirectAfterWindowStillSends(t *testing.T) {
	old := retryWindow
	retryWindow = 20 * time.Millisecond
	defer func() { retryWindow = old }()

	leader := serveResponses(t, func(cmd proto.Command) proto.Response {
		return proto.Response{Status: proto.StatusOk}
	})
	defer leader.close()
	slowFollower := serveResponses(t, func(cmd proto.Command) proto.Response {
		time.Sleep(50 * time.Millisecond) // reply after the window ends
		return proto.Response{Status: proto.StatusNotLeader, Value: []byte(leader.addr)}
	})
	defer slowFollower.close()

	sess, err := connectAny([]string{slowFollower.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	resp, err := sess.Do(proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != proto.StatusOk {
		t.Fatalf("status = %d, want OK from the leader", resp.Status)
	}
}

// Two nodes that name each other as leader must not keep the client past the window.
func TestRedirectPingPongStops(t *testing.T) {
	old := retryWindow
	retryWindow = 50 * time.Millisecond
	defer func() { retryWindow = old }()

	var a, b fakeServer
	a = serveResponses(t, func(proto.Command) proto.Response {
		return proto.Response{Status: proto.StatusNotLeader, Value: []byte(b.addr)}
	})
	defer a.close()
	b = serveResponses(t, func(proto.Command) proto.Response {
		return proto.Response{Status: proto.StatusNotLeader, Value: []byte(a.addr)}
	})
	defer b.close()

	sess, err := connectAny([]string{a.addr})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	done := make(chan struct{})
	go func() {
		_, _ = sess.Do(proto.Command{Op: proto.OpSet, Key: []byte("k"), Value: []byte("v")})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Do did not return: redirect loop ignores the retry window")
	}
}
