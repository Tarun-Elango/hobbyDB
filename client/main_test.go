package main

import (
	"bufio"
	"kvStore/proto"
	"net"
	"testing"
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
