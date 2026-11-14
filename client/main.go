package main

import (
	"bufio"
	"fmt"
	"kvStore/config"
	"kvStore/proto"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// How long to wait between passes when every known node is down or electing.
var (
	redirectPause = config.Duration("client.redirect.pause.ms")
	retryWindow   = config.Duration("client.retry.window.ms")
	dialTimeout   = config.Duration("client.dial.timeout.ms")
)

/*
- conn := dial tcp localhost 9000
- create a reader and writer bufio ( with conn in its input ) / so we can do the flush all in one syscall
- for loop that checks users input and calls send func
- send func : does a proto.encodecommand/flush or decodeResponse and print
*/

// client that connects to one or more servers and follows the Raft leader.
func main() {
	addrs, err := serverAddresses(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	sess, err := connectAny(addrs)
	if err != nil {
		fmt.Println("connect error:", err)
		os.Exit(1)
	}
	defer sess.Close()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("store> ")
	for scanner.Scan() { // process input line by line
		line := scanner.Text() // get line that was just read ( SET a b)
		parts := strings.Fields(line)

		if len(parts) == 0 {
			fmt.Print("store> ")
			continue
		}

		var cmd proto.Command
		switch parts[0] {
		case "set":
			if len(parts) != 3 {
				fmt.Println("usage: set <key> <value>")
				fmt.Print("store> ")
				continue
			}
			// []byte(parts[1]) -> 'h' 'e' 'l' 'l' 'o'
			cmd = proto.Command{Op: proto.OpSet, Key: []byte(parts[1]), Value: []byte(parts[2])}
		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				fmt.Print("store> ")
				continue
			}
			cmd = proto.Command{Op: proto.OpGet, Key: []byte(parts[1])}
		case "del":
			if len(parts) != 2 {
				fmt.Println("usage: del <key>")
				fmt.Print("store> ")
				continue
			}
			cmd = proto.Command{Op: proto.OpDel, Key: []byte(parts[1])}
		case "ping":
			cmd = proto.Command{Op: proto.OpPing}
		case "len":
			if len(parts) != 1 {
				fmt.Println("usage: len")
				fmt.Print("store> ")
				continue
			}
			cmd = proto.Command{Op: proto.OpLen}
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command")
			fmt.Print("store> ")
			continue
		}

		resp, err := sess.Do(cmd)
		if err != nil {
			fmt.Println("request error:", err)
		} else {
			printResponse(resp)
		}
		fmt.Print("store> ")
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Scanner error: ", err)
	}
}

// reads the server addresses from the command line arguments
func serverAddresses(args []string) ([]string, error) {
	if len(args) == 0 {
		return []string{"localhost:9000"}, nil
	}

	var out []string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "--") {
			return nil, fmt.Errorf("usage: go run . [--<port> | --<host:port>]...")
		}
		body := strings.TrimPrefix(arg, "--")
		for _, part := range strings.Split(body, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			addr, err := normalizeServerArg(part)
			if err != nil {
				return nil, err
			}
			out = append(out, addr)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("usage: go run . [--<port> | --<host:port>]...")
	}
	return out, nil
}

func normalizeServerArg(part string) (string, error) {
	if strings.Contains(part, ":") {
		addr, ok := dialableAddr(part)
		if !ok {
			return "", fmt.Errorf("invalid address %q; usage: go run . --<port> or --<host:port>", part)
		}
		return addr, nil
	}

	portNumber, err := strconv.Atoi(part)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", fmt.Errorf("invalid port %q; usage: go run . --<port>", part)
	}
	return "localhost:" + part, nil
}

// session keeps one client connection and the other cluster members it can dial.
type session struct {
	members []string
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	current string
	cursor  int
}

func connectAny(addrs []string) (*session, error) {
	sess := &session{members: append([]string(nil), addrs...)}
	var last error
	for _, addr := range sess.members {
		if err := sess.connect(addr); err != nil {
			last = err
			continue
		}
		return sess, nil
	}
	if last == nil {
		last = fmt.Errorf("no server addresses")
	}
	return nil, last
}

func (s *session) Close() {
	s.closeConn()
}

func (s *session) closeConn() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.conn = nil
	s.reader = nil
	s.writer = nil
}

func (s *session) connect(addr string) error {
	s.closeConn()
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return err
	}
	s.conn = conn
	s.reader = bufio.NewReader(conn)
	s.writer = bufio.NewWriter(conn)
	s.current = addr
	s.remember(addr)
	return nil
}

func (s *session) remember(addr string) {
	for _, existing := range s.members {
		if existing == addr {
			return
		}
	}
	s.members = append(s.members, addr)
}

// Do sends one command. Writes follow a NotLeader redirect to the leader's
// client address, and try the other known members if that leader is gone.
func (s *session) Do(cmd proto.Command) (proto.Response, error) {
	write := cmd.Op == proto.OpSet || cmd.Op == proto.OpDel
	// Keep retrying for a while instead of a fixed number of attempts: after the
	// leader dies, the survivors need an election timeout plus an election
	// before any of them can name a new leader.
	deadline := time.Now().Add(retryWindow)

	var lastErr error
	failed := map[string]bool{}
	for time.Now().Before(deadline) {
		if s.conn == nil {
			addr, ok := s.nextAddr(failed)
			if !ok {
				failed = map[string]bool{}
				time.Sleep(redirectPause)
				addr, ok = s.nextAddr(failed)
				if !ok {
					break
				}
			}
			if err := s.connect(addr); err != nil {
				lastErr = err
				failed[addr] = true
				continue
			}
		}

		resp, err := s.call(cmd)
		if err != nil {
			lastErr = err
			if s.current != "" {
				failed[s.current] = true
			}
			s.closeConn()
			continue
		}
		if !write || resp.Status != proto.StatusNotLeader {
			return resp, nil
		}

		hint := strings.TrimSpace(string(resp.Value))
		addr, ok := dialableAddr(hint)
		if ok && addr != s.current && !failed[addr] {
			fmt.Printf("switching to leader %s\n", addr)
			s.closeConn()
			if err := s.connect(addr); err != nil {
				lastErr = err
				failed[addr] = true
				continue
			}
			continue
		}
		if s.current != "" {
			failed[s.current] = true
		}
		s.closeConn()
		// No usable leader yet: either the node does not know one, or it still
		// points at a leader we already found dead. Give the election time.
		time.Sleep(redirectPause)
	}
	if lastErr != nil {
		return proto.Response{}, lastErr
	}
	return proto.Response{Status: proto.StatusNotLeader}, nil
}

func (s *session) nextAddr(failed map[string]bool) (string, bool) {
	if len(s.members) == 0 {
		return "", false
	}
	for i := 0; i < len(s.members); i++ {
		addr := s.members[s.cursor%len(s.members)]
		s.cursor++
		if !failed[addr] {
			return addr, true
		}
	}
	return "", false
}

func (s *session) call(cmd proto.Command) (proto.Response, error) {
	if err := proto.EncodeCommand(s.writer, cmd); err != nil {
		return proto.Response{}, err
	}
	if err := s.writer.Flush(); err != nil {
		return proto.Response{}, err
	}
	resp, err := proto.DecodeResponse(s.reader)
	if err != nil {
		return proto.Response{}, err
	}
	return resp, nil
}

// dialableAddr reports whether hint is host:port rather than a node id.
func dialableAddr(hint string) (string, bool) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(hint))
	if err != nil || port == "" {
		return "", false
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", false
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), true
}

func printResponse(resp proto.Response) {
	switch resp.Status {
	case proto.StatusOk:
		if len(resp.Value) > 0 {
			fmt.Println(string(resp.Value))
		} else {
			fmt.Println("OK")
		}
	case proto.StatusNotFound:
		fmt.Println("key not found")
	case proto.StatusError:
		if len(resp.Value) > 0 {
			fmt.Printf("server error: %s\n", string(resp.Value))
		} else {
			fmt.Println("server error (no details provided)")
		}
	case proto.StatusNotLeader:
		if len(resp.Value) == 0 {
			fmt.Println("not leader; leader unknown")
		} else {
			fmt.Printf("not leader; leader is %s\n", string(resp.Value))
		}
	default:
		fmt.Println("unknown response status")
	}
}
