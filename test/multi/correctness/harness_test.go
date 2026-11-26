// Package correctness runs a real 5-node kvStore cluster (one cmd/server
// subprocess per node) and checks what clients can observe: linearizable
// histories, no lost acked writes, and when reads go stale.
//
// Slow, so it only runs with KV_BENCH=1. Each run appends to results.md:
//
//	KV_BENCH=1 go test -v -count=1 ./test/multi/correctness/
package correctness

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"kvStore/proto"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	clusterSize = 5
	resultFile  = "results.md"
)

var (
	serverBin string
	repoRoot  string
	binDir    string
)

func TestMain(m *testing.M) {
	if os.Getenv("KV_BENCH") != "1" {
		os.Exit(m.Run())
	}
	wd, _ := os.Getwd()
	repoRoot = filepath.Clean(filepath.Join(wd, "..", "..", ".."))
	var err error
	if binDir, err = os.MkdirTemp("", "kvcorr-"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	serverBin = filepath.Join(binDir, "kvserver")
	build := exec.Command("go", "build", "-o", serverBin, "./cmd/server")
	build.Dir = repoRoot
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building cmd/server failed:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(binDir)
	writeReport()
	os.Exit(code)
}

func requireBench(t *testing.T) {
	t.Helper()
	if os.Getenv("KV_BENCH") != "1" {
		t.Skip("set KV_BENCH=1 to run the correctness tests")
	}
}

// settingsFor writes a copy of settings.properties with server.read.from
// set to mode ("any" or "leader") and returns its path.
func settingsFor(t testing.TB, mode string) string {
	t.Helper()
	src, err := os.ReadFile(filepath.Join(repoRoot, "settings.properties"))
	if err != nil {
		t.Fatal(err)
	}
	out := regexp.MustCompile(`(?m)^server\.read\.from=.*$`).ReplaceAll(src, []byte("server.read.from="+mode))
	p := filepath.Join(t.TempDir(), "settings-"+mode+".properties")
	if err := os.WriteFile(p, out, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// ---------------------------------------------------------------- cluster

type node struct {
	id, clientAddr, replAddr, wal string
	cmd                           *exec.Cmd
	exited                        chan struct{}
	stderr                        *bytes.Buffer
	paused                        bool
}

func (n *node) alive() bool {
	if n.exited == nil {
		return false
	}
	select {
	case <-n.exited:
		return false
	default:
		return true
	}
}

type cluster struct {
	t        testing.TB
	settings string
	mu       sync.Mutex // guards node lifecycle (kill/start/pause) from fault goroutines
	nodes    []*node
}

func freeAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// startCluster boots n nodes with server.read.from=readMode on fresh WALs
// and waits for a leader.
func startCluster(t testing.TB, n int, readMode string) *cluster {
	t.Helper()
	dir := t.TempDir()
	c := &cluster{t: t, settings: settingsFor(t, readMode)}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		c.nodes = append(c.nodes, &node{
			id:         id,
			clientAddr: freeAddr(t),
			replAddr:   freeAddr(t),
			wal:        filepath.Join(dir, id+".wal"),
		})
	}
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(c.killAll)
	c.leader(10 * time.Second)
	return c
}

// start launches node i (again, after a kill) on the same ports and WAL.
func (c *cluster) start(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	me := c.nodes[i]
	if me.alive() {
		return
	}
	var peers, clientPeers []string
	for _, o := range c.nodes {
		if o != me {
			peers = append(peers, o.id+"="+o.replAddr)
			clientPeers = append(clientPeers, o.id+"="+o.clientAddr)
		}
	}
	me.cmd = exec.Command(serverBin,
		"-node-id", me.id,
		"-client-addr", me.clientAddr,
		"-replication-addr", me.replAddr,
		"-wal", me.wal,
		"-peers", strings.Join(peers, ","),
		"-client-peers", strings.Join(clientPeers, ","),
	)
	me.cmd.Dir = repoRoot
	me.cmd.Env = append(os.Environ(), "KVSTORE_SETTINGS="+c.settings)
	me.stderr = &bytes.Buffer{}
	me.cmd.Stderr = &lockedWriter{w: me.stderr}
	if err := me.cmd.Start(); err != nil {
		c.t.Error(err)
		return
	}
	me.paused = false
	me.exited = make(chan struct{})
	ex := me.exited
	go func() { _ = me.cmd.Wait(); close(ex) }()
	for deadline := time.Now().Add(10 * time.Second); ; {
		if conn, err := net.DialTimeout("tcp", me.clientAddr, time.Second); err == nil {
			conn.Close()
			return
		}
		if !me.alive() || time.Now().After(deadline) {
			c.t.Errorf("%s never listened:\n%s", me.id, me.stderr.String())
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// kill is a crash: SIGKILL, no clean shutdown.
func (c *cluster) kill(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[i]
	if n.alive() {
		_ = n.cmd.Process.Signal(syscall.SIGKILL)
		<-n.exited
	}
}

func (c *cluster) killAll() {
	for i := range c.nodes {
		c.kill(i)
	}
}

// pause freezes node i with SIGSTOP: it keeps its sockets and its belief
// about its role, but runs nothing until resume (a GC pause / VM stall).
func (c *cluster) pause(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.nodes[i].cmd.Process.Signal(syscall.SIGSTOP)
	c.nodes[i].paused = true
}

func (c *cluster) resume(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.nodes[i].cmd.Process.Signal(syscall.SIGCONT)
	c.nodes[i].paused = false
}

func (c *cluster) live() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []int
	for i, n := range c.nodes {
		if n.alive() && !n.paused {
			out = append(out, i)
		}
	}
	return out
}

func (c *cluster) byAddr(addr string) int {
	for i, n := range c.nodes {
		if n.clientAddr == addr || n.id == addr {
			return i
		}
	}
	return -1
}

// leader returns the live node that acks a probe write, polling until timeout.
func (c *cluster) leader(timeout time.Duration) int {
	c.t.Helper()
	i, ok := c.findLeader(timeout)
	if !ok {
		c.t.Fatalf("no leader within %s", timeout)
	}
	return i
}

// findLeader is leader without failing the test; safe from any goroutine.
func (c *cluster) findLeader(timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, i := range c.live() {
			resp, err := oneShot(c.nodes[i].clientAddr, proto.Command{Op: proto.OpSet, Key: []byte("__probe__"), Value: []byte("1")}, time.Second)
			if err == nil && resp.Status == proto.StatusOk {
				return i, true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1, false
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

// ---------------------------------------------------------------- client

type client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func dial(addr string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}
	return &client{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}, nil
}

func (c *client) send(cmd proto.Command) error {
	if err := proto.EncodeCommand(c.w, cmd); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *client) do(cmd proto.Command, timeout time.Duration) (proto.Response, error) {
	_ = c.conn.SetDeadline(time.Now().Add(timeout))
	if err := c.send(cmd); err != nil {
		return proto.Response{}, err
	}
	return proto.DecodeResponse(c.r)
}

func (c *client) close() { _ = c.conn.Close() }

func oneShot(addr string, cmd proto.Command, timeout time.Duration) (proto.Response, error) {
	cl, err := dial(addr)
	if err != nil {
		return proto.Response{}, err
	}
	defer cl.close()
	return cl.do(cmd, timeout)
}

// call sends cmd starting at node `start`, following NotLeader redirects
// and moving on from dead nodes, until it gets an answer or `budget` runs
// out. Writes keep their ClientID/Seq across retries, so the server applies
// them at most once.
func (c *cluster) call(start int, cmd proto.Command, budget time.Duration) (proto.Response, bool) {
	deadline := time.Now().Add(budget)
	idx := start
	for time.Now().Before(deadline) {
		resp, err := oneShot(c.nodes[idx].clientAddr, cmd, 2*time.Second)
		switch {
		case err != nil || resp.Status == proto.StatusError:
			idx = (idx + 1) % len(c.nodes)
			time.Sleep(10 * time.Millisecond)
		case resp.Status == proto.StatusNotLeader:
			if to := c.byAddr(string(resp.Value)); to >= 0 {
				idx = to
			} else {
				idx = (idx + 1) % len(c.nodes)
				time.Sleep(10 * time.Millisecond) // election in progress
			}
		default:
			return resp, true
		}
	}
	return proto.Response{}, false
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "**FAIL**"
}

// ---------------------------------------------------------------- report

type table struct {
	title, note string
	header      []string
	rows        [][]string
}

var (
	reportMu sync.Mutex
	tables   []*table
	byTitle  = map[string]*table{}
)

func record(title, note string, header, row []string) {
	reportMu.Lock()
	defer reportMu.Unlock()
	tb := byTitle[title]
	if tb == nil {
		tb = &table{title: title, note: note, header: header}
		byTitle[title] = tb
		tables = append(tables, tb)
	}
	tb.rows = append(tb.rows, row)
}

func writeReport() {
	reportMu.Lock()
	defer reportMu.Unlock()
	if len(tables) == 0 {
		return
	}
	var b strings.Builder
	b.WriteString("# Correctness, 5-node cluster\n\n")
	fmt.Fprintf(&b, "%s · %s/%s, %d CPUs, %s · 5 `cmd/server` processes on localhost · crashes = SIGKILL · stall = SIGSTOP/SIGCONT\n\n",
		time.Now().Format("2006-01-02 15:04"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	for _, tb := range tables {
		fmt.Fprintf(&b, "## %s\n\n", tb.title)
		if tb.note != "" {
			b.WriteString(tb.note + "\n\n")
		}
		b.WriteString("| " + strings.Join(tb.header, " | ") + " |\n")
		b.WriteString("|" + strings.Repeat("---|", len(tb.header)) + "\n")
		for _, r := range tb.rows {
			b.WriteString("| " + strings.Join(r, " | ") + " |\n")
		}
		b.WriteString("\n")
	}
	if err := appendReport(resultFile, b.String()); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		return
	}
	fmt.Fprintf(os.Stderr, "\nresults appended to %s\n", resultFile)
}

func appendReport(path, report string) error {
	old, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var out strings.Builder
	if trimmed := bytes.TrimRight(old, "\r\n"); len(bytes.TrimSpace(trimmed)) > 0 {
		out.Write(trimmed)
		out.WriteString("\n\n-------------------\n\n")
	}
	out.WriteString(report)
	return os.WriteFile(path, []byte(out.String()), 0o644)
}
