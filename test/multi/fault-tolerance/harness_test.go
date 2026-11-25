// Package faulttolerance runs a real 5-node kvStore cluster (one cmd/server
// subprocess per node, its own WAL dir) and kills nodes with SIGKILL to check
// what Raft promises: the cluster keeps serving with up to 2 of 5 nodes down,
// refuses writes without a quorum, and never loses an acknowledged write.
//
// Slow, so only runs with KV_BENCH=1. Each run overwrites results.md.
package faulttolerance

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
	"runtime"
	"sort"
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
)

func TestMain(m *testing.M) {
	if os.Getenv("KV_BENCH") != "1" {
		os.Exit(m.Run())
	}
	wd, _ := os.Getwd()
	repoRoot = filepath.Clean(filepath.Join(wd, "..", "..", ".."))
	tmp, err := os.MkdirTemp("", "kvft-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	serverBin = filepath.Join(tmp, "kvserver")
	build := exec.Command("go", "build", "-o", serverBin, "./cmd/server")
	build.Dir = repoRoot
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "building cmd/server failed:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(tmp)
	writeReport()
	os.Exit(code)
}

func requireBench(t *testing.T) {
	t.Helper()
	if os.Getenv("KV_BENCH") != "1" {
		t.Skip("set KV_BENCH=1 to run the fault-tolerance tests")
	}
}

func workDur() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("KV_BENCH_DUR")); err == nil {
		return d
	}
	return 3 * time.Second
}

// ---------------------------------------------------------------- cluster

type node struct {
	id, clientAddr, replAddr, wal string
	cmd                           *exec.Cmd
	exited                        chan struct{}
	stderr                        *bytes.Buffer
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
	t     testing.TB
	nodes []*node
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

// startCluster boots n nodes on fresh WAL dirs and waits for a leader.
func startCluster(t testing.TB, n int) *cluster {
	t.Helper()
	dir := t.TempDir()
	c := &cluster{t: t}
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
	c.t.Helper()
	me := c.nodes[i]
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
	me.stderr = &bytes.Buffer{}
	me.cmd.Stderr = &lockedWriter{w: me.stderr}
	if err := me.cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	me.exited = make(chan struct{})
	ex := me.exited
	go func() { _ = me.cmd.Wait(); close(ex) }()
	for deadline := time.Now().Add(10 * time.Second); ; {
		if conn, err := net.DialTimeout("tcp", me.clientAddr, time.Second); err == nil {
			conn.Close()
			return
		}
		if !me.alive() || time.Now().After(deadline) {
			c.t.Fatalf("%s never listened:\n%s", me.id, me.stderr.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// kill is a crash: SIGKILL, no clean shutdown.
func (c *cluster) kill(i int) {
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

func (c *cluster) byAddr(addr string) int {
	for i, n := range c.nodes {
		if n.clientAddr == addr || n.id == addr {
			return i
		}
	}
	return -1
}

// followers returns the indexes of live nodes other than the leader.
func (c *cluster) followers(leader int) []int {
	var out []int
	for i, n := range c.nodes {
		if i != leader && n.alive() {
			out = append(out, i)
		}
	}
	return out
}

// leader returns the node that acks a probe write, polling until timeout.
func (c *cluster) leader(timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, n := range c.nodes {
			if !n.alive() {
				continue
			}
			resp, err := oneShot(n.clientAddr, proto.Command{Op: proto.OpSet, Key: []byte("__probe__"), Value: []byte("1")}, time.Second)
			if err == nil && resp.Status == proto.StatusOk {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader within %s", timeout)
	return -1
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

func (c *client) do(cmd proto.Command, timeout time.Duration) (proto.Response, error) {
	_ = c.conn.SetDeadline(time.Now().Add(timeout))
	if err := proto.EncodeCommand(c.w, cmd); err != nil {
		return proto.Response{}, err
	}
	if err := c.w.Flush(); err != nil {
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

// ---------------------------------------------------------------- write load

type ack struct {
	at  time.Time
	lat time.Duration
	key string
}

type loadResult struct {
	acks []ack // sorted by time
	errs int
}

// writeLoad runs `conns` clients that each SET unique keys (value == key) for
// dur, following NotLeader redirects and moving to another node when theirs
// dies. Every acked key is recorded so it can be checked for loss later.
func writeLoad(c *cluster, conns int, dur time.Duration) loadResult {
	var (
		mu  sync.Mutex
		out loadResult
		wg  sync.WaitGroup
	)
	deadline := time.Now().Add(dur)
	for g := 0; g < conns; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			var acks []ack
			errs := 0
			idx := g % len(c.nodes)
			var cl *client
			move := func(to int) {
				if cl != nil {
					cl.close()
					cl = nil
				}
				if to < 0 {
					to = (idx + 1) % len(c.nodes)
				}
				idx = to
			}
			for seq := 0; time.Now().Before(deadline); {
				if cl == nil {
					var err error
					if cl, err = dial(c.nodes[idx].clientAddr); err != nil {
						move(-1)
						time.Sleep(10 * time.Millisecond)
						continue
					}
				}
				key := fmt.Sprintf("w%d-%d", g, seq)
				t0 := time.Now()
				resp, err := cl.do(proto.Command{Op: proto.OpSet, Key: []byte(key), Value: []byte(key)}, 10*time.Second)
				switch {
				case err != nil:
					errs++
					move(-1)
					time.Sleep(10 * time.Millisecond)
				case resp.Status == proto.StatusOk:
					acks = append(acks, ack{time.Now(), time.Since(t0), key})
					seq++
				case resp.Status == proto.StatusNotLeader:
					move(c.byAddr(string(resp.Value)))
					if len(resp.Value) == 0 {
						time.Sleep(10 * time.Millisecond) // election in progress
					}
				default:
					errs++
				}
			}
			if cl != nil {
				cl.close()
			}
			mu.Lock()
			out.acks = append(out.acks, acks...)
			out.errs += errs
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	sort.Slice(out.acks, func(i, j int) bool { return out.acks[i].at.Before(out.acks[j].at) })
	return out
}

// lostAcked reads every acked key from the current leader and counts the
// ones that are missing or wrong.
func lostAcked(c *cluster, acks []ack) int {
	c.t.Helper()
	ld := c.leader(10 * time.Second)
	cl, err := dial(c.nodes[ld].clientAddr)
	if err != nil {
		c.t.Fatal(err)
	}
	defer cl.close()
	lost := 0
	for _, a := range acks {
		resp, err := cl.do(proto.Command{Op: proto.OpGet, Key: []byte(a.key)}, 5*time.Second)
		if err != nil {
			c.t.Fatal(err)
		}
		if resp.Status != proto.StatusOk || string(resp.Value) != a.key {
			lost++
		}
	}
	return lost
}

func latPct(acks []ack, p float64) time.Duration {
	if len(acks) == 0 {
		return 0
	}
	l := make([]time.Duration, len(acks))
	for i, a := range acks {
		l[i] = a.lat
	}
	sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	return l[int(float64(len(l)-1)*p)]
}

func ms(d time.Duration) string {
	v := float64(d) / float64(time.Millisecond)
	if v >= 100 {
		return fmt.Sprintf("%.0f", v)
	}
	if v >= 10 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.2f", v)
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
	b.WriteString("# Fault tolerance, 5-node cluster\n\n")
	fmt.Fprintf(&b, "%s · %s/%s, %d CPUs, %s · all nodes on localhost · nodes killed with SIGKILL\n\n",
		time.Now().Format("2006-01-02 15:04"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	b.WriteString("Quorum is 3 of 5, so the cluster must survive 2 nodes down and refuse writes with 3 down. " +
		"`lost` = acked writes missing when re-read from the leader afterwards (must be 0).\n\n")
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
	if err := os.WriteFile(resultFile, []byte(b.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		return
	}
	fmt.Fprintf(os.Stderr, "\nresults written to %s\n", resultFile)
}
