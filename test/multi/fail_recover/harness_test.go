// Package failrecover runs a real 5-node kvStore cluster (one cmd/server
// subprocess per node, its own WAL) and checks failure and recovery: leader
// election time, a node rejoining, a full-cluster crash, network partitions
// and a rolling restart.
//
// Every node-to-node replication link goes through a small TCP proxy owned by
// the test, so a partition is "the proxy black-holes the link" (bytes go in,
// nothing comes out, RPCs time out) without touching the OS network.
//
// Slow, so only runs with KV_BENCH=1. Each run appends to results.md.
package failrecover

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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const (
	clusterSize = 5
	writerConns = 8
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
	tmp, err := os.MkdirTemp("", "kvfr-bin-")
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
		t.Skip("set KV_BENCH=1 to run the failure/recovery tests")
	}
}

// ---------------------------------------------------------------- cluster

type node struct {
	id, clientAddr, replAddr, wal string
	peerAddrs                     []string // this node's view of each peer (a proxy), by index
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
	t       testing.TB
	nodes   []*node
	proxies [][]*proxy // proxies[from][to]

	netMu  sync.Mutex
	groups []int // partition group per node; equal everywhere = healed
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

// startCluster boots n nodes on fresh WAL dirs, wires every peer link through
// a proxy, and waits for a leader.
func startCluster(t testing.TB, n int) *cluster {
	t.Helper()
	dir := t.TempDir()
	c := &cluster{t: t, groups: make([]int, n)}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		c.nodes = append(c.nodes, &node{
			id:         id,
			clientAddr: freeAddr(t),
			replAddr:   freeAddr(t),
			wal:        filepath.Join(dir, id+".wal"),
		})
	}
	c.proxies = make([][]*proxy, n)
	for i := range c.nodes {
		c.proxies[i] = make([]*proxy, n)
		c.nodes[i].peerAddrs = make([]string, n)
		for j := range c.nodes {
			if i != j {
				p := newProxy(t, c, i, j)
				c.proxies[i][j] = p
				c.nodes[i].peerAddrs[j] = p.ln.Addr().String()
			}
		}
	}
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(func() {
		c.killAll()
		for _, row := range c.proxies {
			for _, p := range row {
				if p != nil {
					p.close()
				}
			}
		}
	})
	c.leader(10 * time.Second)
	return c
}

// start launches node i (again, after a kill) on the same ports and WAL.
func (c *cluster) start(i int) {
	c.t.Helper()
	me := c.nodes[i]
	var peers, clientPeers []string
	for j, o := range c.nodes {
		if j != i {
			peers = append(peers, o.id+"="+me.peerAddrs[j])
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

// stop is a clean shutdown: SIGTERM, SIGKILL after 5 s.
func (c *cluster) stop(i int) {
	n := c.nodes[i]
	if !n.alive() {
		return
	}
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-n.exited:
	case <-time.After(5 * time.Second):
		c.kill(i)
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

func (c *cluster) all() []int {
	out := make([]int, len(c.nodes))
	for i := range out {
		out[i] = i
	}
	return out
}

func (c *cluster) followers(leader int) []int {
	var out []int
	for i, n := range c.nodes {
		if i != leader && n.alive() {
			out = append(out, i)
		}
	}
	return out
}

// leader returns the live node that acks a probe write, polling until timeout.
func (c *cluster) leader(timeout time.Duration) int {
	c.t.Helper()
	return c.leaderAmong(c.all(), timeout)
}

// leaderAmong only probes the given nodes, so a partitioned old leader (whose
// writes hang) is never asked.
func (c *cluster) leaderAmong(idx []int, timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, i := range idx {
			if !c.nodes[i].alive() {
				continue
			}
			resp, err := oneShot(c.nodes[i].clientAddr, proto.Command{Op: proto.OpSet, Key: []byte("__probe__"), Value: []byte("1")}, time.Second)
			if err == nil && resp.Status == proto.StatusOk {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
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

// ---------------------------------------------------------------- network

// proxy carries replication traffic from node `from` to node `to`. When the
// link is cut it accepts connections and swallows their bytes (black hole),
// so the sender's RPCs time out the same way they would on a dead link.
type proxy struct {
	ln       net.Listener
	c        *cluster
	from, to int

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newProxy(t testing.TB, c *cluster, from, to int) *proxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, c: c, from: from, to: to, conns: map[net.Conn]struct{}{}}
	go p.serve()
	return p
}

func (p *proxy) track(cs ...net.Conn) {
	p.mu.Lock()
	for _, c := range cs {
		p.conns[c] = struct{}{}
	}
	p.mu.Unlock()
}

func (p *proxy) untrack(cs ...net.Conn) {
	p.mu.Lock()
	for _, c := range cs {
		delete(p.conns, c)
		_ = c.Close()
	}
	p.mu.Unlock()
}

// dropAll closes every connection through this proxy; the sender redials and
// gets whatever the link's new state is.
func (p *proxy) dropAll() {
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
	p.mu.Unlock()
}

func (p *proxy) close() {
	_ = p.ln.Close()
	p.dropAll()
}

func (p *proxy) serve() {
	for {
		in, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.c.cut(p.from, p.to) {
			p.track(in)
			go func() { _, _ = io.Copy(io.Discard, in); p.untrack(in) }()
			continue
		}
		out, err := net.DialTimeout("tcp", p.c.nodes[p.to].replAddr, time.Second)
		if err != nil {
			_ = in.Close()
			continue
		}
		p.track(in, out)
		go func() { _, _ = io.Copy(out, in); p.untrack(in, out) }()
		go func() { _, _ = io.Copy(in, out); p.untrack(in, out) }()
	}
}

func (c *cluster) cut(a, b int) bool {
	c.netMu.Lock()
	defer c.netMu.Unlock()
	return c.groups[a] != c.groups[b]
}

// partition puts the listed nodes in their own group; everyone else stays in
// group 0. Only links whose state changes are reset.
func (c *cluster) partition(side []int) {
	g := make([]int, len(c.nodes))
	for _, i := range side {
		g[i] = 1
	}
	c.setGroups(g)
}

func (c *cluster) heal() { c.setGroups(make([]int, len(c.nodes))) }

func (c *cluster) setGroups(g []int) {
	c.netMu.Lock()
	old := c.groups
	c.groups = g
	c.netMu.Unlock()
	for i := range c.nodes {
		for j := range c.nodes {
			if i != j && (old[i] != old[j]) != (g[i] != g[j]) {
				c.proxies[i][j].dropAll()
			}
		}
	}
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

func get(c *cluster, i int, key string) string {
	r, err := oneShot(c.nodes[i].clientAddr, proto.Command{Op: proto.OpGet, Key: []byte(key)}, 2*time.Second)
	switch {
	case err != nil:
		return "err"
	case r.Status == proto.StatusOk:
		return string(r.Value)
	case r.Status == proto.StatusNotFound:
		return "missing"
	}
	return fmt.Sprintf("status%d", r.Status)
}

func set(c *cluster, i int, key, val string, timeout time.Duration) (proto.Response, error) {
	return oneShot(c.nodes[i].clientAddr, proto.Command{Op: proto.OpSet, Key: []byte(key), Value: []byte(val)}, timeout)
}

func statusName(r proto.Response, err error) string {
	if err != nil {
		return "client timeout"
	}
	switch r.Status {
	case proto.StatusOk:
		return "OK"
	case proto.StatusNotLeader:
		return "NotLeader"
	case proto.StatusError:
		return "Error"
	}
	return fmt.Sprint(r.Status)
}

// keyCount is the node's local store size (reads are answered locally).
func keyCount(c *cluster, i int) int {
	r, err := oneShot(c.nodes[i].clientAddr, proto.Command{Op: proto.OpLen}, time.Second)
	if err != nil || r.Status != proto.StatusOk {
		return -1
	}
	n, _ := strconv.Atoi(string(r.Value))
	return n
}

// waitConverged polls until every live node holds the same number of keys.
func waitConverged(c *cluster, timeout time.Duration) (time.Duration, bool) {
	t0 := time.Now()
	for time.Since(t0) < timeout {
		want, same := -2, true
		for i, n := range c.nodes {
			if !n.alive() {
				continue
			}
			k := keyCount(c, i)
			if want == -2 {
				want = k
			}
			if k < 0 || k != want {
				same = false
				break
			}
		}
		if same {
			return time.Since(t0), true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return time.Since(t0), false
}

// ---------------------------------------------------------------- write load

type ack struct {
	at  time.Time
	key string
}

type loadResult struct {
	acks []ack // sorted by time
	errs int
}

var loadBatch atomic.Int64

// writeLoad runs `conns` clients that SET unique keys (value == key) for dur,
// following NotLeader redirects and moving on when their node is down or cut
// off. Every acked key is recorded so it can be checked for loss later.
func writeLoad(c *cluster, conns int, dur time.Duration) loadResult {
	batch := loadBatch.Add(1)
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
				key := fmt.Sprintf("b%d-w%d-%d", batch, g, seq)
				resp, err := cl.do(proto.Command{Op: proto.OpSet, Key: []byte(key), Value: []byte(key)}, 6*time.Second)
				switch {
				case err != nil:
					errs++
					move(-1)
					time.Sleep(10 * time.Millisecond)
				case resp.Status == proto.StatusOk:
					acks = append(acks, ack{time.Now(), key})
					seq++
				case resp.Status == proto.StatusNotLeader:
					move(c.byAddr(string(resp.Value)))
					if len(resp.Value) == 0 {
						time.Sleep(10 * time.Millisecond) // election in progress
					}
				default: // commit timeout / lost leadership: unknown outcome, try elsewhere
					errs++
					seq++
					move(-1)
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

// lostOn counts acked keys that node i does not hold with the right value.
func lostOn(c *cluster, i int, acks []ack) int {
	c.t.Helper()
	cl, err := dial(c.nodes[i].clientAddr)
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

// lostAnywhere is the worst lostOn over all live nodes (after convergence).
func lostAnywhere(c *cluster, acks []ack) int {
	worst := 0
	for i, n := range c.nodes {
		if n.alive() {
			if l := lostOn(c, i, acks); l > worst {
				worst = l
			}
		}
	}
	return worst
}

// maxGap is the longest stretch with no acked write between from and to.
func maxGap(acks []ack, from, to time.Time) time.Duration {
	gap, prev := time.Duration(0), from
	for _, a := range acks {
		if a.at.Before(from) || a.at.After(to) {
			continue
		}
		if g := a.at.Sub(prev); g > gap {
			gap = g
		}
		prev = a.at
	}
	if g := to.Sub(prev); g > gap {
		gap = g
	}
	return gap
}

func countBetween(acks []ack, from, to time.Time) int {
	n := 0
	for _, a := range acks {
		if !a.at.Before(from) && a.at.Before(to) {
			n++
		}
	}
	return n
}

func rate(n int, d time.Duration) string { return fmt.Sprintf("%.0f", float64(n)/d.Seconds()) }

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
	b.WriteString("# Failure and recovery, 5-node cluster\n\n")
	fmt.Fprintf(&b, "%s · %s/%s, %d CPUs, %s · localhost · crash = SIGKILL · partition = test proxy black-holes peer links\n\n",
		time.Now().Format("2006-01-02 15:04"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	b.WriteString("`lost` = acked writes missing on any node after recovery (must be 0). `converge` = time until every node holds the same key count.\n\n")
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
