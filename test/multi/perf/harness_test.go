// Package perf benchmarks a real multi-node kvStore cluster (one cmd/server
// subprocess per node, its own WAL dir): latency percentiles, scaling with
// cluster size, mixed workloads, concurrency sweeps, and slow followers.
//
// Slow, so only runs with KV_BENCH=1. Each run overwrites results.md's tables.
package perf

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"kvStore/proto"
	"math/rand/v2"
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
	resultFile  = "results.md"
	preloadKeys = 200
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
	tmp, err := os.MkdirTemp("", "kvperf-bin-")
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
		t.Skip("set KV_BENCH=1 to run the perf tests")
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
	peerAddrs                     []string // where this node dials each peer (direct or a proxy)
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
	proxies [][]*proxy // proxies[from][to], nil when not proxied
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

// startCluster boots n nodes on fresh WAL dirs and waits for a leader. With
// proxied, every peer link goes through a test proxy that can add latency.
func startCluster(t testing.TB, n int, proxied bool) *cluster {
	t.Helper()
	dir := t.TempDir()
	c := &cluster{t: t}
	if proxied { // listen first so freeAddr can't hand out a proxy's port
		c.proxies = make([][]*proxy, n)
		for i := 0; i < n; i++ {
			c.proxies[i] = make([]*proxy, n)
			for j := 0; j < n; j++ {
				if i != j {
					c.proxies[i][j] = newProxy(t)
				}
			}
		}
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		c.nodes = append(c.nodes, &node{
			id:         id,
			clientAddr: freeAddr(t),
			replAddr:   freeAddr(t),
			wal:        filepath.Join(dir, id+".wal"),
			peerAddrs:  make([]string, n),
		})
	}
	for _, me := range c.nodes {
		for j, o := range c.nodes {
			me.peerAddrs[j] = o.replAddr
		}
	}
	for i, row := range c.proxies {
		for j, p := range row {
			if p != nil {
				p.target = c.nodes[j].replAddr
				go p.serve()
				c.nodes[i].peerAddrs[j] = p.ln.Addr().String()
			}
		}
	}
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.kill(i)
		}
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
	args := []string{"-node-id", me.id, "-client-addr", me.clientAddr,
		"-replication-addr", me.replAddr, "-wal", me.wal}
	if len(peers) > 0 {
		args = append(args, "-peers", strings.Join(peers, ","), "-client-peers", strings.Join(clientPeers, ","))
	}
	me.cmd = exec.Command(serverBin, args...)
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

func (c *cluster) kill(i int) {
	n := c.nodes[i]
	if n.alive() {
		_ = n.cmd.Process.Signal(syscall.SIGKILL)
		<-n.exited
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

func (c *cluster) followers(leader int) []int {
	var out []int
	for i := range c.nodes {
		if i != leader {
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
	for _, n := range c.nodes {
		out := n.stderr.String()
		c.t.Logf("%s alive=%v stderr tail:\n%s", n.id, n.alive(), out[max(0, len(out)-1500):])
	}
	c.t.Fatalf("no leader within %s", timeout)
	return -1
}

// preload writes keys k0..k(preloadKeys-1) through the leader, retrying until
// each is acked, then waits until every node has applied all of them.
func (c *cluster) preload() {
	c.t.Helper()
	ld := c.leader(10 * time.Second)
	for k := 0; k < preloadKeys; k++ {
		cmd := proto.Command{Op: proto.OpSet, Key: []byte(fmt.Sprintf("k%d", k)), Value: val128}
		for deadline := time.Now().Add(10 * time.Second); ; {
			resp, err := oneShot(c.nodes[ld].clientAddr, cmd, 5*time.Second)
			if err == nil && resp.Status == proto.StatusOk {
				break
			}
			if time.Now().After(deadline) {
				c.t.Fatalf("preload k%d: %v %v", k, resp.Status, err)
			}
			ld = c.leader(10 * time.Second)
		}
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		ok := true
		for _, n := range c.nodes {
			resp, err := oneShot(n.clientAddr, proto.Command{Op: proto.OpGet, Key: []byte(fmt.Sprintf("k%d", preloadKeys-1))}, time.Second)
			ok = ok && err == nil && resp.Status == proto.StatusOk
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatal("preloaded keys never reached every node")
		}
		time.Sleep(20 * time.Millisecond)
	}
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

// ---------------------------------------------------------------- slow link

// proxy forwards one directed peer link and delays every chunk, in both
// directions, by its current delay (one-way latency). Order is preserved.
type proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	delay time.Duration
	conns map[net.Conn]struct{}
}

func newProxy(t testing.TB) *proxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return &proxy{ln: ln, conns: map[net.Conn]struct{}{}}
}

func (p *proxy) setDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

func (p *proxy) getDelay() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.delay
}

func (p *proxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

func (p *proxy) serve() {
	for {
		in, err := p.ln.Accept()
		if err != nil {
			return
		}
		out, err := net.DialTimeout("tcp", p.target, time.Second)
		if err != nil {
			_ = in.Close()
			continue
		}
		p.mu.Lock()
		p.conns[in], p.conns[out] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(out, in)
		go p.pipe(in, out)
	}
}

type chunk struct {
	due time.Time
	b   []byte
}

// pipe copies src -> dst, holding each chunk until read time + delay.
func (p *proxy) pipe(dst, src net.Conn) {
	q := make(chan chunk, 4096)
	go func() {
		defer close(q)
		for {
			buf := make([]byte, 32<<10)
			n, err := src.Read(buf)
			if n > 0 {
				q <- chunk{time.Now().Add(p.getDelay()), buf[:n]}
			}
			if err != nil {
				return
			}
		}
	}()
	for ch := range q {
		if d := time.Until(ch.due); d > 0 {
			time.Sleep(d)
		}
		if _, err := dst.Write(ch.b); err != nil {
			break
		}
	}
	_ = dst.Close()
	_ = src.Close()
}

// clearSlowLinks resets every proxy link delay to 0.
func (c *cluster) clearSlowLinks() {
	for _, row := range c.proxies {
		for _, p := range row {
			if p != nil {
				p.setDelay(0)
			}
		}
	}
}

// slowLinks delays the leader <-> follower links for each listed follower.
func (c *cluster) slowLinks(leader int, followers []int, oneWay time.Duration) {
	for _, f := range followers {
		c.proxies[leader][f].setDelay(oneWay)
		c.proxies[f][leader].setDelay(oneWay)
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

// ---------------------------------------------------------------- load

var val128 = bytes.Repeat([]byte("v"), 128)

type stats struct {
	reads, writes []time.Duration
	errs          int
	errKinds      map[string]int
	redirects     int // NotLeader replies to writes (leader moved or unknown)
	dur           time.Duration
}

func (s stats) readRate() float64  { return float64(len(s.reads)) / s.dur.Seconds() }
func (s stats) writeRate() float64 { return float64(len(s.writes)) / s.dur.Seconds() }

// load runs `conns` closed-loop clients for dur. Each op is a GET with
// probability readFrac, else a SET of a random preloaded key (128 B).
// Client g reads from node g%n (server.read.from=any) and sends writes to
// the leader, following NotLeader redirects.
func load(c *cluster, conns int, readFrac float64, dur time.Duration) stats {
	ld := c.leader(5 * time.Second)
	var (
		mu  sync.Mutex
		out = stats{errKinds: map[string]int{}}
		wg  sync.WaitGroup
	)
	start := time.Now()
	deadline := start.Add(dur)
	for g := 0; g < conns; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 7))
			var reads, writes []time.Duration
			errs, redirects := 0, 0
			kinds := map[string]int{}
			kind := func(op string, r proto.Response, err error) {
				k := op + ":" + fmt.Sprint(r.Status)
				if err != nil {
					k = op + ":" + err.Error()
					if len(k) > 60 {
						k = k[len(k)-60:]
					}
				}
				kinds[k]++
			}
			rIdx, wIdx := g%len(c.nodes), ld
			var rc, wc *client
			defer func() {
				for _, cl := range []*client{rc, wc} {
					if cl != nil {
						cl.close()
					}
				}
			}()
			for time.Now().Before(deadline) {
				key := []byte(fmt.Sprintf("k%d", rng.IntN(preloadKeys)))
				isRead := rng.Float64() < readFrac
				var err error
				if isRead && rc == nil {
					rc, err = dial(c.nodes[rIdx].clientAddr)
				} else if !isRead && wc == nil {
					wc, err = dial(c.nodes[wIdx].clientAddr)
				}
				if err != nil {
					errs++
					time.Sleep(10 * time.Millisecond)
					continue
				}
				t0 := time.Now()
				if isRead {
					resp, err := rc.do(proto.Command{Op: proto.OpGet, Key: key}, 5*time.Second)
					if err != nil || resp.Status != proto.StatusOk {
						errs++
						kind("get", resp, err)
						rc.close()
						rc = nil
						continue
					}
					reads = append(reads, time.Since(t0))
					continue
				}
				resp, err := wc.do(proto.Command{Op: proto.OpSet, Key: key, Value: val128}, 10*time.Second)
				switch {
				case err != nil:
					errs++
					kind("set", resp, err)
					wc.close()
					wc = nil
				case resp.Status == proto.StatusOk:
					writes = append(writes, time.Since(t0))
				case resp.Status == proto.StatusNotLeader:
					redirects++
					wc.close()
					wc = nil
					if i := c.byAddr(string(resp.Value)); i >= 0 {
						wIdx = i
					} else {
						time.Sleep(10 * time.Millisecond)
					}
				default:
					errs++
					kind("set", resp, err)
				}
			}
			mu.Lock()
			out.reads = append(out.reads, reads...)
			out.writes = append(out.writes, writes...)
			out.errs += errs
			out.redirects += redirects
			for k, v := range kinds {
				out.errKinds[k] += v
			}
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	out.dur = time.Since(start)
	if out.errs > 0 || out.redirects > 0 {
		c.t.Logf("conns=%d read=%.2f writes=%d errors=%v redirects=%d", conns, readFrac, len(out.writes), out.errKinds, out.redirects)
	}
	return out
}

// pcts formats p50 / p99 / p999 in ms.
func pcts(l []time.Duration) string {
	if len(l) == 0 {
		return "-"
	}
	s := append([]time.Duration(nil), l...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(p float64) time.Duration { return s[int(float64(len(s)-1)*p)] }
	return ms(at(.5)) + " / " + ms(at(.99)) + " / " + ms(at(.999))
}

func ms(d time.Duration) string {
	v := float64(d) / float64(time.Millisecond)
	switch {
	case v >= 100:
		return fmt.Sprintf("%.0f", v)
	case v >= 10:
		return fmt.Sprintf("%.1f", v)
	default:
		return fmt.Sprintf("%.2f", v)
	}
}

func rate(r float64) string { return fmt.Sprintf("%.0f", r) }

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

// writeReport rewrites the generated tables and keeps any hand-written
// section starting at "## Takeaways" from the previous results.md.
func writeReport() {
	reportMu.Lock()
	defer reportMu.Unlock()
	if len(tables) == 0 {
		return
	}
	var b strings.Builder
	b.WriteString("# Performance, multi-node cluster\n\n")
	fmt.Fprintf(&b, "%s · %s/%s, %d CPUs, %s · all nodes on one machine (shared disk) · %s per case · 128 B values · `server.read.from=any`\n\n",
		time.Now().Format("2006-01-02 15:04"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version(), workDur())
	b.WriteString("Closed-loop clients, one request in flight per connection. Latency columns are p50 / p99 / p999 ms, client-side. Reads go to node `g % n`, writes to the leader.\n\n")
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
	if old, err := os.ReadFile(resultFile); err == nil {
		if i := strings.Index(string(old), "## Takeaways"); i >= 0 {
			b.WriteString(string(old[i:]))
		}
	}
	if err := os.WriteFile(resultFile, []byte(b.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		return
	}
	fmt.Fprintf(os.Stderr, "\nresults written to %s\n", resultFile)
}
