// Package single holds black-box benchmarks for ONE kvStore node (no peers).
//
// The tests build cmd/server, run it as a real subprocess with its own WAL
// directory, and talk to it over TCP with the project's wire protocol, so the
// numbers include the whole path: TCP -> Raft propose (fsync'd WAL append) ->
// commit -> apply -> response.
//
// They are slow, so they only run when KV_BENCH=1. Each run appends to
// results.md with a summary table plus one table per test.
package single

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"kvStore/proto"
	"kvStore/raft"
	"kvStore/wal"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const resultFile = "results.md"

var (
	serverBin string
	repoRoot  string
)

func TestMain(m *testing.M) {
	if os.Getenv("KV_BENCH") != "1" {
		// Not a benchmark run: let the tests skip themselves, build nothing.
		os.Exit(m.Run())
	}

	wd, _ := os.Getwd()
	repoRoot = filepath.Clean(filepath.Join(wd, "..", ".."))
	tmp, err := os.MkdirTemp("", "kvbench-bin-")
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
		t.Skip("set KV_BENCH=1 to run the single-node benchmarks")
	}
}

// ---------------------------------------------------------------- env knobs

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInts(key string, def []int) []int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []int
	for _, p := range strings.Split(v, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// benchDuration is the measured window of each throughput case.
func benchDuration() time.Duration { return envDuration("KV_BENCH_DUR", 3*time.Second) }

// ---------------------------------------------------------------- server process

type serverProc struct {
	cmd        *exec.Cmd
	exited     chan struct{} // closed when the process exits
	stderr     *bytes.Buffer
	addr       string
	walPath    string
	dir        string
	startedAt  time.Time
	ListenTime time.Duration // process start -> client port accepts (WAL replay is done by then)
	ReadyTime  time.Duration // process start -> first write acked (leader elected, log applied)
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

// startServer launches a single node (no -peers) on dir and waits until it
// accepts writes. A write is only acked after the node is leader AND has
// applied every earlier log entry, so ReadyTime is "fully serving again".
func startServer(t testing.TB, dir string) *serverProc {
	t.Helper()
	p := &serverProc{
		addr:    freeAddr(t),
		walPath: filepath.Join(dir, "n1.wal"),
		dir:     dir,
	}
	p.cmd = exec.Command(serverBin,
		"-node-id", "n1",
		"-client-addr", p.addr,
		"-replication-addr", freeAddr(t),
		"-wal", p.walPath,
	)
	p.cmd.Dir = repoRoot // so config finds settings.properties
	p.stderr = &bytes.Buffer{}
	p.cmd.Stderr = &lockedWriter{w: p.stderr}
	p.startedAt = time.Now()
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p.exited = make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(p.exited) }()
	dead := func() bool {
		select {
		case <-p.exited:
			return true
		default:
			return false
		}
	}

	deadline := time.Now().Add(10 * time.Minute)
	var conn net.Conn
	for {
		c, err := net.DialTimeout("tcp", p.addr, time.Second)
		if err == nil {
			conn = c
			break
		}
		if dead() {
			t.Fatalf("server exited before listening:\n%s", p.stderr.String())
		}
		if time.Now().After(deadline) {
			p.kill()
			t.Fatalf("server never started listening: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	p.ListenTime = time.Since(p.startedAt)

	cl := newClient(conn)
	defer cl.close()
	for {
		resp, err := cl.do(proto.Command{Op: proto.OpSet, Key: []byte("__ready__"), Value: []byte("1")})
		if err == nil && resp.Status == proto.StatusOk {
			break
		}
		if err != nil || dead() || time.Now().After(deadline) {
			p.kill()
			t.Fatalf("server never became ready: status=%v err=%v\n%s", resp.Status, err, p.stderr.String())
		}
		time.Sleep(time.Millisecond)
	}
	p.ReadyTime = time.Since(p.startedAt)
	return p
}

// stop sends SIGTERM and waits for a clean exit.
func (p *serverProc) stop() {
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
	case <-time.After(60 * time.Second):
		p.kill()
	}
}

func (p *serverProc) kill() {
	_ = p.cmd.Process.Kill()
	<-p.exited
}

// lockedWriter lets the exec copy goroutine and the test read stderr safely.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

// rssMB is the resident memory of the server process.
func (p *serverProc) rssMB() float64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(p.cmd.Process.Pid)).Output()
	if err != nil {
		return 0
	}
	kb, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	return kb / 1024
}

func (p *serverProc) walBytes() int64 {
	st, err := os.Stat(p.walPath + "-raft.log")
	if err != nil {
		return 0
	}
	return st.Size()
}

// ---------------------------------------------------------------- client

// client is one synchronous connection: one request in flight at a time.
type client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func newClient(conn net.Conn) *client {
	return &client{conn: conn, r: bufio.NewReaderSize(conn, 64<<10), w: bufio.NewWriterSize(conn, 64<<10)}
}

func dialClient(addr string) (*client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return newClient(conn), nil
}

func (c *client) do(cmd proto.Command) (proto.Response, error) {
	if err := proto.EncodeCommand(c.w, cmd); err != nil {
		return proto.Response{}, err
	}
	if err := c.w.Flush(); err != nil {
		return proto.Response{}, err
	}
	return proto.DecodeResponse(c.r)
}

func (c *client) close() { _ = c.conn.Close() }

// ---------------------------------------------------------------- WAL seeding

// seedWAL writes a raft log file directly (same on-disk format the node
// replays), so big logs are built in seconds instead of one fsync per write.
//
// The log is a realistic *churned* history, not an ever-growing key set:
// the first `keys` records create the keyspace, and every record after that
// overwrites (90%) or deletes (10%) a random existing key, with deleted keys
// re-created later. So a log with 1M records usually holds only `keys` live
// entries; this is exactly the case where the WAL is much bigger than the DB.
func seedWAL(dir string, records, keys, valSize int) (liveKeys int, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	f, err := os.Create(filepath.Join(dir, "n1.wal-raft.log"))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	// Header is wal.walHeader ("KVWL" + version 2); kept private there.
	if _, err := w.Write([]byte{'K', 'V', 'W', 'L', 2}); err != nil {
		return 0, err
	}
	if keys > records {
		keys = records
	}
	rng := rand.New(rand.NewSource(1))
	val := make([]byte, valSize)
	rng.Read(val)
	live := make([]bool, keys)
	var key []byte
	for i := 1; i <= records; i++ {
		var k int
		op := byte(1) // wal.OpSet
		if i <= keys {
			k = i - 1
		} else {
			k = rng.Intn(keys)
			if live[k] && rng.Intn(10) == 0 {
				op = 2 // wal.OpDel
			}
		}
		live[k] = op == 1
		key = appendKey(key[:0], k)
		binaryPut(val, uint64(i))
		rec := wal.Record{Index: uint64(i), Term: 1, Op: op, Key: key}
		if op == 1 {
			rec.Value = val
		}
		if err := wal.EncodeRecord(w, rec); err != nil {
			return 0, err
		}
	}
	if err := w.Flush(); err != nil {
		return 0, err
	}
	// The node refuses a log whose last term is ahead of the persisted term.
	state, err := raft.EncodePersistentState(raft.PersistentState{CurrentTerm: 1})
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(filepath.Join(dir, "n1.wal-raft.state"), state, 0o600); err != nil {
		return 0, err
	}
	for _, l := range live {
		if l {
			liveKeys++
		}
	}
	return liveKeys, f.Sync()
}

func binaryPut(b []byte, v uint64) {
	for i := 0; i < 8 && i < len(b); i++ {
		b[i] = byte(v >> (8 * i))
	}
}

func appendKey(dst []byte, k int) []byte {
	dst = append(dst, "key-"...)
	s := strconv.Itoa(k)
	for i := len(s); i < 8; i++ {
		dst = append(dst, '0')
	}
	return append(dst, s...)
}

// ---------------------------------------------------------------- workload

type mix struct {
	readPct int // % GET
	delPct  int // % DEL (rest are SET)
}

type opStats struct {
	count int
	lats  []time.Duration
}

type workloadResult struct {
	elapsed time.Duration
	reads   opStats
	writes  opStats // SET + DEL
	hits    int     // GETs that found the key
	valueB  int64   // value bytes written
	errs    int
	lastErr string
}

func (r workloadResult) total() int { return r.reads.count + r.writes.count }

// runWorkload drives `conns` synchronous clients for `dur` against a keyspace
// of `keys` keys. Keys are skewed (80% of ops hit the hottest 20% of keys) and
// every write overwrites or deletes-then-recreates keys inside that fixed
// keyspace, so the DB size stays flat instead of growing forever.
func runWorkload(t testing.TB, addr string, conns int, dur time.Duration, keys, valSize int, m mix) workloadResult {
	t.Helper()
	results := make([]workloadResult, conns)
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(dur)
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res := &results[i]
			cl, err := dialClient(addr)
			if err != nil {
				res.errs++
				res.lastErr = err.Error()
				return
			}
			defer cl.close()
			rng := rand.New(rand.NewSource(int64(1000 + i)))
			val := make([]byte, valSize)
			rng.Read(val)
			hot := keys / 5
			if hot < 1 {
				hot = 1
			}
			var key []byte
			for time.Now().Before(deadline) {
				var k int
				if rng.Intn(10) < 8 {
					k = rng.Intn(hot)
				} else {
					k = rng.Intn(keys)
				}
				key = appendKey(key[:0], k)
				roll := rng.Intn(100)
				var cmd proto.Command
				isRead := false
				switch {
				case roll < m.readPct:
					cmd = proto.Command{Op: proto.OpGet, Key: key}
					isRead = true
				case roll < m.readPct+m.delPct:
					cmd = proto.Command{Op: proto.OpDel, Key: key}
				default:
					binaryPut(val, uint64(res.writes.count))
					cmd = proto.Command{Op: proto.OpSet, Key: key, Value: val}
				}
				t0 := time.Now()
				resp, err := cl.do(cmd)
				lat := time.Since(t0)
				if err != nil {
					res.errs++
					res.lastErr = err.Error()
					return
				}
				if resp.Status == proto.StatusError || resp.Status == proto.StatusNotLeader {
					res.errs++
					res.lastErr = fmt.Sprintf("status %d: %s", resp.Status, resp.Value)
					continue
				}
				if isRead {
					res.reads.count++
					res.reads.lats = append(res.reads.lats, lat)
					if resp.Status == proto.StatusOk {
						res.hits++
					}
				} else {
					res.writes.count++
					res.writes.lats = append(res.writes.lats, lat)
					if cmd.Op == proto.OpSet {
						res.valueB += int64(len(val))
					}
				}
			}
		}(i)
	}
	wg.Wait()

	var out workloadResult
	out.elapsed = time.Since(start)
	for _, r := range results {
		out.reads.count += r.reads.count
		out.reads.lats = append(out.reads.lats, r.reads.lats...)
		out.writes.count += r.writes.count
		out.writes.lats = append(out.writes.lats, r.writes.lats...)
		out.hits += r.hits
		out.valueB += r.valueB
		out.errs += r.errs
		if r.lastErr != "" {
			out.lastErr = r.lastErr
		}
	}
	return out
}

func pct(lats []time.Duration, p float64) time.Duration {
	if len(lats) == 0 {
		return 0
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	idx := int(float64(len(lats)-1) * p)
	return lats[idx]
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

func humanBytes(n int) string {
	switch {
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n >= 1<<10 && n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// ---------------------------------------------------------------- report

type table struct {
	title  string
	note   string
	header []string
	rows   [][]string
}

type headline struct {
	order         int
	metric, value string
}

var (
	reportMu  sync.Mutex
	tables    []*table
	byTitle   = map[string]*table{}
	headlines []headline
)

// record adds a row to a named table; tables are written to results.md at the end.
func record(title, note string, header []string, row []string) {
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

// summarize adds one headline number to the summary table (sorted by order).
func summarize(order int, metric, value string) {
	reportMu.Lock()
	defer reportMu.Unlock()
	headlines = append(headlines, headline{order, metric, value})
}

func writeTable(b *strings.Builder, header []string, rows [][]string) {
	b.WriteString("| " + strings.Join(header, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat("---|", len(header)) + "\n")
	for _, r := range rows {
		b.WriteString("| " + strings.Join(r, " | ") + " |\n")
	}
	b.WriteString("\n")
}

// writeReport appends this run to results.md (separated by a divider line)
// and prints the summary to stdout.
func writeReport() {
	reportMu.Lock()
	defer reportMu.Unlock()
	if len(tables) == 0 {
		return
	}
	var b strings.Builder
	b.WriteString("# Single-node benchmark\n\n")
	fmt.Fprintf(&b, "%s · %s/%s, %d CPUs, %s · %s per throughput case\n\n",
		time.Now().Format("2006-01-02 15:04"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(),
		runtime.Version(), benchDuration())
	b.WriteString("One `cmd/server` node over TCP; every write is an fsync'd Raft-log append. Latencies are client-side.\n\n")

	if len(headlines) > 0 {
		sort.SliceStable(headlines, func(i, j int) bool { return headlines[i].order < headlines[j].order })
		rows := make([][]string, len(headlines))
		for i, h := range headlines {
			rows[i] = []string{h.metric, h.value}
		}
		b.WriteString("## Summary\n\n")
		writeTable(&b, []string{"metric", "result"}, rows)
		fmt.Println()
		for _, r := range rows {
			fmt.Printf("  %-40s %s\n", r[0], r[1])
		}
	}
	for _, tb := range tables {
		fmt.Fprintf(&b, "## %s\n\n", tb.title)
		if tb.note != "" {
			b.WriteString(tb.note + "\n\n")
		}
		writeTable(&b, tb.header, tb.rows)
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
