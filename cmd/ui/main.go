// Command ui serves the kestrelKV cluster monitor (cmd/ui/web) and runs the
// cluster's nodes as child processes so the page can start, kill and restart them.
//
//	go run ./cmd/ui            then open http://127.0.0.1:8080
//
// Every node is stopped when the UI starts; press START on a card. Each node is
// launched with -ui-addr so the monitor can read its Raft status and run console
// commands over JSON. Real clients (./client) still use the binary protocol.
//
// Event log sources:
//   - node:     start / kill / exit of a child process, plus its stdout/stderr
//     lines when VERBOSE LOGGING is on for that node
//   - election: leader or term changes seen while polling node status
//   - redirect: the console following NotLeader to the leader, or switching
//     to another node when its node goes down
//   - error:    failed starts, crashes, failed console requests
package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed web
var webFS embed.FS

const pollEvery = 500 * time.Millisecond

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP address for the monitor page")
	bin := flag.String("server-bin", "", "prebuilt cmd/server binary (default: build it at startup)")
	flag.Parse()

	root, err := repoRoot()
	if err != nil {
		log.Fatal(err)
	}
	if *bin == "" {
		*bin, err = buildServer(root)
		if err != nil {
			log.Fatal(err)
		}
	}

	m := newManager(*bin, root)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go m.pollLoop(ctx)

	static, _ := fs.Sub(webFS, "web")
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /events", m.handleEvents)
	mux.HandleFunc("POST /api/kv", m.handleKV)
	mux.HandleFunc("POST /api/nodes", m.handleAddNode)
	mux.HandleFunc("PUT /api/nodes/{id}/config", m.handleUpdateNode)
	mux.HandleFunc("DELETE /api/nodes/{id}", m.handleRemoveNode)
	mux.HandleFunc("POST /api/nodes/{id}/{act}", m.handleNodeAction)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	hs := &http.Server{Handler: mux}
	go func() { _ = hs.Serve(ln) }()
	fmt.Printf("kestrelKV monitor on http://%s (nodes are stopped; press START)\n", ln.Addr())

	<-ctx.Done()
	fmt.Println("\nstopping nodes…")
	m.killAll()
	_ = hs.Close()
}

// repoRoot walks up from the working directory to the folder holding settings.properties.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "settings.properties")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("run cmd/ui from inside the kvStore repo (settings.properties not found)")
		}
		dir = parent
	}
}

func buildServer(root string) (string, error) {
	out := filepath.Join(os.TempDir(), "kestrelkv-ui-node")
	fmt.Println("building cmd/server …")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/server")
	cmd.Dir = root
	if b, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build cmd/server: %v\n%s", err, b)
	}
	return out, nil
}

// ---------- node config and process state ----------

type nodeConfig struct {
	ID         string `json:"id"`
	ClientAddr string `json:"clientAddr"`
	ReplAddr   string `json:"replAddr"`
	Wal        string `json:"wal"`
	PprofAddr  string `json:"pprofAddr"`
	Verbose    bool   `json:"verbose"`
}

// uiAddr is where the node's monitor HTTP listener binds: client port + 5.
func (c nodeConfig) uiAddr() string {
	host, port, _ := net.SplitHostPort(c.ClientAddr)
	p, _ := strconv.Atoi(port)
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, strconv.Itoa(p+5))
}

type nodeStatus struct {
	ID     string `json:"id"`
	Role   string `json:"role"`
	Term   uint64 `json:"term"`
	Leader string `json:"leader"`
}

type node struct {
	cfg nodeConfig

	cmd     *exec.Cmd
	done    chan struct{} // closed when cmd exits
	killing bool          // exit was requested, don't report it as a crash
	ready   bool          // answered its first status; a failed start is reported by start()
	out     *lineTail     // last lines of stdout+stderr

	reachable bool
	status    nodeStatus
}

func (n *node) running() bool {
	if n.cmd == nil {
		return false
	}
	select {
	case <-n.done:
		return false
	default:
		return true
	}
}

// ---------- manager ----------

type logEvent struct {
	TS   int64  `json:"ts"`
	Kind string `json:"kind"`
	Msg  string `json:"msg"`
}

type manager struct {
	bin     string
	workDir string // child cwd: relative WAL paths resolve from here
	env     []string

	mu        sync.Mutex
	nodes     []*node
	connected string
	opMu      sync.Mutex // serializes start/kill/config changes

	lastLeader string
	lastTerm   uint64

	hubMu   sync.Mutex
	subs    map[chan []byte]struct{}
	recent  [][]byte // replayed to new subscribers
	lastMsg []byte   // last state message
}

func newManager(bin, root string) *manager {
	m := &manager{
		bin:     bin,
		workDir: filepath.Join(root, "cmd", "server"),
		env:     append(os.Environ(), "KVSTORE_SETTINGS="+filepath.Join(root, "settings.properties")),
		subs:    map[chan []byte]struct{}{},
	}
	// Same layout as the Readme.
	for _, d := range []struct{ id, client, repl string }{
		{"node-a", "127.0.0.1:9000", "127.0.0.1:9001"},
		{"node-b", "127.0.0.1:9010", "127.0.0.1:9002"},
		{"node-c", "127.0.0.1:9020", "127.0.0.1:9003"},
	} {
		m.nodes = append(m.nodes, &node{cfg: nodeConfig{
			ID: d.id, ClientAddr: d.client, ReplAddr: d.repl, Wal: "data/" + d.id + ".wal",
		}})
	}
	m.connected = m.nodes[0].cfg.ID
	return m
}

func (m *manager) find(id string) *node {
	for _, n := range m.nodes {
		if n.cfg.ID == id {
			return n
		}
	}
	return nil
}

func (m *manager) emit(kind, format string, args ...any) {
	b, _ := json.Marshal(logEvent{TS: time.Now().UnixMilli(), Kind: kind, Msg: fmt.Sprintf(format, args...)})
	msg := []byte("event: log\ndata: " + string(b) + "\n\n")
	m.hubMu.Lock()
	m.recent = append(m.recent, msg)
	if len(m.recent) > 200 {
		m.recent = m.recent[len(m.recent)-200:]
	}
	m.broadcastLocked(msg)
	m.hubMu.Unlock()
}

func (m *manager) broadcastLocked(msg []byte) {
	for ch := range m.subs {
		select {
		case ch <- msg:
		default: // slow subscriber: drop
		}
	}
}

// ---------- start / kill ----------

func (m *manager) start(id string) error {
	m.mu.Lock()
	n := m.find(id)
	if n == nil {
		m.mu.Unlock()
		return fmt.Errorf("no node %s", id)
	}
	if n.running() {
		m.mu.Unlock()
		return fmt.Errorf("%s is already running", id)
	}
	cfg := n.cfg
	var peers, clientPeers []string
	for _, o := range m.nodes {
		if o != n {
			peers = append(peers, o.cfg.ID+"="+o.cfg.ReplAddr)
			clientPeers = append(clientPeers, o.cfg.ID+"="+o.cfg.ClientAddr)
		}
	}
	m.mu.Unlock()

	// If something already answers on this node's UI port (a node started by
	// hand or by another monitor), the readiness check below would be fooled.
	if st, err := fetchStatus(cfg.uiAddr()); err == nil {
		return fmt.Errorf("%s is already served by another process (node %q answers on %s) — stop it first", id, st.ID, cfg.uiAddr())
	}
	if err := os.MkdirAll(filepath.Join(m.workDir, filepath.Dir(cfg.Wal)), 0o755); err != nil {
		return err
	}
	args := []string{
		"-node-id=" + cfg.ID,
		"-client-addr=" + cfg.ClientAddr,
		"-replication-addr=" + cfg.ReplAddr,
		"-ui-addr=" + cfg.uiAddr(),
		"-wal=" + cfg.Wal,
		"-peers=" + strings.Join(peers, ","),
		"-client-peers=" + strings.Join(clientPeers, ","),
	}
	if cfg.PprofAddr != "" {
		args = append(args, "-pprof-addr="+cfg.PprofAddr)
	}
	if cfg.Verbose {
		args = append(args, "-verbose")
	}

	tail := &lineTail{max: 30}
	if cfg.Verbose {
		tail.onLine = func(line string) { m.emit("node", "[%s] %s", id, line) }
	}
	cmd := exec.Command(m.bin, args...)
	cmd.Dir = m.workDir
	cmd.Env = m.env
	cmd.Stdout = tail
	cmd.Stderr = tail
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("launch: %w", err)
	}
	done := make(chan struct{})

	m.mu.Lock()
	n.cmd, n.done, n.out, n.killing, n.reachable = cmd, done, tail, false, false
	m.mu.Unlock()

	go func() {
		err := cmd.Wait()
		close(done)
		m.mu.Lock()
		crashed := n.ready && !n.killing
		n.reachable, n.ready = false, false
		m.mu.Unlock()
		if crashed {
			m.emit("error", "%s exited: %s", id, exitReason(err, tail))
		}
		m.pushState()
	}()

	// Wait until the node answers on its UI listener, or dies trying.
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return fmt.Errorf("%s", exitReason(cmd.ProcessState.ExitCode(), tail))
		case <-deadline:
			m.kill(id)
			return fmt.Errorf("did not answer on %s within 5s", cfg.uiAddr())
		case <-tick.C:
			if st, err := fetchStatus(cfg.uiAddr()); err == nil {
				m.mu.Lock()
				n.reachable, n.ready, n.status = true, true, st
				m.mu.Unlock()
				m.emit("node", "%s started (client %s, replication %s)", id, cfg.ClientAddr, cfg.ReplAddr)
				m.pushState()
				return nil
			}
		}
	}
}

// exitReason picks the most useful line a dead node printed.
func exitReason(err any, tail *lineTail) string {
	lines := tail.lines()
	for _, l := range lines {
		if strings.HasPrefix(l, "panic:") {
			return l
		}
	}
	for i := len(lines) - 1; i >= 0; i-- {
		if s := strings.TrimSpace(lines[i]); s != "" {
			return s
		}
	}
	switch e := err.(type) {
	case int:
		return fmt.Sprintf("exit status %d", e)
	case error:
		return e.Error()
	}
	return "exited"
}

func (m *manager) kill(id string) error {
	m.mu.Lock()
	n := m.find(id)
	if n == nil {
		m.mu.Unlock()
		return fmt.Errorf("no node %s", id)
	}
	if !n.running() {
		m.mu.Unlock()
		return fmt.Errorf("%s is not running", id)
	}
	n.killing = true
	wasLeader := n.reachable && n.status.Role == "leader"
	cmd, done := n.cmd, n.done
	m.mu.Unlock()

	_ = cmd.Process.Kill() // SIGKILL: behaves like a crash
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		return fmt.Errorf("%s did not exit after kill", id)
	}
	if wasLeader {
		m.emit("node", "%s killed (was leader)", id)
	} else {
		m.emit("node", "%s killed", id)
	}
	m.pushState()
	return nil
}

func (m *manager) killAll() {
	m.mu.Lock()
	var ids []string
	for _, n := range m.nodes {
		if n.running() {
			ids = append(ids, n.cfg.ID)
		}
	}
	m.mu.Unlock()
	for _, id := range ids {
		_ = m.kill(id)
	}
}

// ---------- polling, state, election events ----------

func fetchStatus(addr string) (nodeStatus, error) {
	c := http.Client{Timeout: 400 * time.Millisecond}
	resp, err := c.Get("http://" + addr + "/ui/status")
	if err != nil {
		return nodeStatus{}, err
	}
	defer resp.Body.Close()
	var st nodeStatus
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

func (m *manager) pollLoop(ctx context.Context) {
	t := time.NewTicker(pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		m.mu.Lock()
		type target struct {
			n    *node
			addr string
		}
		var targets []target
		for _, n := range m.nodes {
			if n.running() {
				targets = append(targets, target{n, n.cfg.uiAddr()})
			}
		}
		m.mu.Unlock()

		results := make([]nodeStatus, len(targets))
		errs := make([]error, len(targets))
		var wg sync.WaitGroup
		for i, tg := range targets {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i], errs[i] = fetchStatus(tg.addr)
			}()
		}
		wg.Wait()

		m.mu.Lock()
		for i, tg := range targets {
			if !tg.n.running() {
				continue
			}
			tg.n.reachable = errs[i] == nil
			if errs[i] == nil {
				tg.n.status = results[i]
			}
		}
		m.detectElectionLocked()
		m.mu.Unlock()
		m.pushState()
	}
}

// detectElectionLocked logs leader changes. The leader is the reachable node
// that says it leads at the highest term. Caller holds m.mu.
func (m *manager) detectElectionLocked() {
	var leader string
	var term uint64
	for _, n := range m.nodes {
		if !n.reachable {
			continue
		}
		if n.status.Term > term {
			term = n.status.Term
		}
	}
	for _, n := range m.nodes {
		if n.reachable && n.status.Role == "leader" && n.status.Term == term {
			leader = n.cfg.ID
		}
	}
	switch {
	case leader != "" && (leader != m.lastLeader || term != m.lastTerm):
		m.emit("election", "%s elected leader for term %d", leader, term)
	case leader == "" && m.lastLeader != "":
		m.emit("election", "no leader (was %s, term %d) — election in progress", m.lastLeader, m.lastTerm)
	}
	if leader != "" || m.lastLeader != "" {
		m.lastLeader, m.lastTerm = leader, term
	}
}

type stateNode struct {
	ID     string     `json:"id"`
	Addr   string     `json:"addr"`
	Up     bool       `json:"up"`
	Proc   bool       `json:"proc"` // process is running (may still be unreachable)
	Role   string     `json:"role"`
	Term   uint64     `json:"term"`
	Leader string     `json:"leader"`
	Cfg    nodeConfig `json:"cfg"`
}

func (m *manager) stateJSON() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := struct {
		Connected string      `json:"connected"`
		Nodes     []stateNode `json:"nodes"`
	}{Connected: m.connected}
	for _, n := range m.nodes {
		sn := stateNode{ID: n.cfg.ID, Addr: n.cfg.ClientAddr, Proc: n.running(), Role: "down", Cfg: n.cfg}
		if sn.Proc && n.reachable {
			sn.Up, sn.Role, sn.Term, sn.Leader = true, n.status.Role, n.status.Term, n.status.Leader
		}
		out.Nodes = append(out.Nodes, sn)
	}
	b, _ := json.Marshal(out)
	return b
}

func (m *manager) pushState() {
	msg := []byte("event: state\ndata: " + string(m.stateJSON()) + "\n\n")
	m.hubMu.Lock()
	defer m.hubMu.Unlock()
	if bytes.Equal(msg, m.lastMsg) {
		return
	}
	m.lastMsg = msg
	m.broadcastLocked(msg)
}

func (m *manager) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	ch := make(chan []byte, 256)
	m.hubMu.Lock()
	for _, msg := range m.recent {
		_, _ = w.Write(msg)
	}
	m.subs[ch] = struct{}{}
	m.hubMu.Unlock()
	defer func() {
		m.hubMu.Lock()
		delete(m.subs, ch)
		m.hubMu.Unlock()
	}()
	_, _ = w.Write([]byte("event: state\ndata: " + string(m.stateJSON()) + "\n\n"))
	fl.Flush()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			_, _ = w.Write(msg)
			fl.Flush()
		case <-ping.C:
			_, _ = w.Write([]byte(": ping\n\n"))
			fl.Flush()
		}
	}
}

// ---------- console ----------

type kvRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type kvResult struct {
	Status string `json:"status"`
	Value  string `json:"value,omitempty"`
	Node   string `json:"node,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (m *manager) handleKV(w http.ResponseWriter, r *http.Request) {
	var req kvRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, kvResult{Status: "error", Error: "bad request"})
		return
	}
	writeJSON(w, m.kv(req))
}

// kv sends the command to the console's node, following NotLeader like the CLI client.
func (m *manager) kv(req kvRequest) kvResult {
	body, _ := json.Marshal(req)
	c := http.Client{Timeout: 8 * time.Second}
	for hop := 0; hop < 4; hop++ {
		m.mu.Lock()
		target := m.find(m.connected)
		if target == nil || !target.reachable {
			old := m.connected
			target = nil
			for _, n := range m.nodes {
				if n.reachable {
					target = n
					break
				}
			}
			if target == nil {
				m.mu.Unlock()
				return kvResult{Status: "error", Error: "no node is running — press START on a node first"}
			}
			m.connected = target.cfg.ID
			m.emit("redirect", "console: %s is down, switched to %s", old, target.cfg.ID)
		}
		id, addr := target.cfg.ID, target.cfg.uiAddr()
		m.mu.Unlock()
		m.pushState()

		resp, err := c.Post("http://"+addr+"/ui/kv", "application/json", bytes.NewReader(body))
		if err != nil {
			return kvResult{Status: "error", Node: id, Error: "request failed: " + err.Error()}
		}
		var out struct{ Status, Value, Leader string }
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if err != nil {
			return kvResult{Status: "error", Node: id, Error: "bad reply: " + err.Error()}
		}
		switch out.Status {
		case "ok", "not_found":
			return kvResult{Status: out.Status, Value: out.Value, Node: id}
		case "not_leader":
			if out.Leader == "" {
				return kvResult{Status: "error", Node: id, Error: "no leader elected yet"}
			}
			m.mu.Lock()
			next := ""
			for _, n := range m.nodes {
				if n.cfg.ID == out.Leader || n.cfg.ClientAddr == out.Leader {
					next = n.cfg.ID
				}
			}
			if next == "" || next == id {
				m.mu.Unlock()
				return kvResult{Status: "error", Node: id, Error: "leader " + out.Leader + " is unknown to the monitor"}
			}
			m.connected = next
			m.mu.Unlock()
			m.emit("redirect", "%s → NotLeader, following leader %s", id, next)
		default:
			return kvResult{Status: "error", Node: id, Error: out.Value}
		}
	}
	return kvResult{Status: "error", Error: "too many redirects"}
}

// ---------- node actions and config ----------

func (m *manager) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	id, act := r.PathValue("id"), r.PathValue("act")
	m.opMu.Lock()
	defer m.opMu.Unlock()
	var err error
	switch act {
	case "start":
		err = m.start(id)
	case "kill":
		err = m.kill(id)
	case "restart":
		m.mu.Lock()
		n := m.find(id)
		running := n != nil && n.running()
		m.mu.Unlock()
		if running {
			err = m.kill(id)
		}
		if err == nil {
			err = m.start(id)
		}
	default:
		err = fmt.Errorf("unknown action %q", act)
	}
	reply(w, err)
}

var (
	idRE   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,16}$`)
	addrRE = regexp.MustCompile(`^[A-Za-z0-9.\-]*:\d{1,5}$`)
)

func (m *manager) validate(cfg nodeConfig, self *node) error {
	if !idRE.MatchString(cfg.ID) {
		return errors.New("node id: use 1–16 letters, digits, - or _")
	}
	if !addrRE.MatchString(cfg.ClientAddr) || !addrRE.MatchString(cfg.ReplAddr) {
		return errors.New("addresses must look like host:port")
	}
	if cfg.PprofAddr != "" && !addrRE.MatchString(cfg.PprofAddr) {
		return errors.New("pprof address must look like host:port")
	}
	if cfg.Wal == "" {
		return errors.New("WAL path is required")
	}
	mine := []string{cfg.ClientAddr, cfg.ReplAddr, cfg.uiAddr(), cfg.PprofAddr}
	seen := map[string]string{}
	for _, n := range m.nodes {
		if n == self {
			continue
		}
		if n.cfg.ID == cfg.ID {
			return fmt.Errorf("%s already exists", cfg.ID)
		}
		for _, a := range []string{n.cfg.ClientAddr, n.cfg.ReplAddr, n.cfg.uiAddr(), n.cfg.PprofAddr} {
			if a != "" {
				seen[portOf(a)] = n.cfg.ID
			}
		}
	}
	local := map[string]bool{}
	for _, a := range mine {
		if a == "" {
			continue
		}
		p := portOf(a)
		if owner, ok := seen[p]; ok {
			return fmt.Errorf("port %s is already used by %s (the monitor also uses client port + 5)", p, owner)
		}
		if local[p] {
			return fmt.Errorf("port %s is used twice (the monitor also uses client port + 5)", p)
		}
		local[p] = true
	}
	return nil
}

func portOf(addr string) string {
	_, p, _ := net.SplitHostPort(addr)
	return p
}

func (m *manager) anyRunningLocked(except *node) bool {
	for _, n := range m.nodes {
		if n != except && n.running() {
			return true
		}
	}
	return false
}

func (m *manager) handleAddNode(w http.ResponseWriter, r *http.Request) {
	var cfg nodeConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		reply(w, errors.New("bad request"))
		return
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.Lock()
	if m.anyRunningLocked(nil) {
		m.mu.Unlock()
		reply(w, errors.New("stop every node first — peers are fixed when a node starts"))
		return
	}
	if err := m.validate(cfg, nil); err != nil {
		m.mu.Unlock()
		reply(w, err)
		return
	}
	m.nodes = append(m.nodes, &node{cfg: cfg})
	m.mu.Unlock()
	m.emit("node", "%s added (client %s, replication %s) — stopped", cfg.ID, cfg.ClientAddr, cfg.ReplAddr)
	m.pushState()
	reply(w, nil)
}

func (m *manager) handleUpdateNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var cfg nodeConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		reply(w, errors.New("bad request"))
		return
	}
	cfg.ID = id
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.Lock()
	n := m.find(id)
	if n == nil {
		m.mu.Unlock()
		reply(w, fmt.Errorf("%s no longer exists", id))
		return
	}
	addrChanged := cfg.ClientAddr != n.cfg.ClientAddr || cfg.ReplAddr != n.cfg.ReplAddr
	if addrChanged && m.anyRunningLocked(n) {
		m.mu.Unlock()
		reply(w, errors.New("stop the other nodes before changing addresses — they learn peer addresses when they start"))
		return
	}
	if err := m.validate(cfg, n); err != nil {
		m.mu.Unlock()
		reply(w, err)
		return
	}
	n.cfg = cfg
	wasRunning := n.running()
	m.mu.Unlock()
	m.emit("node", "%s settings saved", id)
	m.pushState()

	var err error
	if wasRunning {
		if err = m.kill(id); err == nil {
			err = m.start(id)
		}
	}
	reply(w, err)
}

func (m *manager) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.Lock()
	if m.anyRunningLocked(nil) {
		m.mu.Unlock()
		reply(w, errors.New("stop every node first — peers are fixed when a node starts"))
		return
	}
	if len(m.nodes) <= 1 {
		m.mu.Unlock()
		reply(w, errors.New("the cluster needs at least one node"))
		return
	}
	i := -1
	for j, n := range m.nodes {
		if n.cfg.ID == id {
			i = j
		}
	}
	if i < 0 {
		m.mu.Unlock()
		reply(w, fmt.Errorf("%s no longer exists", id))
		return
	}
	m.nodes = append(m.nodes[:i], m.nodes[i+1:]...)
	if m.connected == id {
		m.connected = m.nodes[0].cfg.ID
	}
	m.mu.Unlock()
	m.emit("node", "%s removed from the cluster (its WAL files are kept)", id)
	m.pushState()
	reply(w, nil)
}

func reply(w http.ResponseWriter, err error) {
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// ---------- lineTail: keeps the last lines a child printed ----------

type lineTail struct {
	mu     sync.Mutex
	max    int
	buf    []string
	part   []byte
	onLine func(string)
}

func (t *lineTail) Write(p []byte) (int, error) {
	t.mu.Lock()
	t.part = append(t.part, p...)
	var done []string
	for {
		i := bytes.IndexByte(t.part, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(string(t.part[:i]), "\r")
		t.part = t.part[i+1:]
		t.buf = append(t.buf, line)
		if len(t.buf) > t.max {
			t.buf = t.buf[1:]
		}
		done = append(done, line)
	}
	cb := t.onLine
	t.mu.Unlock()
	if cb != nil {
		for _, l := range done {
			if strings.TrimSpace(l) != "" {
				cb(l)
			}
		}
	}
	return len(p), nil
}

func (t *lineTail) lines() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := append([]string(nil), t.buf...)
	if len(t.part) > 0 {
		out = append(out, string(t.part))
	}
	return out
}
