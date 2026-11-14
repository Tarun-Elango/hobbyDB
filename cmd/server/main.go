package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"kvStore/logx"
	"kvStore/proto"
	"kvStore/raft"
	"kvStore/replication"
	"kvStore/server"
	"kvStore/store"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// imports the server package,
// wires up its dependencies (creates a store.Store,
// creates a server.Server, decides on :9000),
// and calls .Serve(). It's glue, not logic.

type options struct {
	nodeID          string
	clientAddr      string
	replicationAddr string
	pprofAddr       string
	walPath         string
	peers           string
	clientPeers     string
	verbose         bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var opts options

	// cmd options, user input will be assigned to theses

	flag.StringVar(&opts.nodeID, "node-id", "", "unique node ID")
	flag.StringVar(&opts.clientAddr, "client-addr", ":9000", "client listen address")
	flag.StringVar(&opts.replicationAddr, "replication-addr", ":9001", "peer listen address")
	flag.StringVar(&opts.pprofAddr, "pprof-addr", "", "optional pprof HTTP listen address")
	flag.StringVar(&opts.walPath, "wal", "", "WAL file path")
	flag.StringVar(&opts.peers, "peers", "", "comma separated id=host:port pairs")
	flag.StringVar(&opts.clientPeers, "client-peers", "", "comma separated id=clientHost:port pairs, used to redirect clients to the leader")
	flag.BoolVar(&opts.verbose, "verbose", false, "enable server logging output")
	flag.Parse() // parse the cmd line arguments

	if opts.nodeID == "" {
		return fmt.Errorf("-node-id is required")
	}

	if opts.walPath == "" {
		opts.walPath = "data/" + opts.nodeID + ".wal"
	}
	logx.SetEnabled(opts.verbose)
	ctx, stop := signal.NotifyContext( // get root context, signal watches for os signals
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Expose Go's pprof endpoints on localhost, separately from the TCP client
	// listener. Binding the listener here enables profiling for either role.
	if opts.pprofAddr != "" {
		pprofListener, err := net.Listen("tcp", opts.pprofAddr)
		if err != nil {
			return fmt.Errorf("start pprof listener on %s: %w", opts.pprofAddr, err)
		}
		pprofServer := &http.Server{}
		defer pprofServer.Close()

		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := pprofServer.Shutdown(shutdownCtx); err != nil {
				_ = pprofServer.Close()
			}
		}()
		go func() {
			if err := pprofServer.Serve(pprofListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logx.Errorf("pprof server: %v\n", err)
			}
		}()
	}

	logx.Printf("INFO: starting this as a follower Node\n")
	return runNode(ctx, opts)

}

// runNode boots one cluster member as a follower:
// - NewStore: empty in-memory KV
// - NewFollower: open WAL and replay into the store (no leader pinned yet)
// - runCtx: cancellable context to stop raft, replication, and client together
// - NewReplicationServer: bind replication TCP (does not accept until Serve)
// - OpenNode + SetPeers + go Run: load Raft state, register peers, start elections
// - NewWithConfig: wire client HTTP server (WAL owned by Follower)
// - go RunApply: apply committed raft entries to the store, in order
// - go peerServer.Serve: accept leader replication and apply via Follower
// - srv.Serve: block serving client requests until shutdown
// - cleanup: cancel, close replication and follower, surface errors
func runNode(ctx context.Context, opts options) error {
	st := store.NewStore[string, []byte]()

	// replay the wal into the store, nobody leader yet
	follower, err := replication.NewFollower(opts.walPath, st, "")
	if err != nil {
		return fmt.Errorf("create follower: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx) // child context for the node, so we can cancel it if needed
	defer cancel()
	// open raft node
	raftNode, err := raft.OpenNode(opts.nodeID, opts.walPath+"-raft.state")
	if err != nil {
		_ = follower.Close()
		return fmt.Errorf("open raft node: %w", err)
	}
	// load the durable raft log; committed entries are re-applied to the store
	// by RunApply once a leader (or its no-op) commits them
	if err := raftNode.OpenLog(opts.walPath + "-raft.log"); err != nil {
		_ = follower.Close()
		return err
	}
	defer raftNode.CloseLog()
	// listen for peers, this is the "wait for messages" part
	peerServer, err := replication.NewReplicationServer(
		runCtx,
		opts.replicationAddr,
		follower,
		raftNode,
	)
	if err != nil {
		_ = follower.Close()
		return fmt.Errorf("create replication server: %w", err)
	}

	peerSet, err := replication.NewPeerSet(splitNonEmpty(opts.peers))
	if err != nil {
		_ = follower.Close()
		return err
	}
	defer peerSet.Close()

	//peersIds := splitNonEmpty(opts.peers)
	if err := raftNode.SetPeers(peerSet.IDs()); err != nil {
		_ = follower.Close()
		return fmt.Errorf("set raft peers: %w", err)
	}
	clientAddrs, err := clientAddrMap(opts.nodeID, opts.clientAddr, opts.clientPeers)
	if err != nil {
		_ = follower.Close()
		return err
	}
	go raftNode.Run(runCtx, peerSet.RequestVote)              // run the election timer in its own goroutine
	go raftNode.RunHeartbeats(runCtx, peerSet.Heartbeat)      // run the heartbeat timer in its own goroutine
	go raftNode.RunReplication(runCtx, peerSet.AppendEntries) // copy the leader log to peers
	go raftNode.RunApply(runCtx, func(cmd proto.Command) {    // apply committed raft log entries, one at a time
		applyRaftCommand(st, cmd)
	})

	srv := server.NewWithConfig(
		st,
		nil, // wal owned by replication.Follower
		&server.Config{
			Role:               replication.RoleFollower,
			FollowerReplicator: follower,
			RaftNode:           raftNode,
			ClientAddrs:        clientAddrs,
		},
	)

	peerErrCh := make(chan error, 1) // error channel for the peer server
	go func() {
		err := peerServer.Serve() // start replication server in goroutine
		if err != nil && runCtx.Err() == nil {
			cancel()
		}
		peerErrCh <- err // send error to the channel
	}()

	logx.Printf(
		"starting follower node=%s client=%s replication=%s term=%d\n",
		opts.nodeID,
		opts.clientAddr,
		opts.replicationAddr,
		raftNode.Snapshot().CurrentTerm,
	)
	serveErr := srv.Serve(runCtx, opts.clientAddr) // block and serve the client requests
	cancel()
	_ = peerServer.Close()
	peerErr := <-peerErrCh
	_ = follower.Close()
	if serveErr != nil {
		return fmt.Errorf("follower server: %w", serveErr)
	}
	if peerErr != nil && runCtx.Err() == nil {
		return fmt.Errorf("replication server: %w", peerErr)
	}
	return nil

}

// applyRaftCommand writes one committed raft command into the in-memory store.
func applyRaftCommand(st *store.Store[string, []byte], cmd proto.Command) {
	switch cmd.Op {
	case proto.OpSet:
		st.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
	case proto.OpDel:
		st.Delete(string(cmd.Key))
	}
}

// clientAddrMap records where clients can dial each node.
// This node's own listen address is included so peers that learn its id
// can be given a matching entry from their own -client-peers list.
func clientAddrMap(selfID, selfAddr, specs string) (map[string]string, error) {
	out := make(map[string]string)
	if selfID != "" {
		out[selfID] = dialableClientAddr(selfAddr)
	}
	for _, spec := range splitNonEmpty(specs) {
		id, addr, ok := strings.Cut(spec, "=")
		id = strings.TrimSpace(id)
		addr = strings.TrimSpace(addr)
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("client peer must look like id=host:port, got %q", spec)
		}
		out[id] = dialableClientAddr(addr)
	}
	return out, nil
}

// dialableClientAddr turns a listen address into one a client can dial.
func dialableClientAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return net.JoinHostPort("127.0.0.1", port)
	}
	return addr
}

// split a comma separated list of non-empty strings
func splitNonEmpty(value string) []string {
	if value == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}
