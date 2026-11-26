package main

import (
	"context"
	"encoding/json"
	"errors"
	"kvStore/logx"
	"kvStore/proto"
	"kvStore/raft"
	"kvStore/server"
	"net"
	"net/http"
	"time"
)

// UI-only HTTP listener (-ui-addr). It exists so the cluster monitor in
// cmd/ui can read a node's Raft status and run console commands as JSON.
// Real clients still talk the binary protocol on -client-addr.
//
//	GET  /ui/status -> {"id","role","term","leader"}
//	POST /ui/kv     {"op","key","value"} -> {"status","value","leader"}

type uiStatus struct {
	ID     string `json:"id"`
	Role   string `json:"role"`
	Term   uint64 `json:"term"`
	Leader string `json:"leader"`
}

type uiKVRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type uiKVResponse struct {
	Status string `json:"status"`           // ok | not_found | error | not_leader
	Value  string `json:"value,omitempty"`  // GET/LEN result, or error text
	Leader string `json:"leader,omitempty"` // not_leader: leader's client address (or id)
}

var uiOps = map[string]byte{
	"get": proto.OpGet, "set": proto.OpSet, "del": proto.OpDel,
	"len": proto.OpLen, "ping": proto.OpPing,
}

func roleName(r raft.Role) string {
	switch r {
	case raft.Leader:
		return "leader"
	case raft.Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

// startUIServer binds addr and serves until ctx is done.
func startUIServer(ctx context.Context, addr, nodeID string, node *raft.Node, srv *server.Server) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ui/status", func(w http.ResponseWriter, r *http.Request) {
		st := node.Status()
		writeJSON(w, uiStatus{ID: nodeID, Role: roleName(st.Role), Term: st.CurrentTerm, Leader: st.LeaderID})
	})
	mux.HandleFunc("POST /ui/kv", func(w http.ResponseWriter, r *http.Request) {
		var req uiKVRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeJSON(w, uiKVResponse{Status: "error", Value: "bad request: " + err.Error()})
			return
		}
		op, ok := uiOps[req.Op]
		if !ok {
			writeJSON(w, uiKVResponse{Status: "error", Value: "unknown op " + req.Op})
			return
		}
		resp, err := srv.Do(r.Context(), proto.Command{Op: op, Key: []byte(req.Key), Value: []byte(req.Value)})
		if err != nil {
			writeJSON(w, uiKVResponse{Status: "error", Value: err.Error()})
			return
		}
		out := uiKVResponse{}
		switch resp.Status {
		case proto.StatusOk:
			out.Status, out.Value = "ok", string(resp.Value)
		case proto.StatusNotFound:
			out.Status = "not_found"
		case proto.StatusNotLeader:
			out.Status, out.Leader = "not_leader", string(resp.Value)
		default:
			out.Status, out.Value = "error", string(resp.Value)
		}
		writeJSON(w, out)
	})

	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = hs.Close()
	}()
	go func() {
		if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logx.Errorf("ui http server: %v\n", err)
		}
	}()
	return nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
