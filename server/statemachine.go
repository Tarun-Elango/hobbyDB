package server

import (
	"kvStore/proto"
	"kvStore/store"
)

// StateMachine applies committed Raft commands to the store.
//
// A client may resend a write after a timeout, a dropped connection or a
// NotLeader reply, and the first attempt may already be in the log, so the
// same write can be committed twice. Each write carries the client's id and a
// sequence number that grows by one per write and stays the same on retries.
// A write is applied only if its number is above the last one applied for
// that client; otherwise it is skipped, and the retry still answers OK.
//
// Every node applies the same log in the same order, so every node skips the
// same entries. The table is in memory only: after a restart RunApply replays
// the log from index 1 and rebuilds it along with the store.
//
// Apply is called only from the single RunApply goroutine, so it needs no lock.
// The table keeps one entry per client id and is never trimmed.
type StateMachine struct {
	store   *store.Store[string, []byte]
	lastSeq map[uint64]uint64 // client id -> highest applied seq
}

func NewStateMachine(st *store.Store[string, []byte]) *StateMachine {
	return &StateMachine{store: st, lastSeq: make(map[uint64]uint64)}
}

// Apply writes one committed command into the store, at most once per (client, seq).
func (m *StateMachine) Apply(cmd proto.Command) {
	if !proto.IsWrite(cmd.Op) {
		return // no-op entries change nothing
	}
	if cmd.ClientID != 0 {
		if cmd.Seq <= m.lastSeq[cmd.ClientID] {
			return // duplicate, or an older write from this client arriving late
		}
		m.lastSeq[cmd.ClientID] = cmd.Seq
	}
	switch cmd.Op {
	case proto.OpSet:
		m.store.Set(string(cmd.Key), append([]byte(nil), cmd.Value...))
	case proto.OpDel:
		m.store.Delete(string(cmd.Key))
	}
}
