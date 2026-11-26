package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"kvStore/proto"
	"kvStore/store"
)

const (
	snapTagEOF byte = 0
	snapTagKV  byte = 1
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
// same entries. Snapshots include lastSeq alongside the KV pairs so retries
// stay deduplicated after log compaction or InstallSnapshot.
//
// Apply, BeginSnapshot, and RestoreSnapshot are called only from the single
// RunApply goroutine (or before it starts), so lastSeq needs no lock.
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

// BeginSnapshot captures a consistent point-in-time view of both lastSeq and
// the store without blocking future reads or writes. The returned writeFn
// streams the snapshot payload on a background goroutine; closeFn releases any
// remaining copy-on-write state.
func (m *StateMachine) BeginSnapshot() (func(io.Writer) error, func()) {
	seqCopy := make(map[uint64]uint64, len(m.lastSeq))
	for k, v := range m.lastSeq {
		seqCopy[k] = v
	}
	snap := m.store.BeginSnapshot()

	writeFn := func(w io.Writer) error {
		if err := binary.Write(w, binary.BigEndian, uint32(len(seqCopy))); err != nil {
			return err
		}
		var pair [16]byte
		for cid, seq := range seqCopy {
			binary.BigEndian.PutUint64(pair[0:8], cid)
			binary.BigEndian.PutUint64(pair[8:16], seq)
			if _, err := w.Write(pair[:]); err != nil {
				return err
			}
		}
		var lenBuf [5]byte
		lenBuf[0] = snapTagKV
		if err := snap.ForEach(func(k string, v []byte) error {
			binary.BigEndian.PutUint32(lenBuf[1:5], uint32(len(k)))
			if _, err := w.Write(lenBuf[:]); err != nil {
				return err
			}
			if _, err := io.WriteString(w, k); err != nil {
				return err
			}
			if err := binary.Write(w, binary.BigEndian, uint32(len(v))); err != nil {
				return err
			}
			if len(v) > 0 {
				if _, err := w.Write(v); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		_, err := w.Write([]byte{snapTagEOF})
		return err
	}

	return writeFn, snap.Close
}

// RestoreSnapshot replaces the store and client dedup table from a payload
// previously written by BeginSnapshot.
func (m *StateMachine) RestoreSnapshot(r io.Reader) error {
	var seqCount uint32
	if err := binary.Read(r, binary.BigEndian, &seqCount); err != nil {
		return fmt.Errorf("statemachine: read dedup count: %w", err)
	}
	nextSeq := make(map[uint64]uint64, seqCount)
	var pair [16]byte
	for i := uint32(0); i < seqCount; i++ {
		if _, err := io.ReadFull(r, pair[:]); err != nil {
			return fmt.Errorf("statemachine: read dedup entry: %w", err)
		}
		cid := binary.BigEndian.Uint64(pair[0:8])
		seq := binary.BigEndian.Uint64(pair[8:16])
		nextSeq[cid] = seq
	}

	data := make(map[string][]byte)
	var tag [1]byte
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(r, tag[:]); err != nil {
			return fmt.Errorf("statemachine: read entry tag: %w", err)
		}
		if tag[0] == snapTagEOF {
			break
		}
		if tag[0] != snapTagKV {
			return fmt.Errorf("statemachine: unknown snapshot tag %d", tag[0])
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return fmt.Errorf("statemachine: read key len: %w", err)
		}
		kLen := int(binary.BigEndian.Uint32(lenBuf[:]))
		if kLen > proto.MaxKeyLen {
			return errors.New("statemachine: snapshot key exceeds max length")
		}
		key := make([]byte, kLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return fmt.Errorf("statemachine: read key: %w", err)
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return fmt.Errorf("statemachine: read value len: %w", err)
		}
		vLen := int(binary.BigEndian.Uint32(lenBuf[:]))
		if vLen > proto.MaxValueLen {
			return errors.New("statemachine: snapshot value exceeds max length")
		}
		val := make([]byte, vLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return fmt.Errorf("statemachine: read value: %w", err)
		}
		data[string(key)] = val
	}

	m.store.Replace(data)
	m.lastSeq = nextSeq
	return nil
}
