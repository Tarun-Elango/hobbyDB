package raft

import (
	"fmt"

	"kvStore/proto"
	"kvStore/wal"
)

// OpenLog opens the durable raft log at path and loads every entry into memory.
// Entries are not applied to the store here: whether they are committed is
// only known once a leader says so.
func (n *Node) OpenLog(path string) error {
	log, err := wal.Open(path)
	if err != nil {
		return fmt.Errorf("open raft log: %w", err)
	}
	var entries []LogEntry
	err = log.Replay(func(rec wal.Record) error {
		op, err := protoOp(rec.Op)
		if err != nil {
			return err
		}
		entries = append(entries, LogEntry{
			Index:   rec.Index,
			Term:    rec.Term,
			Command: proto.Command{Op: op, Key: rec.Key, Value: rec.Value},
		})
		return nil
	})
	if err != nil {
		_ = log.Close()
		return fmt.Errorf("replay raft log: %w", err)
	}

	n.mu.Lock()
	n.logWAL = log
	n.Log = entries
	n.mu.Unlock()
	return nil
}

// CloseLog flushes and closes the durable log.
func (n *Node) CloseLog() error {
	n.mu.Lock()
	log := n.logWAL
	n.logWAL = nil
	n.mu.Unlock()
	if log == nil {
		return nil
	}
	return log.Close()
}

// persistAppendLocked makes entry durable. Call it before the entry is added
// to the in-memory log or acknowledged to anyone.
func (n *Node) persistAppendLocked(entry LogEntry) error {
	if n.logWAL == nil {
		return nil
	}
	op, err := walOp(entry.Command.Op)
	if err != nil {
		return err
	}
	return n.logWAL.Append(wal.Record{
		Index: entry.Index,
		Term:  entry.Term,
		Op:    op,
		Key:   entry.Command.Key,
		Value: entry.Command.Value,
	})
}

// persistTruncateLocked drops entry index and everything after it from disk.
func (n *Node) persistTruncateLocked(index uint64) error {
	if n.logWAL == nil {
		return nil
	}
	return n.logWAL.TruncateFrom(index)
}

func walOp(op byte) (byte, error) {
	switch op {
	case proto.OpSet:
		return byte(wal.OpSet), nil
	case proto.OpDel:
		return byte(wal.OpDel), nil
	case proto.OpNoop:
		return byte(wal.OpNoop), nil
	}
	return 0, fmt.Errorf("raft: cannot persist op %d", op)
}

func protoOp(op byte) (byte, error) {
	switch wal.Op(op) {
	case wal.OpSet:
		return proto.OpSet, nil
	case wal.OpDel:
		return proto.OpDel, nil
	case wal.OpNoop:
		return proto.OpNoop, nil
	}
	return 0, fmt.Errorf("raft: unknown persisted op %d", op)
}
