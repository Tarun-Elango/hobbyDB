package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentedRotationCompactAndReplayAfter(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "raft.log")

	// Tiny segment size to force frequent rotation.
	log, err := OpenWithSegmentMaxBytes(base, 128)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	val := bytes.Repeat([]byte("x"), 40)
	for i := uint64(1); i <= 20; i++ {
		if err := log.Append(Record{Index: i, Term: 1, Op: byte(OpSet), Key: []byte("k"), Value: val}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	segsBefore, err := discoverSegments(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(segsBefore) < 4 {
		t.Fatalf("expected >=4 segments, got %d", len(segsBefore))
	}
	sizeBefore := log.Size()

	// Compact through 15: older segments must be deleted and size must shrink.
	if err := log.Compact(15); err != nil {
		t.Fatalf("compact(15): %v", err)
	}
	segsAfter, err := discoverSegments(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(segsAfter) >= len(segsBefore) {
		t.Fatalf("expected segment count to shrink: before=%d after=%d", len(segsBefore), len(segsAfter))
	}
	if log.Size() >= sizeBefore {
		t.Fatalf("expected WAL size to shrink: before=%d after=%d", sizeBefore, log.Size())
	}

	// Append more after compaction, then close and reopen.
	for i := uint64(21); i <= 25; i++ {
		if err := log.Append(Record{Index: i, Term: 2, Op: byte(OpSet), Key: []byte("k2"), Value: val}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithSegmentMaxBytes(base, 128)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	var got []uint64
	if err := reopened.ReplayAfter(15, func(rec Record) error {
		got = append(got, rec.Index)
		return nil
	}); err != nil {
		t.Fatalf("ReplayAfter(15): %v", err)
	}
	if len(got) != 10 || got[0] != 16 || got[len(got)-1] != 25 {
		t.Fatalf("replayed indexes = %v, want 16..25", got)
	}
	if reopened.LastIndex() != 25 {
		t.Fatalf("LastIndex() = %d, want 25", reopened.LastIndex())
	}
}

func TestSegmentedTruncateAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "raft.log")

	log, err := OpenWithSegmentMaxBytes(base, 128)
	if err != nil {
		t.Fatal(err)
	}
	val := bytes.Repeat([]byte("y"), 40)
	for i := uint64(1); i <= 15; i++ {
		if err := log.Append(Record{Index: i, Term: 1, Op: byte(OpSet), Key: []byte("k"), Value: val}); err != nil {
			t.Fatal(err)
		}
	}

	if err := log.TruncateFrom(6); err != nil {
		t.Fatalf("TruncateFrom(6): %v", err)
	}
	if log.LastIndex() != 5 {
		t.Fatalf("LastIndex() = %d, want 5", log.LastIndex())
	}
	if err := log.Append(Record{Index: 6, Term: 2, Op: byte(OpSet), Key: []byte("k"), Value: []byte("new")}); err != nil {
		t.Fatalf("append after truncate: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithSegmentMaxBytes(base, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	var got []Record
	if err := reopened.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 || got[5].Term != 2 || string(got[5].Value) != "new" {
		t.Fatalf("unexpected records after truncate: %+v", got)
	}
}

func TestResetAfterDiscardsAllSegments(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "raft.log")

	log, err := OpenWithSegmentMaxBytes(base, 128)
	if err != nil {
		t.Fatal(err)
	}
	val := bytes.Repeat([]byte("z"), 40)
	for i := uint64(1); i <= 12; i++ {
		if err := log.Append(Record{Index: i, Term: 1, Op: byte(OpSet), Key: []byte("k"), Value: val}); err != nil {
			t.Fatal(err)
		}
	}

	if err := log.ResetAfter(100); err != nil {
		t.Fatalf("ResetAfter(100): %v", err)
	}
	segs, err := discoverSegments(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("expected 1 fresh segment after ResetAfter, got %d", len(segs))
	}
	if err := log.Append(Record{Index: 101, Term: 3, Op: byte(OpSet), Key: []byte("a"), Value: []byte("b")}); err != nil {
		t.Fatalf("append 101: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithSegmentMaxBytes(base, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	var got []Record
	if err := reopened.ReplayAfter(100, func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("ReplayAfter(100): %v", err)
	}
	if len(got) != 1 || got[0].Index != 101 || got[0].Term != 3 {
		t.Fatalf("replayed = %+v, want only index 101", got)
	}
}

func TestCompactSealsStraddlingActiveSegment(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "raft.log")

	// Large segment size so rotation ONLY happens via Compact sealing.
	log, err := OpenWithSegmentMaxBytes(base, 16*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	for i := uint64(1); i <= 10; i++ {
		if err := log.Append(Record{Index: i, Term: 1, Op: byte(OpSet), Key: []byte("k"), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	// Straddles 1..10: Compact(5) seals segment 0 and rotates so future appends land in a new segment.
	if err := log.Compact(5); err != nil {
		t.Fatalf("Compact(5): %v", err)
	}
	for i := uint64(11); i <= 15; i++ {
		if err := log.Append(Record{Index: i, Term: 1, Op: byte(OpSet), Key: []byte("k"), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	// Next compaction past 10 must unlink the sealed 1..10 segment.
	if err := log.Compact(12); err != nil {
		t.Fatalf("Compact(12): %v", err)
	}
	firstSeg := segmentPath(base, 1)
	if _, err := os.Stat(firstSeg); !os.IsNotExist(err) {
		t.Fatalf("expected original segment %s to be deleted, stat err=%v", firstSeg, err)
	}
}

func TestReplayAfterRejectsGap(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "raft.log")

	log, err := Open(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.ResetAfter(10); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(Record{Index: 12, Term: 1, Op: byte(OpSet), Key: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	err = log.ReplayAfter(10, func(Record) error { return nil })
	if !errors.Is(err, ErrIndexOutOfOrder) {
		t.Fatalf("ReplayAfter(10) err = %v, want ErrIndexOutOfOrder", err)
	}
	_ = log.Close()
}
