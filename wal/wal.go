package wal

// Package wal owns a directory of fixed-size WAL segment files (<20-digit-seq>.wal)
// with append, replay, suffix truncation, and non-blocking prefix compaction.
import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"kvStore/config"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const segmentExt = ".wal"

// walHeader identifies the WAL format and version at offset 0 of every segment.
var walHeader = []byte{'K', 'V', 'W', 'L', 3}

var defaultSegmentMaxBytes = int64(config.Int("wal.segment.max.bytes"))

var ErrUnsupportedFormat = errors.New(
	"wal: unsupported format; remove or recreate the WAL directory",
)

var errLogClosed = errors.New("wal: log is closed")

type segment struct {
	path     string
	seq      uint64 // strictly increasing >= 1; encoded in <20-digit-seq>.wal
	minIndex uint64 // 0 if empty
	maxIndex uint64 // 0 if empty
	size     int64
}

type Log struct {
	dir             string
	segmentMaxBytes int64

	muWal      sync.Mutex
	file       *os.File // open fd for the active (last) segment
	segments   []segment
	lastIndex  uint64 // highest log index in WAL
	totalBytes int64
	closed     bool
}

// ensureHeader writes the WAL header to a new segment or validates it on an existing one.
func ensureHeader(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}

	if info.Size() == 0 {
		n, err := file.Write(walHeader)
		if err != nil {
			return 0, err
		}
		if n != len(walHeader) {
			return 0, io.ErrShortWrite
		}
		if err := file.Sync(); err != nil {
			return 0, err
		}
		return int64(len(walHeader)), nil
	}

	got := make([]byte, len(walHeader))
	n, err := file.ReadAt(got, 0)
	if err != nil || n != len(walHeader) || !bytes.Equal(got, walHeader) {
		return 0, fmt.Errorf("%w: expected WAL v3 header", ErrUnsupportedFormat)
	}
	return info.Size(), nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

func segmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", seq, segmentExt))
}

// discoverSegments returns all <20-digit-seq>.wal files in dir sorted oldest to newest.
func discoverSegments(dir string) ([]segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segs []segment
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, segmentExt) {
			continue
		}
		stem := strings.TrimSuffix(name, segmentExt)
		if len(stem) != 20 {
			continue
		}
		seq, err := strconv.ParseUint(stem, 10, 64)
		if err != nil || seq == 0 {
			continue
		}
		segs = append(segs, segment{path: filepath.Join(dir, name), seq: seq})
	}
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].seq < segs[j].seq
	})
	return segs, nil
}

// Open opens (or creates) a segmented WAL directory at dir.
func Open(dir string) (*Log, error) {
	return OpenWithSegmentMaxBytes(dir, defaultSegmentMaxBytes)
}

// OpenWithSegmentMaxBytes opens a segmented WAL directory with a custom rotation threshold.
func OpenWithSegmentMaxBytes(dir string, maxBytes int64) (*Log, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("Failed to make dir: %w", err)
	}

	segs, err := discoverSegments(dir)
	if err != nil {
		return nil, fmt.Errorf("Failed to list WAL dir: %w", err)
	}
	createdInitial := false
	if len(segs) == 0 {
		segs = []segment{{path: segmentPath(dir, 1), seq: 1}}
		createdInitial = true
	}

	var activeFile *os.File
	var totalBytes int64
	for i := range segs {
		f, err := os.OpenFile(segs[i].path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			if activeFile != nil {
				_ = activeFile.Close()
			}
			return nil, fmt.Errorf("Failed to open file: %w", err)
		}
		size, err := ensureHeader(f)
		if err != nil {
			_ = f.Close()
			if activeFile != nil {
				_ = activeFile.Close()
			}
			return nil, err
		}
		segs[i].size = size
		totalBytes += size
		if i == len(segs)-1 {
			activeFile = f
		} else {
			_ = f.Close()
		}
	}
	if createdInitial {
		if err := syncDir(dir); err != nil {
			_ = activeFile.Close()
			return nil, fmt.Errorf("sync WAL dir: %w", err)
		}
	}

	return &Log{
		dir:             dir,
		segmentMaxBytes: maxBytes,
		file:            activeFile,
		segments:        segs,
		totalBytes:      totalBytes,
	}, nil
}

// rotateLocked closes the current active segment and opens a fresh empty segment
// with sequence number strictly greater than any existing segment.
func (l *Log) rotateLocked(hintSeq uint64) error {
	nextSeq := hintSeq
	if len(l.segments) > 0 {
		last := l.segments[len(l.segments)-1]
		if nextSeq <= last.seq {
			nextSeq = last.seq + 1
		}
		if nextSeq <= last.maxIndex {
			nextSeq = last.maxIndex + 1
		}
	}
	if nextSeq == 0 {
		nextSeq = 1
	}

	newPath := segmentPath(l.dir, nextSeq)
	f, err := os.OpenFile(newPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("create WAL segment: %w", err)
	}
	size, err := ensureHeader(f)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("init WAL segment header: %w", err)
	}
	if err := syncDir(l.dir); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync WAL dir: %w", err)
	}

	if l.file != nil {
		_ = l.file.Close()
	}
	l.file = f
	l.segments = append(l.segments, segment{
		path: newPath,
		seq:  nextSeq,
		size: size,
	})
	l.totalBytes += size
	return nil
}

// Append persists a single record and fsyncs before returning.
func (l *Log) Append(rec Record) error {
	return l.AppendBatch([]Record{rec})
}

// AppendBatch writes all records to the WAL in a single write and fsyncs once
// for the entire batch. If any record fails validation or encoding, nothing is
// written to disk.
func (l *Log) AppendBatch(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, rec := range recs {
		if err := EncodeRecord(&buf, rec); err != nil {
			return fmt.Errorf("Failed to append: %w", err)
		}
	}

	l.muWal.Lock()
	defer l.muWal.Unlock()
	if l.closed {
		return fmt.Errorf("Failed to append: %w", errLogClosed)
	}

	act := &l.segments[len(l.segments)-1]
	if l.segmentMaxBytes > 0 && act.maxIndex > 0 && act.size >= l.segmentMaxBytes {
		if err := l.rotateLocked(recs[0].Index); err != nil {
			return fmt.Errorf("Failed to append: %w", err)
		}
		act = &l.segments[len(l.segments)-1]
	}

	n, err := l.file.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to append: %w", err)
	}
	if n != buf.Len() {
		return fmt.Errorf("Failed to append: %w", io.ErrShortWrite)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	act.size += int64(n)
	l.totalBytes += int64(n)
	for _, rec := range recs {
		if act.minIndex == 0 || rec.Index < act.minIndex {
			act.minIndex = rec.Index
		}
		if rec.Index > act.maxIndex {
			act.maxIndex = rec.Index
		}
	}
	l.lastIndex = recs[len(recs)-1].Index
	return nil
}

// TruncateFrom removes the record at index and every record after it.
// Index is one-based, so TruncateFrom(1) leaves only an empty segment header.
func (l *Log) TruncateFrom(index uint64) error {
	if index == 0 {
		return ErrInvalidIndex
	}

	var toRemove []string
	err := func() error {
		l.muWal.Lock()
		defer l.muWal.Unlock()
		if l.closed {
			return errLogClosed
		}

		// Drop trailing segments whose records are all >= index, keeping at least one segment.
		cut := len(l.segments)
		for cut > 1 {
			seg := l.segments[cut-1]
			prev := l.segments[cut-2]
			if (seg.minIndex > 0 && seg.minIndex >= index) ||
				(seg.minIndex == 0 && prev.maxIndex >= index) {
				toRemove = append(toRemove, seg.path)
				l.totalBytes -= seg.size
				cut--
				continue
			}
			break
		}
		if cut < len(l.segments) {
			if l.file != nil {
				_ = l.file.Close()
				l.file = nil
			}
			l.segments = l.segments[:cut]
			actPath := l.segments[len(l.segments)-1].path
			f, err := os.OpenFile(actPath, os.O_RDWR|os.O_APPEND, 0600)
			if err != nil {
				return fmt.Errorf("reopen WAL segment after truncate: %w", err)
			}
			l.file = f
		}

		act := &l.segments[len(l.segments)-1]
		if _, err := l.file.Seek(int64(len(walHeader)), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek WAL start: %w", err)
		}

		var minIdx, maxIdx uint64
		newSize := act.size
		for {
			position, err := l.file.Seek(0, io.SeekCurrent)
			if err != nil {
				return fmt.Errorf("failed to get WAL position: %w", err)
			}

			record, err := DecodeRecord(l.file)
			if errors.Is(err, io.EOF) {
				newSize = position
				break
			}
			if err != nil {
				return fmt.Errorf("read WAL while truncating: %w", err)
			}

			if record.Index >= index {
				if err := l.file.Truncate(position); err != nil {
					return fmt.Errorf("truncate WAL from index %d: %w", index, err)
				}
				if err := l.file.Sync(); err != nil {
					return fmt.Errorf("sync truncated WAL: %w", err)
				}
				newSize = position
				break
			}

			if minIdx == 0 {
				minIdx = record.Index
			}
			maxIdx = record.Index
		}

		l.totalBytes += newSize - act.size
		act.size = newSize
		act.minIndex = minIdx
		act.maxIndex = maxIdx

		if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
			return fmt.Errorf("failed to seek WAL end: %w", err)
		}

		if maxIdx > 0 {
			l.lastIndex = maxIdx
		} else if len(l.segments) > 1 {
			l.lastIndex = l.segments[len(l.segments)-2].maxIndex
		} else {
			l.lastIndex = 0
		}
		return nil
	}()
	for _, p := range toRemove {
		_ = os.Remove(p)
	}
	return err
}

// Replay applies every record in the WAL from index 1 in order.
func (l *Log) Replay(apply func(Record) error) error {
	return l.ReplayAfter(0, apply)
}

// ReplayAfter scans all segments in order, skipping records with Index <= afterIndex
// and calling apply for each record with Index > afterIndex (requiring contiguity
// starting at afterIndex+1).
func (l *Log) ReplayAfter(afterIndex uint64, apply func(Record) error) error {
	l.muWal.Lock()
	defer l.muWal.Unlock()
	if l.closed {
		return errLogClosed
	}

	var (
		totalBytes int64
		lastSeen   uint64
		emitted    bool
		expected   = afterIndex + 1
	)

	for i := range l.segments {
		seg := &l.segments[i]
		isLast := i == len(l.segments)-1

		f := l.file
		if !isLast {
			var err error
			f, err = os.Open(seg.path)
			if err != nil {
				return fmt.Errorf("open WAL segment %s: %w", seg.path, err)
			}
		}

		if _, err := f.Seek(int64(len(walHeader)), io.SeekStart); err != nil {
			if !isLast {
				_ = f.Close()
			}
			return fmt.Errorf("failed to move cursor: %w", err)
		}

		var minIdx, maxIdx uint64
		segSize := seg.size
		for {
			pos, err := f.Seek(0, io.SeekCurrent)
			if err != nil {
				if !isLast {
					_ = f.Close()
				}
				return fmt.Errorf("failed to get WAL position: %w", err)
			}

			rec, err := DecodeRecord(f)
			if errors.Is(err, io.EOF) {
				segSize = pos
				break
			}
			if errors.Is(err, ErrIncompleteRecord) {
				if !isLast {
					_ = f.Close()
					return err
				}
				if err := f.Truncate(pos); err != nil {
					return fmt.Errorf("failed to truncate incomplete WAL tail: %w", err)
				}
				segSize = pos
				break
			}
			if err != nil {
				if !isLast {
					_ = f.Close()
				}
				return err
			}

			if minIdx == 0 {
				minIdx = rec.Index
			}
			maxIdx = rec.Index

			if rec.Index <= afterIndex {
				if rec.Index > lastSeen {
					lastSeen = rec.Index
				}
				continue
			}

			if rec.Index != expected {
				if !isLast {
					_ = f.Close()
				}
				return fmt.Errorf(
					"%w: got index %d, want %d",
					ErrIndexOutOfOrder,
					rec.Index,
					expected,
				)
			}
			if err := apply(rec); err != nil {
				if !isLast {
					_ = f.Close()
				}
				return err
			}
			emitted = true
			lastSeen = rec.Index
			expected = rec.Index + 1
		}

		if !isLast {
			_ = f.Close()
		}
		seg.minIndex = minIdx
		seg.maxIndex = maxIdx
		seg.size = segSize
		totalBytes += segSize
	}

	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek WAL end: %w", err)
	}
	l.totalBytes = totalBytes
	if emitted || lastSeen > afterIndex {
		l.lastIndex = lastSeen
	} else {
		l.lastIndex = afterIndex
	}
	return nil
}

// Compact deletes closed segments whose records are all <= throughIndex outside muWal.
// If the active segment contains records <= throughIndex, it is rotated cleanly
// (with zero file rewrite) so either it is unlinked immediately (if all its
// records are <= throughIndex) or sealed for deletion on the next compaction.
func (l *Log) Compact(throughIndex uint64) error {
	if throughIndex == 0 {
		return nil
	}

	var toRemove []string
	err := func() error {
		l.muWal.Lock()
		defer l.muWal.Unlock()
		if l.closed {
			return nil
		}

		act := l.segments[len(l.segments)-1]
		if act.maxIndex > 0 && act.minIndex <= throughIndex {
			hint := throughIndex + 1
			if act.maxIndex >= hint {
				hint = act.maxIndex + 1
			}
			if err := l.rotateLocked(hint); err != nil {
				return err
			}
		}

		kept := l.segments[:0]
		lastPos := len(l.segments) - 1
		for i, seg := range l.segments {
			if i < lastPos && seg.maxIndex <= throughIndex {
				toRemove = append(toRemove, seg.path)
				l.totalBytes -= seg.size
				continue
			}
			kept = append(kept, seg)
		}
		l.segments = kept
		return nil
	}()
	for _, p := range toRemove {
		_ = os.Remove(p)
	}
	return err
}

// ResetAfter discards all existing WAL segments and opens a fresh empty segment
// starting after index. Used when InstallSnapshot replaces a divergent log.
func (l *Log) ResetAfter(index uint64) error {
	var toRemove []string
	err := func() error {
		l.muWal.Lock()
		defer l.muWal.Unlock()
		if l.closed {
			return errLogClosed
		}

		oldSegs := append([]segment(nil), l.segments...)
		if err := l.rotateLocked(index + 1); err != nil {
			return err
		}
		newSeg := l.segments[len(l.segments)-1]
		l.segments = []segment{newSeg}
		l.totalBytes = newSeg.size
		l.lastIndex = index
		for _, seg := range oldSegs {
			if seg.path != newSeg.path {
				toRemove = append(toRemove, seg.path)
			}
		}
		return nil
	}()
	for _, p := range toRemove {
		_ = os.Remove(p)
	}
	return err
}

// Size returns the approximate total bytes on disk across all live WAL segments.
func (l *Log) Size() int64 {
	l.muWal.Lock()
	defer l.muWal.Unlock()
	return l.totalBytes
}

// Close flushes and closes the active WAL segment.
func (l *Log) Close() error {
	l.muWal.Lock()
	defer l.muWal.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.file == nil {
		return nil
	}
	if err := l.file.Sync(); err != nil {
		_ = l.file.Close()
		return fmt.Errorf("Error during closing Wal file: %w", err)
	}
	return l.file.Close()
}

// LastIndex returns the highest record index observed during append or replay.
func (l *Log) LastIndex() uint64 {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	return l.lastIndex
}
