package raft

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"kvStore/config"
	"kvStore/logx"
	"kvStore/store"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"
)

var (
	snapshotMagic   = [4]byte{'K', 'V', 'S', 'N'}
	snapshotVersion = byte(1)
)

const snapshotHeaderSize = 4 + 1 + 8 + 8 // magic + version + index + term
const snapshotFooterSize = 4             // crc32 IEEE

var (
	defaultSnapshotThresholdEntries = uint64(config.Int("raft.snapshot.threshold.entries"))
	defaultSnapshotThresholdBytes   = int64(config.Int("raft.snapshot.threshold.bytes"))
	defaultSnapshotTrailingLogs     = uint64(config.Int("raft.snapshot.trailing.logs"))
	snapshotChunkBytes              = mustPositiveInt("raft.snapshot.chunk.bytes", config.Int("raft.snapshot.chunk.bytes"))
	snapshotMaxBytesPerSec          = int64(config.Int("raft.snapshot.max.bytes.per.sec"))
)

var (
	ErrCorruptSnapshot = errors.New("raft: corrupt snapshot")
)

// Snapshotter connects Raft snapshotting and InstallSnapshot to the state machine.
// All methods are called from the single RunApply goroutine (or during startup
// before RunApply starts), except the writeFn returned by BeginSnapshot which
// runs on a background goroutine.
type Snapshotter interface {
	BeginSnapshot() (writeFn func(w io.Writer) error, closeFn func())
	RestoreSnapshot(r io.Reader) error
}

// SnapshotMeta identifies the last Raft log entry included in a snapshot.
type SnapshotMeta struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

// InstallSnapshotArgs carries one chunk of a snapshot file from leader to follower.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Offset            uint64
	Data              []byte
	Done              bool
}

// InstallSnapshotResult replies with the follower's current term.
type InstallSnapshotResult struct {
	Term uint64
}

// InstallSnapshotRPC sends one InstallSnapshot chunk RPC to a peer.
type InstallSnapshotRPC func(ctx context.Context, peerID string, args InstallSnapshotArgs) (InstallSnapshotResult, error)

type pendingSnapshot struct {
	index uint64
	term  uint64
}

type snapshotReceiver struct {
	index  uint64
	term   uint64
	offset uint64
	file   *os.File
	path   string
}

// jitter adds ±15% random variance so nodes don't all snapshot and fsync together.
func jitterUint64(v uint64) uint64 {
	if v < 10 {
		return v
	}
	span := v * 30 / 100
	if span == 0 {
		return v
	}
	return v - span/2 + rand.Uint64N(span+1)
}

func jitterInt64(v int64) int64 {
	if v < 10 {
		return v
	}
	span := v * 30 / 100
	if span == 0 {
		return v
	}
	return v - span/2 + rand.Int64N(span+1)
}

// SetSnapshotter attaches the state machine snapshotter.
func (n *Node) SetSnapshotter(sm Snapshotter) {
	n.mu.Lock()
	n.snapshotter = sm
	n.mu.Unlock()
}

// SetInstallSnapshotRPC attaches the peer RPC used to stream snapshots to lagging followers.
func (n *Node) SetInstallSnapshotRPC(rpc InstallSnapshotRPC) {
	n.mu.Lock()
	n.installSnapRPC = rpc
	n.mu.Unlock()
}

func (n *Node) getInstallSnapshotRPC() InstallSnapshotRPC {
	n.mu.RLock()
	rpc := n.installSnapRPC
	n.mu.RUnlock()
	return rpc
}

// SetSnapshotConfig overrides automatic snapshot thresholds (useful in tests).
// A zero threshold disables that specific trigger.
func (n *Node) SetSnapshotConfig(thresholdEntries uint64, thresholdBytes int64, trailingLogs uint64) {
	n.mu.Lock()
	n.snapThresholdEntries = thresholdEntries
	n.snapThresholdBytes = thresholdBytes
	n.snapTrailingLogs = trailingLogs
	n.mu.Unlock()
}

// OpenSnapshot registers path as the node's snapshot file and, if a snapshot
// already exists on disk, validates its checksum and term, restores it into sm,
// and initializes SnapshotIndex/CommitIndex/LastApplied. Call after OpenNode
// and before OpenLog.
func (n *Node) OpenSnapshot(path string, sm Snapshotter) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}

	n.mu.Lock()
	n.snapPath = path
	if sm != nil {
		n.snapshotter = sm
	}
	currentTerm := n.CurrentTerm
	n.mu.Unlock()

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	meta, payloadSize, err := verifySnapshotFile(f)
	if err != nil {
		return fmt.Errorf("verify snapshot %s: %w", path, err)
	}
	if meta.LastIncludedTerm > currentTerm {
		return fmt.Errorf(
			"%w: snapshot %s is at index %d term %d, state file %s has term %d",
			ErrLogAheadOfState, path, meta.LastIncludedIndex, meta.LastIncludedTerm, n.statePath, currentTerm,
		)
	}

	if sm != nil {
		if _, err := f.Seek(snapshotHeaderSize, io.SeekStart); err != nil {
			return fmt.Errorf("seek snapshot payload: %w", err)
		}
		if err := sm.RestoreSnapshot(io.LimitReader(bufio.NewReader(f), payloadSize)); err != nil {
			return fmt.Errorf("restore snapshot %s: %w", path, err)
		}
	}

	n.mu.Lock()
	n.SnapshotIndex = meta.LastIncludedIndex
	n.SnapshotTerm = meta.LastIncludedTerm
	if n.CommitIndex < meta.LastIncludedIndex {
		n.setCommitLocked(meta.LastIncludedIndex, meta.LastIncludedTerm)
	}
	if n.LastApplied < meta.LastIncludedIndex {
		n.setLastAppliedLocked(meta.LastIncludedIndex)
	}
	n.mu.Unlock()
	return nil
}

func encodeSnapshotHeader(meta SnapshotMeta) [snapshotHeaderSize]byte {
	var hdr [snapshotHeaderSize]byte
	copy(hdr[0:4], snapshotMagic[:])
	hdr[4] = snapshotVersion
	binary.BigEndian.PutUint64(hdr[5:13], meta.LastIncludedIndex)
	binary.BigEndian.PutUint64(hdr[13:21], meta.LastIncludedTerm)
	return hdr
}

func decodeSnapshotHeader(hdr []byte) (SnapshotMeta, error) {
	if len(hdr) < snapshotHeaderSize {
		return SnapshotMeta{}, fmt.Errorf("%w: truncated header", ErrCorruptSnapshot)
	}
	if !bytes.Equal(hdr[0:4], snapshotMagic[:]) {
		return SnapshotMeta{}, fmt.Errorf("%w: bad magic", ErrCorruptSnapshot)
	}
	if hdr[4] != snapshotVersion {
		return SnapshotMeta{}, fmt.Errorf("%w: unsupported version %d", ErrCorruptSnapshot, hdr[4])
	}
	meta := SnapshotMeta{
		LastIncludedIndex: binary.BigEndian.Uint64(hdr[5:13]),
		LastIncludedTerm:  binary.BigEndian.Uint64(hdr[13:21]),
	}
	if meta.LastIncludedIndex == 0 {
		return SnapshotMeta{}, fmt.Errorf("%w: zero last included index", ErrCorruptSnapshot)
	}
	return meta, nil
}

// verifySnapshotFile validates header and full-file CRC32 trailer of f.
func verifySnapshotFile(f *os.File) (SnapshotMeta, int64, error) {
	info, err := f.Stat()
	if err != nil {
		return SnapshotMeta{}, 0, err
	}
	minSize := int64(snapshotHeaderSize + snapshotFooterSize)
	if info.Size() < minSize {
		return SnapshotMeta{}, 0, fmt.Errorf("%w: file too small (%d bytes)", ErrCorruptSnapshot, info.Size())
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return SnapshotMeta{}, 0, err
	}
	var hdr [snapshotHeaderSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return SnapshotMeta{}, 0, fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
	}
	meta, err := decodeSnapshotHeader(hdr[:])
	if err != nil {
		return SnapshotMeta{}, 0, err
	}

	payloadSize := info.Size() - minSize
	h := crc32.NewIEEE()
	_, _ = h.Write(hdr[:])
	br := bufio.NewReader(f)
	if _, err := io.CopyN(h, br, payloadSize); err != nil {
		return SnapshotMeta{}, 0, fmt.Errorf("%w: read payload: %v", ErrCorruptSnapshot, err)
	}
	var crcBuf [snapshotFooterSize]byte
	if _, err := io.ReadFull(br, crcBuf[:]); err != nil {
		return SnapshotMeta{}, 0, fmt.Errorf("%w: read crc: %v", ErrCorruptSnapshot, err)
	}
	wantCRC := binary.BigEndian.Uint32(crcBuf[:])
	if gotCRC := h.Sum32(); gotCRC != wantCRC {
		return SnapshotMeta{}, 0, fmt.Errorf("%w: crc mismatch got=%#x want=%#x", ErrCorruptSnapshot, gotCRC, wantCRC)
	}
	return meta, payloadSize, nil
}

// rateLimitedWriter paces sequential snapshot I/O so background disk writes do
// not starve foreground WAL group-commit fsync latency.
type rateLimitedWriter struct {
	w           io.Writer
	bytesPerSec int64
	written     int64
	start       time.Time
}

func (rw *rateLimitedWriter) Write(p []byte) (int, error) {
	n, err := rw.w.Write(p)
	if n > 0 && rw.bytesPerSec > 0 {
		if rw.start.IsZero() {
			rw.start = time.Now()
		}
		rw.written += int64(n)
		want := time.Duration(float64(rw.written) / float64(rw.bytesPerSec) * float64(time.Second))
		if delay := want - time.Since(rw.start); delay > time.Millisecond {
			time.Sleep(delay)
		}
	}
	return n, err
}

// shouldSnapshotLocked reports whether RunApply should kick off a background
// local snapshot at the current LastApplied index. Caller holds n.mu.
func (n *Node) shouldSnapshotLocked() bool {
	if n.snapPath == "" || n.snapshotter == nil || n.snapInFlight {
		return false
	}
	if n.LastApplied <= n.SnapshotIndex {
		return false
	}
	if n.snapThresholdEntries > 0 && n.LastApplied-n.SnapshotIndex >= n.snapThresholdEntries {
		return true
	}
	if n.snapThresholdBytes > 0 && n.logWAL != nil && n.logWAL.Size() >= n.snapThresholdBytes {
		return true
	}
	return false
}

// finishLocalSnapshot streams the already-captured CoW snapshot to a temp file,
// fsyncs it, atomically installs it, trims the in-memory log, and compacts old
// WAL segments — all without blocking reads, Propose, or RunApply.
func (n *Node) finishLocalSnapshot(
	snapIdx, snapTerm uint64,
	snapPath string,
	writeFn func(io.Writer) error,
	closeFn func(),
) {
	defer closeFn()
	defer func() {
		n.mu.Lock()
		n.snapInFlight = false
		n.mu.Unlock()
	}()

	dir := filepath.Dir(snapPath)
	tmp, err := os.CreateTemp(dir, ".raft-snap-*")
	if err != nil {
		logx.Errorf("raft: create snapshot temp file: %v\n", err)
		return
	}
	tmpPath := tmp.Name()
	keepTmp := false
	defer func() {
		if !keepTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	hdr := encodeSnapshotHeader(SnapshotMeta{
		LastIncludedIndex: snapIdx,
		LastIncludedTerm:  snapTerm,
	})
	h := crc32.NewIEEE()
	bw := bufio.NewWriterSize(tmp, 256*1024)
	rlw := &rateLimitedWriter{w: bw, bytesPerSec: snapshotMaxBytesPerSec}
	mw := io.MultiWriter(rlw, h)

	if _, err := mw.Write(hdr[:]); err != nil {
		_ = tmp.Close()
		logx.Errorf("raft: write snapshot header: %v\n", err)
		return
	}
	if err := writeFn(mw); err != nil {
		_ = tmp.Close()
		if !errors.Is(err, store.ErrSnapshotAborted) {
			logx.Errorf("raft: write snapshot payload: %v\n", err)
		}
		return
	}
	var crcBuf [snapshotFooterSize]byte
	binary.BigEndian.PutUint32(crcBuf[:], h.Sum32())
	if _, err := bw.Write(crcBuf[:]); err != nil {
		_ = tmp.Close()
		logx.Errorf("raft: write snapshot crc: %v\n", err)
		return
	}
	if err := bw.Flush(); err != nil {
		_ = tmp.Close()
		logx.Errorf("raft: flush snapshot: %v\n", err)
		return
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		logx.Errorf("raft: sync snapshot: %v\n", err)
		return
	}
	if err := tmp.Close(); err != nil {
		logx.Errorf("raft: close snapshot: %v\n", err)
		return
	}

	var (
		compactThrough uint64
		walLog         = n.logWAL
	)
	n.mu.Lock()
	if snapIdx <= n.SnapshotIndex || n.snapPath != snapPath {
		n.mu.Unlock()
		return
	}
	n.snapFileMu.Lock()
	err = os.Rename(tmpPath, snapPath)
	n.snapFileMu.Unlock()
	if err != nil {
		n.mu.Unlock()
		logx.Errorf("raft: install snapshot: %v\n", err)
		return
	}
	keepTmp = true

	n.SnapshotIndex = snapIdx
	n.SnapshotTerm = snapTerm
	if snapIdx > n.snapTrailingLogs {
		compactThrough = snapIdx - n.snapTrailingLogs
	}
	if compactThrough > 0 {
		n.trimLogPrefixLocked(compactThrough)
	}
	walLog = n.logWAL
	n.mu.Unlock()

	// Persist the renamed directory entry BEFORE unlinking old WAL segments so
	// crash recovery can never lose both the snapshot entry and the WAL prefix.
	if err := syncDir(dir); err != nil {
		logx.Errorf("raft: sync snapshot dir: %v\n", err)
		return
	}
	if compactThrough > 0 && walLog != nil {
		if err := walLog.Compact(compactThrough); err != nil {
			logx.Errorf("raft: compact WAL through %d: %v\n", compactThrough, err)
		}
	}
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

// applyPendingSnapshot runs on the single RunApply goroutine to restore an
// installed snapshot into the state machine without racing against Apply.
func (n *Node) applyPendingSnapshot(ps *pendingSnapshot) {
	n.mu.RLock()
	sm := n.snapshotter
	snapPath := n.snapPath
	n.mu.RUnlock()

	if sm != nil && snapPath != "" {
		f, err := n.openSnapshotFile(snapPath)
		if err != nil {
			logx.Errorf("raft: open pending snapshot: %v\n", err)
			return
		}
		meta, payloadSize, err := verifySnapshotFile(f)
		if err != nil {
			_ = f.Close()
			logx.Errorf("raft: verify pending snapshot: %v\n", err)
			return
		}
		if meta.LastIncludedIndex < ps.index {
			_ = f.Close()
			return
		}
		if _, err := f.Seek(snapshotHeaderSize, io.SeekStart); err != nil {
			_ = f.Close()
			logx.Errorf("raft: seek pending snapshot: %v\n", err)
			return
		}
		err = sm.RestoreSnapshot(io.LimitReader(bufio.NewReader(f), payloadSize))
		_ = f.Close()
		if err != nil {
			logx.Errorf("raft: restore pending snapshot: %v\n", err)
			return
		}
		ps = &pendingSnapshot{index: meta.LastIncludedIndex, term: meta.LastIncludedTerm}
	}

	n.mu.Lock()
	if n.LastApplied < ps.index {
		n.setLastAppliedLocked(ps.index)
	}
	if n.pendingSnap != nil && n.pendingSnap.index <= ps.index {
		n.pendingSnap = nil
	}
	n.applyCond.Broadcast()
	n.mu.Unlock()
}

func (n *Node) openSnapshotFile(path string) (*os.File, error) {
	n.snapFileMu.Lock()
	defer n.snapFileMu.Unlock()
	return os.Open(path)
}

func (n *Node) abortSnapshotRecvLocked() {
	if n.snapRecv == nil {
		return
	}
	if n.snapRecv.file != nil {
		_ = n.snapRecv.file.Close()
	}
	if n.snapRecv.path != "" {
		_ = os.Remove(n.snapRecv.path)
	}
	n.snapRecv = nil
}

// HandleInstallSnapshot receives one chunk of a snapshot from the leader.
// Disk writes happen under snapRecvMu (never n.mu), and state-machine restore
// is handed off to the single RunApply goroutine.
func (n *Node) HandleInstallSnapshot(args InstallSnapshotArgs) (InstallSnapshotResult, error) {
	st := n.Status()
	if args.Term < st.CurrentTerm {
		return InstallSnapshotResult{Term: st.CurrentTerm}, nil
	}
	n.pokeElectionTimer()
	if args.Term > st.CurrentTerm {
		if err := n.SetPersistentState(args.Term, ""); err != nil {
			return InstallSnapshotResult{}, err
		}
	}

	n.mu.Lock()
	if n.CurrentTerm != args.Term {
		term := n.CurrentTerm
		n.mu.Unlock()
		return InstallSnapshotResult{Term: term}, nil
	}
	wasLeader := n.Role == Leader
	n.setMetaLocked(Follower, args.LeaderID)
	if wasLeader {
		n.cancelReadRound()
		n.applyCond.Broadcast()
		n.pokeElectionTimer()
	}
	snapPath := n.snapPath
	alreadyHave := args.LastIncludedIndex <= n.SnapshotIndex || args.LastIncludedIndex <= n.CommitIndex
	term := n.CurrentTerm
	n.mu.Unlock()

	if alreadyHave || args.LastIncludedIndex == 0 {
		n.pokeElectionTimer()
		return InstallSnapshotResult{Term: term}, nil
	}
	if snapPath == "" {
		return InstallSnapshotResult{}, errors.New("raft: snapshot path not configured")
	}

	n.snapRecvMu.Lock()
	defer n.snapRecvMu.Unlock()

	if args.Offset == 0 {
		n.abortSnapshotRecvLocked()
		dir := filepath.Dir(snapPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return InstallSnapshotResult{}, fmt.Errorf("create snapshot dir: %w", err)
		}
		tmp, err := os.CreateTemp(dir, ".raft-snap-recv-*")
		if err != nil {
			return InstallSnapshotResult{}, fmt.Errorf("create snapshot recv file: %w", err)
		}
		n.snapRecv = &snapshotReceiver{
			index: args.LastIncludedIndex,
			term:  args.LastIncludedTerm,
			file:  tmp,
			path:  tmp.Name(),
		}
	}

	recv := n.snapRecv
	if recv == nil || recv.index != args.LastIncludedIndex || recv.term != args.LastIncludedTerm {
		return InstallSnapshotResult{}, errors.New("raft: unexpected snapshot chunk")
	}
	if args.Offset < recv.offset {
		// Duplicate retry of a chunk already persisted.
		n.pokeElectionTimer()
		return InstallSnapshotResult{Term: n.Status().CurrentTerm}, nil
	}
	if args.Offset > recv.offset {
		n.abortSnapshotRecvLocked()
		return InstallSnapshotResult{}, fmt.Errorf("raft: snapshot offset gap got=%d want=%d", args.Offset, recv.offset)
	}

	if len(args.Data) > 0 {
		if _, err := recv.file.Write(args.Data); err != nil {
			n.abortSnapshotRecvLocked()
			return InstallSnapshotResult{}, fmt.Errorf("write snapshot chunk: %w", err)
		}
		recv.offset += uint64(len(args.Data))
	}

	if !args.Done {
		n.pokeElectionTimer()
		return InstallSnapshotResult{Term: n.Status().CurrentTerm}, nil
	}

	if err := recv.file.Sync(); err != nil {
		n.abortSnapshotRecvLocked()
		return InstallSnapshotResult{}, fmt.Errorf("sync received snapshot: %w", err)
	}
	meta, _, err := verifySnapshotFile(recv.file)
	if err != nil {
		n.abortSnapshotRecvLocked()
		return InstallSnapshotResult{}, fmt.Errorf("verify received snapshot: %w", err)
	}
	if meta.LastIncludedIndex != args.LastIncludedIndex || meta.LastIncludedTerm != args.LastIncludedTerm {
		n.abortSnapshotRecvLocked()
		return InstallSnapshotResult{}, fmt.Errorf("%w: header metadata mismatch", ErrCorruptSnapshot)
	}
	tmpPath := recv.path
	if err := recv.file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		n.snapRecv = nil
		return InstallSnapshotResult{}, fmt.Errorf("close received snapshot: %w", err)
	}
	n.snapRecv = nil

	n.mu.Lock()
	if n.CurrentTerm != args.Term || args.LastIncludedIndex <= n.SnapshotIndex || args.LastIncludedIndex <= n.CommitIndex {
		termNow := n.CurrentTerm
		n.mu.Unlock()
		_ = os.Remove(tmpPath)
		n.pokeElectionTimer()
		return InstallSnapshotResult{Term: termNow}, nil
	}

	n.snapFileMu.Lock()
	renameErr := os.Rename(tmpPath, snapPath)
	n.snapFileMu.Unlock()
	if renameErr != nil {
		n.mu.Unlock()
		_ = os.Remove(tmpPath)
		return InstallSnapshotResult{}, fmt.Errorf("install received snapshot: %w", renameErr)
	}

	// Raft §7: retain existing log suffix only if entry at LastIncludedIndex has
	// the same term; otherwise discard the entire log.
	haveTerm, hasEntry := n.logTermAtLocked(args.LastIncludedIndex)
	retainSuffix := hasEntry && haveTerm == args.LastIncludedTerm
	if retainSuffix {
		n.trimLogPrefixLocked(args.LastIncludedIndex)
	} else {
		n.Log = nil
	}

	n.SnapshotIndex = args.LastIncludedIndex
	n.SnapshotTerm = args.LastIncludedTerm
	if args.LastIncludedIndex > n.CommitIndex {
		n.setCommitLocked(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	if args.LastIncludedIndex > n.LastApplied {
		n.pendingSnap = &pendingSnapshot{
			index: args.LastIncludedIndex,
			term:  args.LastIncludedTerm,
		}
		n.applyCond.Broadcast()
	}
	walLog := n.logWAL
	termNow := n.CurrentTerm
	n.mu.Unlock()

	if err := syncDir(filepath.Dir(snapPath)); err != nil {
		return InstallSnapshotResult{}, fmt.Errorf("sync snapshot dir: %w", err)
	}
	if walLog != nil {
		if retainSuffix {
			_ = walLog.Compact(args.LastIncludedIndex)
		} else if err := walLog.ResetAfter(args.LastIncludedIndex); err != nil {
			return InstallSnapshotResult{}, fmt.Errorf("reset WAL after snapshot: %w", err)
		}
	}

	n.pokeElectionTimer()
	return InstallSnapshotResult{Term: termNow}, nil
}

// sendSnapshotToPeer streams the leader's persisted snapshot file to a lagging
// peer in chunks without holding n.mu.
func (n *Node) sendSnapshotToPeer(
	ctx context.Context,
	peer string,
	term uint64,
) (snapIndex uint64, ok bool, steppedDown bool) {
	rpc := n.getInstallSnapshotRPC()
	if rpc == nil {
		return 0, false, false
	}

	n.mu.RLock()
	if n.Role != Leader || n.CurrentTerm != term || n.snapPath == "" || n.SnapshotIndex == 0 {
		n.mu.RUnlock()
		return 0, false, false
	}
	snapPath := n.snapPath
	leaderID := n.ID
	n.mu.RUnlock()

	f, err := n.openSnapshotFile(snapPath)
	if err != nil {
		return 0, false, false
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || info.Size() < snapshotHeaderSize+snapshotFooterSize {
		return 0, false, false
	}
	var hdr [snapshotHeaderSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return 0, false, false
	}
	meta, err := decodeSnapshotHeader(hdr[:])
	if err != nil {
		return 0, false, false
	}

	totalSize := info.Size()
	buf := make([]byte, snapshotChunkBytes)
	var offset int64
	start := time.Now()

	for offset < totalSize {
		if ctx.Err() != nil || !n.isLeadingTerm(term) {
			return 0, false, false
		}
		toRead := totalSize - offset
		if toRead > int64(len(buf)) {
			toRead = int64(len(buf))
		}
		nRead, err := f.ReadAt(buf[:toRead], offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, false, false
		}
		done := offset+int64(nRead) >= totalSize

		chunk := make([]byte, nRead)
		copy(chunk, buf[:nRead])
		res, err := rpc(ctx, peer, InstallSnapshotArgs{
			Term:              term,
			LeaderID:          leaderID,
			LastIncludedIndex: meta.LastIncludedIndex,
			LastIncludedTerm:  meta.LastIncludedTerm,
			Offset:            uint64(offset),
			Data:              chunk,
			Done:              done,
		})
		if err != nil {
			return 0, false, false
		}
		if res.Term > term {
			_ = n.stepDown(res.Term)
			return 0, false, true
		}
		offset += int64(nRead)

		if !done && snapshotMaxBytesPerSec > 0 {
			want := time.Duration(float64(offset) / float64(snapshotMaxBytesPerSec) * float64(time.Second))
			if delay := want - time.Since(start); delay > time.Millisecond {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return 0, false, false
				case <-timer.C:
				}
			}
		}
	}

	n.mu.Lock()
	if n.Role == Leader && n.CurrentTerm == term {
		if meta.LastIncludedIndex+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = meta.LastIncludedIndex + 1
		}
		if meta.LastIncludedIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = meta.LastIncludedIndex
			n.advanceCommitLocked()
		}
	}
	n.mu.Unlock()
	return meta.LastIncludedIndex, true, false
}
