package replication

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"kvStore/proto"
	"kvStore/raft"
	"math"
)

func EncodeRequestVote(w io.Writer, args raft.RequestVoteArgs) error {
	var payload bytes.Buffer
	if err := writeU64(&payload, args.Term); err != nil {
		return err
	}
	if err := writeU16(&payload, uint16(len(args.CandidateID))); err != nil {
		return err
	}
	if err := writeFull(&payload, []byte(args.CandidateID)); err != nil {
		return err
	}
	if err := writeU64(&payload, args.LastLogIndex); err != nil {
		return err
	}
	if err := writeU64(&payload, args.LastLogTerm); err != nil {
		return err
	}
	return writeFrame(w, messageRequestVote, payload.Bytes())

}

func DecodeRequestVotePayload(payload []byte) (raft.RequestVoteArgs, error) {
	r := bytes.NewReader(payload)
	term, err := readU64(r)
	if err != nil {
		return raft.RequestVoteArgs{}, err
	}

	idLen, err := readU16(r)
	if err != nil {
		return raft.RequestVoteArgs{}, err
	}
	id, err := readBytes(r, uint32(idLen))
	if err != nil {
		return raft.RequestVoteArgs{}, err
	}
	lastIndex, err := readU64(r)
	if err != nil {
		return raft.RequestVoteArgs{}, err
	}
	lastTerm, err := readU64(r)
	if err != nil {
		return raft.RequestVoteArgs{}, err
	}

	return raft.RequestVoteArgs{
		Term:         term,
		CandidateID:  string(id),
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}, nil
}

func EncodeRequestVoteResult(w io.Writer, result raft.RequestVoteResult) error {
	var payload bytes.Buffer
	if err := writeU64(&payload, result.Term); err != nil {
		return err
	}
	granted := byte(0)
	if result.VoteGranted {
		granted = 1
	}

	if err := writeU8(&payload, granted); err != nil {
		return err
	}
	return writeFrame(w, messageRequestVoteRes, payload.Bytes())
}

func DecodeRequestVoteResultPayload(payload []byte) (raft.RequestVoteResult, error) {
	r := bytes.NewReader(payload)
	term, err := readU64(r) // read 8 bytes for term
	if err != nil {
		return raft.RequestVoteResult{}, err
	}
	granted, err := readU8(r) // read 1 byte for granted
	if err != nil {
		return raft.RequestVoteResult{}, err
	}

	return raft.RequestVoteResult{
		Term:        term,
		VoteGranted: granted == 1,
	}, nil
}

func EncodeHeartbeat(w io.Writer, args raft.HeartbeatArgs) error {
	var payload bytes.Buffer
	if err := writeU64(&payload, args.Term); err != nil {
		return err
	}
	if err := writeU16(&payload, uint16(len(args.LeaderID))); err != nil {
		return err
	}
	if err := writeFull(&payload, []byte(args.LeaderID)); err != nil {
		return err
	}

	return writeFrame(w, messageHeartbeat, payload.Bytes())
}

func DecodeHeartbeatPayload(payload []byte) (raft.HeartbeatArgs, error) {
	r := bytes.NewReader(payload)
	term, err := readU64(r)
	if err != nil {
		return raft.HeartbeatArgs{}, err
	}
	idLen, err := readU16(r)
	if err != nil {
		return raft.HeartbeatArgs{}, err
	}
	id, err := readBytes(r, uint32(idLen))
	if err != nil {
		return raft.HeartbeatArgs{}, err
	}
	return raft.HeartbeatArgs{
		Term:     term,
		LeaderID: string(id)}, nil
}

func EncodeHeartbeatResult(w io.Writer, result raft.HeartbeatResult) error {
	var payload bytes.Buffer
	if err := writeU64(&payload, result.Term); err != nil {
		return err
	}
	return writeFrame(w, messageHeartbeatRes, payload.Bytes())
}

func DecodeHeartbeatResultPayload(payload []byte) (raft.HeartbeatResult, error) {
	r := bytes.NewReader(payload)
	term, err := readU64(r)
	if err != nil {
		return raft.HeartbeatResult{}, err
	}
	return raft.HeartbeatResult{
		Term: term,
	}, nil
}

func EncodeRaftAppendEntries(w io.Writer, args raft.AppendEntriesArgs) error {
	if err := validateRaftAppendEntries(args); err != nil {
		return err
	}
	var payload bytes.Buffer
	if err := writeU64(&payload, args.Term); err != nil {
		return err
	}
	if err := writeU16(&payload, uint16(len(args.LeaderID))); err != nil {
		return err
	}
	if err := writeFull(&payload, []byte(args.LeaderID)); err != nil {
		return err
	}
	if err := writeU64(&payload, args.PrevLogIndex); err != nil {
		return err
	}
	if err := writeU64(&payload, args.PrevLogTerm); err != nil {
		return err
	}
	if err := writeU64(&payload, args.LeaderCommit); err != nil {
		return err
	}
	if err := writeU32(&payload, uint32(len(args.Entries))); err != nil {
		return err
	}
	for _, entry := range args.Entries {
		if err := encodeRaftLogEntry(&payload, entry); err != nil {
			return err
		}
	}
	if payload.Len() > maxAppendRequestPayload {
		return fmt.Errorf(
			"replication: append entries payload is %d bytes, max %d",
			payload.Len(),
			maxAppendRequestPayload,
		)
	}
	return writeFrame(w, messageRaftAppendEntries, payload.Bytes())
}

func DecodeRaftAppendEntriesPayload(payload []byte) (raft.AppendEntriesArgs, error) {
	r := bytes.NewReader(payload)
	term, err := readU64(r)
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	idLen, err := readU16(r)
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	if idLen == 0 || int(idLen) > maxLeaderIDLength {
		return raft.AppendEntriesArgs{}, errors.New("replication: invalid leader ID length")
	}
	id, err := readBytes(r, uint32(idLen))
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	prevIndex, err := readU64(r)
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	prevTerm, err := readU64(r)
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	leaderCommit, err := readU64(r)
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	count, err := readU32(r)
	if err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	if int(count) > maxEntriesPerRequest {
		return raft.AppendEntriesArgs{}, fmt.Errorf("replication: too many entries: %d", count)
	}
	entries := make([]raft.LogEntry, count)
	for i := range entries {
		entry, err := decodeRaftLogEntry(r)
		if err != nil {
			return raft.AppendEntriesArgs{}, err
		}
		entries[i] = entry
	}
	if r.Len() != 0 {
		return raft.AppendEntriesArgs{}, errors.New("replication: trailing append entries bytes")
	}
	args := raft.AppendEntriesArgs{
		Term:         term,
		LeaderID:     string(id),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	if err := validateRaftAppendEntries(args); err != nil {
		return raft.AppendEntriesArgs{}, err
	}
	return args, nil
}

func EncodeRaftAppendEntriesResult(w io.Writer, result raft.AppendEntriesResult) error {
	var payload bytes.Buffer
	if err := writeU64(&payload, result.Term); err != nil {
		return err
	}
	success := byte(0)
	if result.Success {
		success = 1
	}
	if err := writeU8(&payload, success); err != nil {
		return err
	}
	return writeFrame(w, messageRaftAppendEntriesRes, payload.Bytes())
}

func DecodeRaftAppendEntriesResultPayload(payload []byte) (raft.AppendEntriesResult, error) {
	r := bytes.NewReader(payload)
	term, err := readU64(r)
	if err != nil {
		return raft.AppendEntriesResult{}, err
	}
	success, err := readU8(r)
	if err != nil {
		return raft.AppendEntriesResult{}, err
	}
	if success > 1 {
		return raft.AppendEntriesResult{}, fmt.Errorf("replication: invalid append success byte %d", success)
	}
	if r.Len() != 0 {
		return raft.AppendEntriesResult{}, errors.New("replication: trailing append result bytes")
	}
	return raft.AppendEntriesResult{
		Term:    term,
		Success: success == 1,
	}, nil
}

func encodeRaftLogEntry(w io.Writer, entry raft.LogEntry) error {
	if err := writeU64(w, entry.Index); err != nil {
		return err
	}
	if err := writeU64(w, entry.Term); err != nil {
		return err
	}
	if err := writeU8(w, entry.Command.Op); err != nil {
		return err
	}
	if err := writeU32(w, uint32(len(entry.Command.Key))); err != nil {
		return err
	}
	if err := writeFull(w, entry.Command.Key); err != nil {
		return err
	}
	if err := writeU32(w, uint32(len(entry.Command.Value))); err != nil {
		return err
	}
	if err := writeFull(w, entry.Command.Value); err != nil {
		return err
	}
	if err := writeU64(w, entry.Command.ClientID); err != nil {
		return err
	}
	return writeU64(w, entry.Command.Seq)
}

func decodeRaftLogEntry(r io.Reader) (raft.LogEntry, error) {
	index, err := readU64(r)
	if err != nil {
		return raft.LogEntry{}, err
	}
	term, err := readU64(r)
	if err != nil {
		return raft.LogEntry{}, err
	}
	op, err := readU8(r)
	if err != nil {
		return raft.LogEntry{}, err
	}
	keyLen, err := readU32(r)
	if err != nil {
		return raft.LogEntry{}, err
	}
	if int(keyLen) > proto.MaxKeyLen {
		return raft.LogEntry{}, fmt.Errorf("replication: key is too large: %d", keyLen)
	}
	key, err := readBytes(r, keyLen)
	if err != nil {
		return raft.LogEntry{}, err
	}
	valueLen, err := readU32(r)
	if err != nil {
		return raft.LogEntry{}, err
	}
	if int(valueLen) > proto.MaxValueLen {
		return raft.LogEntry{}, fmt.Errorf("replication: value is too large: %d", valueLen)
	}
	value, err := readBytes(r, valueLen)
	if err != nil {
		return raft.LogEntry{}, err
	}
	clientID, err := readU64(r)
	if err != nil {
		return raft.LogEntry{}, err
	}
	seq, err := readU64(r)
	if err != nil {
		return raft.LogEntry{}, err
	}
	return raft.LogEntry{
		Index: index,
		Term:  term,
		Command: proto.Command{
			Op:       op,
			Key:      key,
			Value:    value,
			ClientID: clientID,
			Seq:      seq,
		},
	}, nil
}

func validateRaftAppendEntries(args raft.AppendEntriesArgs) error {
	if args.LeaderID == "" {
		return errors.New("replication: leader ID is required")
	}
	if len(args.LeaderID) > maxLeaderIDLength {
		return errors.New("replication: leader ID is too long")
	}
	if len(args.Entries) > maxEntriesPerRequest {
		return fmt.Errorf("replication: too many entries: %d", len(args.Entries))
	}
	if args.PrevLogIndex == math.MaxUint64 && len(args.Entries) > 0 {
		return errors.New("replication: entry index overflow")
	}
	expect := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if entry.Index != expect {
			return fmt.Errorf("replication: entry %d has index %d, want %d", i, entry.Index, expect)
		}
		expect++
		if entry.Index == 0 {
			return errors.New("replication: log index must be greater than zero")
		}
		if len(entry.Command.Key) > proto.MaxKeyLen {
			return errors.New("replication: key is too large")
		}
		if len(entry.Command.Value) > proto.MaxValueLen {
			return errors.New("replication: value is too large")
		}
	}
	return nil
}

func EncodeInstallSnapshot(w io.Writer, args raft.InstallSnapshotArgs) error {
	if args.LeaderID == "" || len(args.LeaderID) > maxLeaderIDLength {
		return errors.New("replication: invalid leader ID length")
	}
	var payload bytes.Buffer
	if err := writeU64(&payload, args.Term); err != nil {
		return err
	}
	if err := writeU16(&payload, uint16(len(args.LeaderID))); err != nil {
		return err
	}
	if err := writeFull(&payload, []byte(args.LeaderID)); err != nil {
		return err
	}
	if err := writeU64(&payload, args.LastIncludedIndex); err != nil {
		return err
	}
	if err := writeU64(&payload, args.LastIncludedTerm); err != nil {
		return err
	}
	if err := writeU64(&payload, args.Offset); err != nil {
		return err
	}
	done := byte(0)
	if args.Done {
		done = 1
	}
	if err := writeU8(&payload, done); err != nil {
		return err
	}
	if err := writeU32(&payload, uint32(len(args.Data))); err != nil {
		return err
	}
	if err := writeFull(&payload, args.Data); err != nil {
		return err
	}
	if payload.Len() > maxAppendRequestPayload {
		return fmt.Errorf("replication: install snapshot payload is %d bytes, max %d", payload.Len(), maxAppendRequestPayload)
	}
	return writeFrame(w, messageInstallSnapshot, payload.Bytes())
}

func DecodeInstallSnapshotPayload(payload []byte) (raft.InstallSnapshotArgs, error) {
	r := bytes.NewReader(payload)
	term, err := readU64(r)
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	idLen, err := readU16(r)
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if idLen == 0 || int(idLen) > maxLeaderIDLength {
		return raft.InstallSnapshotArgs{}, errors.New("replication: invalid leader ID length")
	}
	id, err := readBytes(r, uint32(idLen))
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	lastIdx, err := readU64(r)
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	lastTerm, err := readU64(r)
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	offset, err := readU64(r)
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	done, err := readU8(r)
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if done > 1 {
		return raft.InstallSnapshotArgs{}, fmt.Errorf("replication: invalid snapshot done byte %d", done)
	}
	dataLen, err := readU32(r)
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if int(dataLen) > maxAppendRequestPayload {
		return raft.InstallSnapshotArgs{}, fmt.Errorf("replication: snapshot chunk too large: %d", dataLen)
	}
	data, err := readBytes(r, dataLen)
	if err != nil {
		return raft.InstallSnapshotArgs{}, err
	}
	if r.Len() != 0 {
		return raft.InstallSnapshotArgs{}, errors.New("replication: trailing install snapshot bytes")
	}
	return raft.InstallSnapshotArgs{
		Term:              term,
		LeaderID:          string(id),
		LastIncludedIndex: lastIdx,
		LastIncludedTerm:  lastTerm,
		Offset:            offset,
		Data:              data,
		Done:              done == 1,
	}, nil
}

func EncodeInstallSnapshotResult(w io.Writer, result raft.InstallSnapshotResult) error {
	var payload bytes.Buffer
	if err := writeU64(&payload, result.Term); err != nil {
		return err
	}
	return writeFrame(w, messageInstallSnapshotRes, payload.Bytes())
}

func DecodeInstallSnapshotResultPayload(payload []byte) (raft.InstallSnapshotResult, error) {
	r := bytes.NewReader(payload)
	term, err := readU64(r)
	if err != nil {
		return raft.InstallSnapshotResult{}, err
	}
	if r.Len() != 0 {
		return raft.InstallSnapshotResult{}, errors.New("replication: trailing install snapshot result bytes")
	}
	return raft.InstallSnapshotResult{Term: term}, nil
}
