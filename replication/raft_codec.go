package replication

import (
	"bytes"
	"io"
	"kvStore/raft"
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
