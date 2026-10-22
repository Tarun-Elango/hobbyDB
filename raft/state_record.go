package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const stateFormatVersion byte = 1

// MaxVotedForLen matches replication leader/candidate ID limits.
const MaxVotedForLen = 256

var (
	ErrUnsupportedStateVersion = errors.New("raft: unsupported persistent state version")
	ErrVotedForTooLong         = errors.New("raft: voted_for exceeds max length")
	ErrMalformedState          = errors.New("raft: malformed persistent state")
)

// On-disk layout (big-endian):
//
//	[version: 1][current_term: 8][voted_for_len: 2][voted_for: len]
func EncodePersistentState(state PersistentState) ([]byte, error) {
	if len(state.VotedFor) > MaxVotedForLen {
		return nil, ErrVotedForTooLong
	}

	buf := new(bytes.Buffer)
	if err := buf.WriteByte(stateFormatVersion); err != nil {
		return nil, err
	}
	if err := writeUint64(buf, state.CurrentTerm); err != nil {
		return nil, err
	}
	if err := writeUint16(buf, uint16(len(state.VotedFor))); err != nil {
		return nil, err
	}
	if state.VotedFor != "" {
		if _, err := buf.WriteString(state.VotedFor); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func DecodePersistentState(data []byte) (PersistentState, error) {
	if len(data) < 1+8+2 {
		return PersistentState{}, fmt.Errorf("%w: file too short", ErrMalformedState)
	}

	r := bytes.NewReader(data)
	version, err := r.ReadByte()
	if err != nil {
		return PersistentState{}, fmt.Errorf("%w: %v", ErrMalformedState, err)
	}
	if version != stateFormatVersion {
		return PersistentState{}, fmt.Errorf("%w: got %d", ErrUnsupportedStateVersion, version)
	}

	term, err := readUint64(r)
	if err != nil {
		return PersistentState{}, fmt.Errorf("%w: %v", ErrMalformedState, err)
	}

	votedForLen, err := readUint16(r)
	if err != nil {
		return PersistentState{}, fmt.Errorf("%w: %v", ErrMalformedState, err)
	}
	if votedForLen > MaxVotedForLen {
		return PersistentState{}, ErrVotedForTooLong
	}

	votedFor := make([]byte, votedForLen)
	if votedForLen > 0 {
		if _, err := io.ReadFull(r, votedFor); err != nil {
			return PersistentState{}, fmt.Errorf("%w: %v", ErrMalformedState, err)
		}
	}
	if r.Len() != 0 {
		return PersistentState{}, fmt.Errorf("%w: trailing bytes", ErrMalformedState)
	}

	return PersistentState{
		CurrentTerm: term,
		VotedFor:    string(votedFor),
	}, nil
}

func writeUint16(w io.Writer, v uint16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func readUint16(r io.Reader) (uint16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(buf[:]), nil
}

func writeUint64(w io.Writer, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func readUint64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buf[:]), nil
}
