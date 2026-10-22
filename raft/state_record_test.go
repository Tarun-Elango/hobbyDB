package raft

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeDecodePersistentStateRoundTrip(t *testing.T) {
	want := PersistentState{
		CurrentTerm: 3,
		VotedFor:    "node-b",
	}
	data, err := EncodePersistentState(want)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodePersistentState(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestEncodeDecodePersistentStateEmptyVote(t *testing.T) {
	want := PersistentState{CurrentTerm: 0, VotedFor: ""}
	data, err := EncodePersistentState(want)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodePersistentState(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestDecodePersistentStateRejectsTrailingBytes(t *testing.T) {
	data, err := EncodePersistentState(PersistentState{CurrentTerm: 1})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	data = append(data, 0xFF)
	if _, err := DecodePersistentState(data); err == nil {
		t.Fatal("expected error for trailing bytes")
	}
}

func TestDecodePersistentStateUnsupportedVersion(t *testing.T) {
	data := []byte{99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	_, err := DecodePersistentState(data)
	if !errors.Is(err, ErrUnsupportedStateVersion) {
		t.Fatalf("got %v, want %v", err, ErrUnsupportedStateVersion)
	}
}

func TestEncodePersistentStateVotedForTooLong(t *testing.T) {
	_, err := EncodePersistentState(PersistentState{
		CurrentTerm: 1,
		VotedFor:    string(bytes.Repeat([]byte{'x'}, MaxVotedForLen+1)),
	})
	if err != ErrVotedForTooLong {
		t.Fatalf("got %v, want %v", err, ErrVotedForTooLong)
	}
}
