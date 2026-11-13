package replication

import (
	"bytes"
	"testing"

	"kvStore/proto"
	"kvStore/raft"
)

func TestRaftAppendEntriesRoundTrip(t *testing.T) {
	args := raft.AppendEntriesArgs{
		Term:         4,
		LeaderID:     "node-a",
		PrevLogIndex: 2,
		PrevLogTerm:  3,
		LeaderCommit: 2,
		Entries: []raft.LogEntry{{
			Index: 3,
			Term:  4,
			Command: proto.Command{
				Op:    proto.OpSet,
				Key:   []byte("user"),
				Value: []byte("ada"),
			},
		}},
	}

	var buf bytes.Buffer
	if err := EncodeRaftAppendEntries(&buf, args); err != nil {
		t.Fatal(err)
	}
	frame, err := readFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if frame.messageType != messageRaftAppendEntries {
		t.Fatalf("message type = %d", frame.messageType)
	}
	got, err := DecodeRaftAppendEntriesPayload(frame.payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.Term != args.Term || got.LeaderID != args.LeaderID || got.PrevLogIndex != args.PrevLogIndex || got.PrevLogTerm != args.PrevLogTerm || got.LeaderCommit != args.LeaderCommit {
		t.Fatalf("header = %+v", got)
	}
	if len(got.Entries) != 1 || string(got.Entries[0].Command.Value) != "ada" || got.Entries[0].Term != 4 {
		t.Fatalf("entries = %+v", got.Entries)
	}

	buf.Reset()
	want := raft.AppendEntriesResult{Term: 4, Success: true}
	if err := EncodeRaftAppendEntriesResult(&buf, want); err != nil {
		t.Fatal(err)
	}
	frame, err = readFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	result, err := DecodeRaftAppendEntriesResultPayload(frame.payload)
	if err != nil {
		t.Fatal(err)
	}
	if result != want {
		t.Fatalf("result = %+v", result)
	}
}
