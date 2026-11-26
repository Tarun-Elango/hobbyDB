package replication

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"kvStore/proto"
	"kvStore/raft"
	"kvStore/server"
	"kvStore/store"
)

func TestInstallSnapshotCodecRoundTrip(t *testing.T) {
	want := raft.InstallSnapshotArgs{
		Term:              7,
		LeaderID:          "node-a",
		LastIncludedIndex: 1234,
		LastIncludedTerm:  6,
		Offset:            512,
		Data:              []byte("snapshot-chunk-bytes"),
		Done:              true,
	}
	var buf bytes.Buffer
	if err := EncodeInstallSnapshot(&buf, want); err != nil {
		t.Fatalf("encode: %v", err)
	}
	frame, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if frame.messageType != messageInstallSnapshot {
		t.Fatalf("messageType = %d, want %d", frame.messageType, messageInstallSnapshot)
	}
	got, err := DecodeInstallSnapshotPayload(frame.payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Term != want.Term || got.LeaderID != want.LeaderID ||
		got.LastIncludedIndex != want.LastIncludedIndex || got.LastIncludedTerm != want.LastIncludedTerm ||
		got.Offset != want.Offset || got.Done != want.Done || !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	var resBuf bytes.Buffer
	if err := EncodeInstallSnapshotResult(&resBuf, raft.InstallSnapshotResult{Term: 9}); err != nil {
		t.Fatal(err)
	}
	resFrame, err := readFrame(&resBuf)
	if err != nil {
		t.Fatal(err)
	}
	gotRes, err := DecodeInstallSnapshotResultPayload(resFrame.payload)
	if err != nil {
		t.Fatal(err)
	}
	if gotRes.Term != 9 {
		t.Fatalf("result term = %d, want 9", gotRes.Term)
	}
}

func TestInstallSnapshotEndToEndOverTCP(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Boot a 2-node cluster over real TCP where leader compacts its log before
	// follower starts listening, then follower boots and catches up via TCP InstallSnapshot.
	lStore := store.NewStore[string, []byte]()
	lSM := server.NewStateMachine(lStore)
	leader, err := raft.OpenNode("leader", filepath.Join(dir, "leader.state"))
	if err != nil {
		t.Fatal(err)
	}
	defer leader.CloseLog()
	if err := leader.OpenSnapshot(filepath.Join(dir, "leader.snap"), lSM); err != nil {
		t.Fatal(err)
	}
	if err := leader.OpenLog(filepath.Join(dir, "leader.log")); err != nil {
		t.Fatal(err)
	}
	leader.SetSnapshotConfig(5, 0, 0)

	// Make leader single-node first so it commits and compacts 10 entries, then add follower.
	if err := leader.SetPeers(nil); err != nil {
		t.Fatal(err)
	}
	go leader.Run(ctx, func(context.Context, string, raft.RequestVoteArgs) (raft.RequestVoteResult, error) {
		return raft.RequestVoteResult{}, nil
	})
	go leader.RunApply(ctx, lSM.Apply)

	deadline := time.Now().Add(3 * time.Second)
	for leader.Status().Role != raft.Leader && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if leader.Status().Role != raft.Leader {
		t.Fatal("leader never elected")
	}

	for i := 1; i <= 10; i++ {
		if _, err := leader.Propose(ctx, proto.Command{
			Op:    proto.OpSet,
			Key:   []byte(fmt.Sprintf("k-%d", i)),
			Value: []byte(fmt.Sprintf("v-%d", i)),
		}); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	deadline = time.Now().Add(3 * time.Second)
	for leader.Snapshot().SnapshotIndex < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if leader.Snapshot().SnapshotIndex < 5 {
		t.Fatalf("leader never snapshotted: %+v", leader.Snapshot())
	}

	// Now boot a fresh empty follower over real TCP.
	fStore := store.NewStore[string, []byte]()
	fSM := server.NewStateMachine(fStore)
	follower, err := raft.OpenNode("follower", filepath.Join(dir, "follower.state"))
	if err != nil {
		t.Fatal(err)
	}
	defer follower.CloseLog()
	if err := follower.OpenSnapshot(filepath.Join(dir, "follower.snap"), fSM); err != nil {
		t.Fatal(err)
	}
	if err := follower.OpenLog(filepath.Join(dir, "follower.log")); err != nil {
		t.Fatal(err)
	}
	go follower.RunApply(ctx, fSM.Apply)

	replSrv, err := NewReplicationServer(ctx, "127.0.0.1:0", follower)
	if err != nil {
		t.Fatal(err)
	}
	defer replSrv.Close()
	go func() { _ = replSrv.Serve() }()

	peerSet, err := NewPeerSet([]string{"follower=" + replSrv.listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer peerSet.Close()

	// Directly register follower and stream snapshot via real TCP PeerSet.InstallSnapshot.
	if err := leader.SetPeers(peerSet.IDs()); err != nil {
		t.Fatal(err)
	}
	leader.SetInstallSnapshotRPC(peerSet.InstallSnapshot)
	go leader.RunReplicationPipelined(ctx, peerSet.AppendPipeline)

	// Propose one more entry (needs follower quorum now!) — forces leader to
	// send InstallSnapshot over TCP and then replicate the new entry.
	if _, err := leader.Propose(ctx, proto.Command{
		Op:    proto.OpSet,
		Key:   []byte("after-tcp-snap"),
		Value: []byte("yes"),
	}); err != nil {
		t.Fatalf("propose after follower joined: %v", err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := fStore.Get("after-tcp-snap"); ok && string(v) == "yes" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if v, ok := fStore.Get("after-tcp-snap"); !ok || string(v) != "yes" {
		t.Fatalf("follower missing after-tcp-snap: %q (%v)", v, ok)
	}
	for i := 1; i <= 10; i++ {
		if v, ok := fStore.Get(fmt.Sprintf("k-%d", i)); !ok || string(v) != fmt.Sprintf("v-%d", i) {
			t.Fatalf("follower k-%d = %q (%v), want v-%d", i, v, ok, i)
		}
	}
}
