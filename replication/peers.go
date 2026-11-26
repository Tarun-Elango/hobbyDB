package replication

import (
	"context"
	"fmt"
	"kvStore/raft"
	"strings"
)

type peerClients struct {
	control  *TCPPeerClient     // Heartbeat and RequestVote
	append   *TCPPeerClient     // single-RPC AppendEntries
	pipeline *TCPAppendPipeline // pipelined FIFO AppendEntries stream
}

type PeerSet struct {
	ids   []string
	peers map[string]*peerClients
}

// create a new peer set from a list of specs
func NewPeerSet(specs []string) (*PeerSet, error) {
	set := &PeerSet{peers: make(map[string]*peerClients)}
	for _, spec := range specs {
		id, addr, ok := strings.Cut(spec, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("peer must look like id=host:port, got %q", spec)
		}
		if _, exists := set.peers[id]; exists {
			return nil, fmt.Errorf("duplicate peer id %s", id)
		}
		set.ids = append(set.ids, id)
		set.peers[id] = &peerClients{
			control:  NewTCPPeerClient(addr),
			append:   NewTCPPeerClient(addr),
			pipeline: NewTCPAppendPipeline(addr),
		}
	}
	return set, nil
}

func (p *PeerSet) IDs() []string {
	return append([]string(nil), p.ids...) // copy the slice and return a new slice
}

func (p *PeerSet) Close() error {
	if p == nil {
		return nil
	}
	var err error
	for _, peer := range p.peers {
		if e := peer.control.Close(); e != nil {
			err = e
		}
		if e := peer.append.Close(); e != nil {
			err = e
		}
		if e := peer.pipeline.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (p *PeerSet) RequestVote(
	ctx context.Context,
	peerID string,
	args raft.RequestVoteArgs,
) (raft.RequestVoteResult, error) {
	peer := p.peers[peerID]
	if peer == nil {
		return raft.RequestVoteResult{}, fmt.Errorf("replication: unknown peer %s", peerID)
	}
	return peer.control.RequestVote(ctx, args)
}

func (p *PeerSet) AppendEntries(
	ctx context.Context,
	peerID string,
	args raft.AppendEntriesArgs,
) (raft.AppendEntriesResult, error) {
	peer := p.peers[peerID]
	if peer == nil {
		return raft.AppendEntriesResult{}, fmt.Errorf("replication: unknown peer %s", peerID)
	}
	return peer.append.AppendEntries(ctx, args)
}

func (p *PeerSet) AppendPipeline(peerID string) (raft.AppendPipeline, error) {
	peer := p.peers[peerID]
	if peer == nil {
		return nil, fmt.Errorf("replication: unknown peer %s", peerID)
	}
	return peer.pipeline, nil
}

func (p *PeerSet) Heartbeat(
	ctx context.Context,
	peerID string,
	args raft.HeartbeatArgs,
) (raft.HeartbeatResult, error) {
	peer := p.peers[peerID]
	if peer == nil {
		return raft.HeartbeatResult{}, fmt.Errorf("replication: unknown peer %s", peerID)
	}
	return peer.control.Heartbeat(ctx, args)
}

func (p *PeerSet) InstallSnapshot(
	ctx context.Context,
	peerID string,
	args raft.InstallSnapshotArgs,
) (raft.InstallSnapshotResult, error) {
	peer := p.peers[peerID]
	if peer == nil {
		return raft.InstallSnapshotResult{}, fmt.Errorf("replication: unknown peer %s", peerID)
	}
	return peer.append.InstallSnapshot(ctx, args)
}
