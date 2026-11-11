package replication

import (
	"context"
	"fmt"
	"kvStore/raft"
	"strings"
)

type PeerSet struct {
	ids     []string
	clients map[string]*TCPPeerClient
}

// create a new peer set from a list of specs
func NewPeerSet(specs []string) (*PeerSet, error) {
	set := &PeerSet{clients: make(map[string]*TCPPeerClient)}
	for _, spec := range specs {
		id, addr, ok := strings.Cut(spec, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("peer must look like id=host:port, got %q", spec)
		}
		if _, exists := set.clients[id]; exists {
			return nil, fmt.Errorf("duplicate peer id %s", id)
		}
		set.ids = append(set.ids, id)
		set.clients[id] = NewTCPPeerClient(addr)
	}
	return set, nil
}

func (p *PeerSet) IDs() []string {
	return append([]string(nil), p.ids...) // copy the slice and return a new slice
}

func (p *PeerSet) Close() error {
	var err error
	for _, client := range p.clients {
		if e := client.Close(); e != nil {
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
	clients := p.clients[peerID]
	if clients == nil {
		return raft.RequestVoteResult{}, fmt.Errorf("replication: unknown peer %s", peerID)
	}
	return clients.RequestVote(ctx, args)
}

func (p *PeerSet) Heartbeat(
	ctx context.Context,
	peerID string,
	args raft.HeartbeatArgs,
) (raft.HeartbeatResult, error) {
	clients := p.clients[peerID]
	if clients == nil {
		return raft.HeartbeatResult{}, fmt.Errorf("replication: unknown peer %s", peerID)
	}
	return clients.Heartbeat(ctx, args)
}
