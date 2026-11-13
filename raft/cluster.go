package raft

import "fmt"

func (n *Node) SetPeers(peerIds []string) error {
	seen := map[string]struct{}{
		n.ID: {},
	}
	for _, peerId := range peerIds {
		if peerId == "" {
			return fmt.Errorf("peer ID cannot be empty")
		}
		if _, ok := seen[peerId]; ok {
			return fmt.Errorf("duplicate peer ID: %s", peerId)
		}
		seen[peerId] = struct{}{}
	}

	wakes := make(map[string]chan struct{}, len(peerIds))
	for _, id := range peerIds {
		wakes[id] = make(chan struct{}, 1)
	}

	n.mu.Lock()
	n.peers = append([]string(nil), peerIds...)
	n.peerWake = wakes
	n.mu.Unlock()
	return nil
}

func (n *Node) quorumLocked() int {
	size := len(n.peers) + 1 // +1 for the current node
	return size/2 + 1
}
