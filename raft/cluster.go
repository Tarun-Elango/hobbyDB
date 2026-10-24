package raft

import "fmt"

func (n *Node) SetPeers(peerIds []string) error {

	// use to check for duplicate
	seen := map[string]struct{}{
		n.ID: {}, // key string and value struct{}, init with n.ID and empty struct
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

	n.mu.Lock()
	n.peers = append([]string(nil), peerIds...) // create a copy of the peer IDs, and assign
	n.mu.Unlock()
	return nil
}

func (n *Node) quorumLocked() int {
	size := len(n.peers) + 1 // +1 for the current node
	return size/2 + 1
}
