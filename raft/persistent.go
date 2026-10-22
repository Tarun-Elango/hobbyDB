// remember a nodes raft state
// on-disk layout (*.state, binary):
//   [version: 1][current_term: 8][voted_for_len: 2][voted_for: len]

package raft

import (
	"fmt"
	"os"
	"path/filepath"
)

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
}

// restores term and vote from disk, or creates state file
func OpenNode(id, path string) (*Node, error) {
	node := NewNode(id)
	node.statePath = path

	data, err := os.ReadFile(path)
	if err == nil {
		state, err := DecodePersistentState(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode persistent state: %w", err)
		}
		node.CurrentTerm = state.CurrentTerm
		node.VotedFor = state.VotedFor
		return node, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read persistent state: %w", err)
	}
	if err := writePersistentState(path, PersistentState{}); err != nil {
		return nil, err
	}

	return node, nil
}

// write temp file, then rename it over the state file
func writePersistentState(path string, state PersistentState) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	data, err := EncodePersistentState(state)
	if err != nil {
		return fmt.Errorf("failed to encode persistent state: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".raft-state-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("failed to write persistent state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("failed to sync persistent state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close persistent state: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename persistent state: %w", err)
	}

	// flush the dir change caused by the above os.rename
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open directory: %w", err)
	}
	defer dirFile.Close()

	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}
	return nil
}

// wrties and syncs state before chaning the in memory copy
// call when raft advacnes terms/records a vote
func (n *Node) SetPersistentState(term uint64, votedFor string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.statePath == "" {
		return fmt.Errorf("state path is not set")
	}

	if term < n.CurrentTerm {
		return fmt.Errorf("cannot decrement term from %d to %d", n.CurrentTerm, term)
	}

	// Same term, and a vote is already recorded: only that same vote may be written again.
	// An empty VotedFor means this term has no vote yet, so the first candidate is allowed.
	// A higher term skips this check and can record a new vote.
	if term == n.CurrentTerm && n.VotedFor != "" && votedFor != n.VotedFor {
		return fmt.Errorf("already voted for %q in term %d", n.VotedFor, n.CurrentTerm)
	}

	state := PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}
	if err := writePersistentState(n.statePath, state); err != nil {
		return err
	}
	n.CurrentTerm = term
	n.VotedFor = votedFor
	return nil
}
