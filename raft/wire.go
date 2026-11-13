// tags lets one socket carry votes and leader messages besides append
package raft

const (
	messageAppendRequest  byte = 1
	messageAppendResponse byte = 2
	MessageRequestVote    byte = 3
	MessageRequestVoteRes byte = 4
	MessageHeartbeat      byte = 5
	MessageHeartbeatRes   byte = 6

	MessageRaftAppendEntries    byte = 7
	MessageRaftAppendEntriesRes byte = 8
)
