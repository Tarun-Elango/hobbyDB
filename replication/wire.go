package replication

import "kvStore/config"

const (
	messageProtocol byte = 1

	//	when we read message type byte, we can decode either req or resp
	messageAppendRequest  byte = 1
	messageAppendResponse byte = 2
	messageRequestVote    byte = 3
	messageRequestVoteRes byte = 4
	messageHeartbeat      byte = 5
	messageHeartbeatRes   byte = 6

	messageRaftAppendEntries    byte = 7
	messageRaftAppendEntriesRes byte = 8
)

// max message size
var maxReplicationFrame = config.Int("replication.max.frame")

/*
u32 frame length
u8 protocol version
u8 message type
message payload
*/

// appendrequest struct in types.go

// append response struct in types.go
