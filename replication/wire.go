package replication

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

	maxReplicationFrame = 64 << 20 // 64 * 2^20 = 64 MB ( max message size )
)

/*
u32 frame length
u8 protocol version
u8 message type
message payload
*/

// appendrequest struct in types.go

// append response struct in types.go
