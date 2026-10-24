// tags lets one socket carry votes and leader messages besides append

messageAppendRequest  byte = 1
messageAppendResponse byte = 2
messageRequestVote    byte = 3
messageRequestVoteRes byte = 4
messageHeartbeat      byte = 5
messageHeartbeatRes   byte = 6