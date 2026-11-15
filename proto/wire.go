package proto

import (
	"errors"
	"kvStore/config"
)

// opcodes
const (
	OpGet  byte = 1
	OpSet  byte = 2
	OpDel  byte = 3
	OpPing byte = 4
	OpLen  byte = 5
	// OpNoop is internal to Raft: a new leader appends one so it can commit
	// entries from earlier terms. Clients cannot send it.
	OpNoop byte = 6
)

// status codes for responses
const (
	StatusOk        byte = 0
	StatusNotFound  byte = 1
	StatusError     byte = 2
	StatusNotLeader byte = 3
)

var (
	MaxKeyLen   = config.Int("proto.max.key.len")
	MaxValueLen = config.Int("proto.max.value.len")
)

var (
	ErrUnknownOpcode = errors.New("proto: unknown opcode")
	ErrUnknownStatus = errors.New("proto: unknown status")
	ErrKeyTooLarge   = errors.New("proto: key exceeds max length")
	ErrValueTooLarge = errors.New("proto: value exceeds max length")
)

type Command struct {
	Op    byte
	Key   []byte
	Value []byte

	// Writes only (SET, DEL): who sent it and that client's write number.
	// A retried write keeps the same pair, so the state machine applies it
	// at most once. ClientID 0 means "no dedup".
	ClientID uint64
	Seq      uint64
}

// IsWrite reports whether op changes the store and carries ClientID and Seq on the wire.
func IsWrite(op byte) bool {
	return op == OpSet || op == OpDel
}

type Response struct {
	Status byte
	Value  []byte
}

func ValidOpcode(op byte) bool {
	switch op {
	case OpSet, OpDel, OpGet, OpPing, OpLen:
		return true
	default:
		return false
	}
}

func validStatus(r byte) bool {
	switch r {
	case StatusError, StatusNotFound, StatusOk, StatusNotLeader:
		return true
	default:
		return false
	}
}

/*

 cmd := Command{
     Op: OpSet,
     Key: []byte("name"),
     Value: []byte("Tarun"),
 }

 02
 00 00 00 04
 6E 61 6D 65
 00 00 00 05
 54 61 72 75 6E

 02
 │
 └── Opcode
     OpSet = 2


 00 00 00 04
 │
 └── Key length
     uint32 = 4 bytes


 6E 61 6D 65
 │
 └── Key bytes
     "name"


 00 00 00 05
 │
 └── Value length
     uint32 = 5 bytes


 54 61 72 75 6E
 │
 └── Value bytes
     "Tarun"

*/
