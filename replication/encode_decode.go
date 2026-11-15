package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"kvStore/config"
)

//*
//u32 framelength
// u8 protocollength
// message type
// message payload
//

var (
	maxLeaderIDLength    = config.Int("replication.max.leader.id.len")
	maxEntriesPerRequest = config.Int("replication.max.entries.per.request")
)

var maxAppendRequestPayload = maxReplicationFrame - 2 // max payload 64MiB -2 bytes

// -2 because the frame also contains:
// 1 byte protocol version + 1 byte message type

type replicationFrame struct {
	protocolVersion byte
	messageType     byte
	payload         []byte
}

// writes (len, protocolversion, type, payload) to conn
func writeFrame(
	w io.Writer,
	messageType byte,
	payload []byte,
) error {
	frameLength := 2 + len(payload)
	if frameLength > maxReplicationFrame {
		return fmt.Errorf(
			"frame too large: got %d max %d",
			frameLength,
			maxReplicationFrame,
		)
	}

	if err := writeU32(w, uint32(frameLength)); err != nil {
		return err
	}

	if err := writeU8(w, messageProtocol); err != nil {
		return err
	}

	if err := writeU8(w, messageType); err != nil {
		return err
	}

	return writeFull(w, payload)
}

func readFrame(r io.Reader) (replicationFrame, error) {
	frameLength, err := readU32(r)

	if err != nil {
		return replicationFrame{}, fmt.Errorf(
			"read frame length: %w",
			err,
		)
	}

	if frameLength < 2 {
		return replicationFrame{}, errors.New(
			"frame is too small",
		)
	}

	// just in case conversion, dont need it

	if uint64(frameLength) > uint64(maxReplicationFrame) {
		return replicationFrame{}, fmt.Errorf(
			"frame too large: got %d max %d",
			frameLength,
			maxReplicationFrame,
		)
	}

	framebytes, err := readBytes(r, frameLength)
	if err != nil {
		return replicationFrame{}, fmt.Errorf(
			"read frame: %w",
			err,
		)
	}

	return replicationFrame{
		protocolVersion: framebytes[0],
		messageType:     framebytes[1],
		payload:         framebytes[2:],
	}, nil

}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data) // n tells how many writte
		if n > 0 {              // if left
			data = data[n:]
		}
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writeU8(w io.Writer, value uint8) error {
	var buf [1]byte
	buf[0] = value
	return writeFull(w, buf[:])
}

func writeU16(w io.Writer, value uint16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], value)
	return writeFull(w, buf[:])
}

func writeU32(w io.Writer, value uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)

	return writeFull(w, buf[:])
}

func writeU64(w io.Writer, value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)

	return writeFull(w, buf[:])
}

func readU8(r io.Reader) (uint8, error) {
	var buf [1]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return buf[0], nil
}

func readU16(r io.Reader) (uint16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint16(buf[:]), nil
}

func readU32(r io.Reader) (uint32, error) {
	var buf [4]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf[:]), nil
}

func readU64(r io.Reader) (uint64, error) {
	var buf [8]byte

	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buf[:]), nil
}

// read bytes size length
func readBytes(r io.Reader, length uint32) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	data := make([]byte, int(length))

	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return data, nil
}
