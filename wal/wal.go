package wal

// owns the wal file and api to interact
import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Identifies the WAL format and version so incompatible or legacy files can be rejected safely.
// bytes added to top of file as a indicator.
// v3 records may end with [clientID][seq]; v2 records never do. A v2 file is
// read as-is and new records are appended to it in the v3 layout, which a
// v3 reader accepts either way.
var walHeader = []byte{'K', 'V', 'W', 'L', 3} // 3 -> version

// older headers this version still reads
var walHeaderV2 = []byte{'K', 'V', 'W', 'L', 2}

var ErrUnsupportedFormat = errors.New(
	"wal: unsupported format; migrate or delete the existing WAL",
)

type Log struct {
	file      *os.File // represent open file descriptor
	muWal     sync.Mutex
	lastIndex uint64 // highest log index in WAL
}

// ensureHeader writes the WAL format header for a new file or validates it for an existing file.
func ensureHeader(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		// newly created WAL file
		n, err := file.Write(walHeader)
		if err != nil {
			return err
		}

		if n != len(walHeader) {
			return io.ErrShortWrite
		}
		return file.Sync()
	}

	got := make([]byte, len(walHeader))
	n, err := file.ReadAt(got, 0) // read at offset 0 -> got list
	if err != nil || n != len(walHeader) || (!bytes.Equal(got, walHeader) && !bytes.Equal(got, walHeaderV2)) {
		return fmt.Errorf("%w: expected WAL v2 or v3 header", ErrUnsupportedFormat)
	}
	return nil
}

// open
// O_APPEND -> write atomic to end of file, no overwrite
func Open(path string) (*Log, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("Failed to make dir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file: %w", err)
	}

	if err := ensureHeader(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Log{file: f}, nil
}

// append
// caller to not treat command as durable until return err
func (l *Log) Append(rec Record) error {
	return l.AppendBatch([]Record{rec})
}

// AppendBatch writes all records to the WAL in a single write and fsyncs once
// for the entire batch. If any record fails validation or encoding, nothing is
// written to the file.
func (l *Log) AppendBatch(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}

	l.muWal.Lock()
	defer l.muWal.Unlock()

	var buf bytes.Buffer
	for _, rec := range recs {
		if err := EncodeRecord(&buf, rec); err != nil {
			return fmt.Errorf("Failed to append: %w", err)
		}
	}

	n, err := l.file.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to append: %w", err)
	}
	if n != buf.Len() {
		return fmt.Errorf("Failed to append: %w", io.ErrShortWrite)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	l.lastIndex = recs[len(recs)-1].Index
	return nil
}

// TruncateFrom removes the record at index and every record after it.
// It is used when a follower replaces a divergent log suffix with the
// leader's version. Index is one-based, so TruncateFrom(1) leaves only the
// WAL header.
func (l *Log) TruncateFrom(index uint64) error {
	if index == 0 {
		return ErrInvalidIndex
	}

	l.muWal.Lock()
	defer l.muWal.Unlock()

	if _, err := l.file.Seek(int64(len(walHeader)), io.SeekStart); err != nil { // move cursor to first record
		return fmt.Errorf("failed to seek WAL start: %w", err)
	}

	var lastIndex uint64
	for {
		position, err := l.file.Seek(0, io.SeekCurrent) // byte offset of the current record in the file
		if err != nil {
			return fmt.Errorf("failed to get WAL position: %w", err)
		}

		record, err := DecodeRecord(l.file) // read the record
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read WAL while truncating: %w", err)
		}

		if record.Index >= index { // if the record index is >= index , then trucnate from the next record
			if err := l.file.Truncate(position); err != nil {
				return fmt.Errorf("truncate WAL from index %d: %w", index, err)
			}
			if err := l.file.Sync(); err != nil {
				return fmt.Errorf("sync truncated WAL: %w", err)
			}
			break
		}

		lastIndex = record.Index
	}

	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek WAL end: %w", err)
	}

	// The replication log is contiguous. If index is beyond the end,
	// lastIndex remains the current last record, which makes this a safe no-op.
	l.lastIndex = lastIndex
	return nil
}

// Replay + callback func, apply all commands from file to memory
func (l *Log) Replay(apply func(Record) error) error {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	if _, err := l.file.Seek(int64(len(walHeader)), io.SeekStart); err != nil {
		return fmt.Errorf("failed to move cursor: %w", err)
	}
	l.lastIndex = 0 // set to 0
	for {
		// get the current position, before decoding
		pos, err := l.file.Seek(0, io.SeekCurrent) // just tells
		if err != nil {
			return fmt.Errorf("failed to get WAL position: %w", err)
		}
		rec, err := DecodeRecord(l.file)

		if errors.Is(err, io.EOF) {
			break
		}

		if errors.Is(err, ErrIncompleteRecord) {
			// remove incomplete record and all after, done on purpose
			if err := l.file.Truncate(pos); err != nil {
				return fmt.Errorf("failed to truncate incomplete WAL tail: %w", err)
			}
			break
		}

		if err != nil {
			return err
		}

		if rec.Index != l.lastIndex+1 {
			return fmt.Errorf(
				"%w: got index %d, want %d",
				ErrIndexOutOfOrder,
				rec.Index,
				l.lastIndex+1,
			)
		}

		if err := apply(rec); err != nil {
			return err
		}

		l.lastIndex = rec.Index // last successful played index
	}

	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek WAL end: %w", err)
	}

	return nil
}

// close
func (l *Log) Close() error {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("Error during closing Wal file: %w", err)
	}
	return l.file.Close()
}

// LastIndex returns the highest record index observed during append or replay.
func (l *Log) LastIndex() uint64 {
	l.muWal.Lock()
	defer l.muWal.Unlock()

	return l.lastIndex
}
