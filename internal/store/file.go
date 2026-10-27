package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// File is a durable Store backed by an append-only JSON Lines log. Every
// change appends the full event snapshot and fsyncs before it is acknowledged;
// on startup the log is replayed (last line per ID wins) to rebuild the
// in-memory index.
type File struct {
	*Memory
	f    *os.File
	size int64 // bytes of the log known to be complete
}

var _ Store = (*File)(nil)

// OpenFile opens (or creates) the log at path and replays it. A torn final
// line, left by a crash in the middle of a write, is truncated away; any other
// malformed line is reported as corruption.
func OpenFile(path string) (*File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}
	s := &File{Memory: NewMemory(), f: f}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	s.persist = s.append
	return s, nil
}

func (s *File) replay() error {
	r := bufio.NewReader(s.f)
	for lineNo := 1; ; lineNo++ {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(line) > 0 { // no trailing newline: the last write was torn
				if err := s.f.Truncate(s.size); err != nil {
					return fmt.Errorf("truncate torn tail: %w", err)
				}
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("read log: %w", err)
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil || e.ID == "" {
			return fmt.Errorf("corrupt log at line %d", lineNo)
		}
		s.apply(e)
		s.size += int64(len(line))
	}
}

// append writes evs as one batch and fsyncs. On failure the log is cut back to
// its previous length so disk and index never disagree. Called with mu held.
func (s *File) append(evs ...Event) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf) // Encode adds the trailing newline
	for _, e := range evs {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	if _, err := s.f.Write(buf.Bytes()); err != nil {
		_ = s.f.Truncate(s.size)
		return fmt.Errorf("append log: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		_ = s.f.Truncate(s.size)
		return fmt.Errorf("fsync log: %w", err)
	}
	s.size += int64(buf.Len())
	return nil
}

// Close flushes and closes the log.
func (s *File) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.f.Close()
}
