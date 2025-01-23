package main

import (
	"fmt"
	"sync/atomic"
)

// Stats stored with a connection
type Stats struct {
	TotalFiles atomic.Uint64
	TotalBytes atomic.Uint64
}

// Adds the stats from the other stats
func (s *Stats) Add(other *Stats) {
	s.TotalFiles.Add(other.TotalFiles.Load())
	s.TotalBytes.Add(other.TotalBytes.Load())
}

// Subtracts the stats from the other stats
func (s *Stats) Sub(other *Stats) {
	s.TotalFiles.Add(-other.TotalFiles.Load())
	s.TotalBytes.Add(-other.TotalBytes.Load())
}

// Resets the stats
func (s *Stats) Reset() {
	s.TotalFiles.Store(0)
	s.TotalBytes.Store(0)
}

// String representation of the stats
func (s *Stats) String() string {
	return fmt.Sprintf("Files: %d | Bytes: %d", s.TotalFiles.Load(), s.TotalBytes.Load())
}

// Increments the stats
func (s *Stats) Inc(totalBytes uint64) {
	s.TotalFiles.Add(1)
	s.TotalBytes.Add(totalBytes)
}
