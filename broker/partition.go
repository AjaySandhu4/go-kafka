package broker

import (
	"errors"
	"sync"
)

type Partition struct {
	Key                  PartitionKey
	SegmentIndex         []*Segment
	NextOffset           int
	LastPersistedOffset  int // Offset of last persisted message
	LastPersistedSegment int // Segment ID of last persisted segment (based on start offset)
	Messages             []Message
	partitionMu          sync.RWMutex
}

type Segment struct {
	StartOffset int
	Size        int
}

type Message struct {
	Offset int
	Data   []byte
}

func (p *Partition) GetSegment(segmentID int) (*Segment, error) {
	if segmentID < 0 || segmentID > p.SegmentIndex[len(p.SegmentIndex)-1].StartOffset {
		return nil, errors.New("Invalid segment ID")
	}

	// Find the segment (TODO optimize with binary search)
	for _, segment := range p.SegmentIndex {
		if segment.StartOffset == segmentID {
			return segment, nil
		}
	}
	return nil, errors.New("Segment not found")
}

func (p *Partition) createNewSegment(startOffset int) {
	newSegment := &Segment{
		StartOffset: startOffset,
		Size:        0,
	}
	p.SegmentIndex = append(p.SegmentIndex, newSegment)
	p.LastPersistedSegment = startOffset
}
