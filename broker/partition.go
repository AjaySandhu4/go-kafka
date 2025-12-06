package broker

import (
	"errors"
	"log"
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

func (p *Partition) flushLogs(brokerPort Port, topicName string, partitionKey PartitionKey) error {

	p.partitionMu.RLock()
	lastPersistedOffset := p.LastPersistedOffset
	lastPersistedSegment := p.LastPersistedSegment
	messagesToFlush := p.Messages
	if len(messagesToFlush) == 0 {
		p.partitionMu.RUnlock()
		log.Println("No messages to flush for topic", topicName, "partition", partitionKey)
		return nil // Nothing to flush
	}
	if lastPersistedSegment == -1 {
		// No segments persisted yet, start from the beginning
		lastPersistedSegment = messagesToFlush[0].Offset
	}
	segmentIndexEntry, err := p.GetSegment(lastPersistedSegment)
	if err != nil {
		p.partitionMu.RUnlock()
		return errors.New("Failed to get segment index: " + err.Error())
	}

	segmentFile, err := createSegmentFile(brokerPort, topicName, partitionKey, lastPersistedSegment)
	if err != nil {
		p.partitionMu.RUnlock()
		return errors.New("Failed to open log file for writing: " + err.Error())
	}

	for _, msg := range messagesToFlush {
		if msg.Offset <= lastPersistedOffset {
			log.Println("Skipping already persisted message at offset (this shouldn't happen)", msg.Offset)
			continue // Skip already persisted messages
		}
		if segmentIndexEntry.Size+len(msg.Data) > SegmentSize {
			log.Println("Segment size limit reached, stopping flush for this segment and creating new segment")
			segmentFile.Close()
			p.createNewSegment(msg.Offset)
			segmentFile, err = createSegmentFile(brokerPort, topicName, partitionKey, msg.Offset)
			if err != nil {
				p.partitionMu.RUnlock()
				return errors.New("Failed to create new segment file: " + err.Error())
			}
		}
		segmentFile.Write(msg.Data)
		segmentFile.Write([]byte("\n")) // Write a newline after each message (good for debugging but may be harder to parse later)
		p.LastPersistedOffset = msg.Offset
		segmentIndexEntry.Size += len(msg.Data)
	}
	segmentFile.Close()
	p.partitionMu.RUnlock()
	return nil
}
