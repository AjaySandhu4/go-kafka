package broker

import (
	"errors"
	"go-kafka/cluster"
	"log"
	"slices"
	"sync"
)

type Partition struct {
	Key                  cluster.PartitionKey
	SegmentIndex         []*Segment
	NextOffset           int64
	LastPersistedOffset  int64 // Offset of last persisted message
	LastPersistedSegment int64 // Segment ID of last persisted segment (based on start offset)
	Messages             []Message
	partitionMu          sync.RWMutex
}

type Segment struct {
	StartOffset int64
	Size        int64
}

type Message struct {
	Header MessageHeader
	Data   []byte
}

const MessageHeaderSize = 20

type MessageHeader struct {
	Size     uint32 // Total message size (header + data)
	Offset   int64  // Logical offset
	CRC      uint32 // Checksum for integrity
	DataSize uint32 // Size of actual data
}

func (p *Partition) GetSegmentContainingOffset(offset int64) (int, *Segment, error) {
	index, found := slices.BinarySearchFunc(p.SegmentIndex, offset, func(seg *Segment, targetOffset int64) int {
		return int(seg.StartOffset - targetOffset)
	})
	if found {
		return index, p.SegmentIndex[index], nil
	}
	return index - 1, p.SegmentIndex[index-1], nil
}

func (p *Partition) GetSegment(segmentID int64) (*Segment, error) {
	if segmentID < 0 || segmentID > p.SegmentIndex[len(p.SegmentIndex)-1].StartOffset {
		return nil, errors.New("Invalid segment ID")
	}

	// Find the segment
	index, found := slices.BinarySearchFunc(p.SegmentIndex, segmentID, func(seg *Segment, targetID int64) int {
		return int(seg.StartOffset - targetID)
	})
	if !found {
		return nil, errors.New("Segment not found")
	}
	return p.SegmentIndex[index], nil
}

func (p *Partition) createNewSegment(startOffset int64) *Segment {
	newSegment := &Segment{
		StartOffset: startOffset,
		Size:        0,
	}
	p.SegmentIndex = append(p.SegmentIndex, newSegment)
	p.LastPersistedSegment = startOffset
	return newSegment
}

func (p *Partition) flushLogs(brokerPort cluster.Port, topicName string, partitionKey cluster.PartitionKey) error {

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
		// No segments persisted yet, create the first segment starting from the first message's offset
		p.createNewSegment(messagesToFlush[0].Header.Offset)
		lastPersistedSegment = messagesToFlush[0].Header.Offset
	}
	segmentIndexEntry, err := p.GetSegment(lastPersistedSegment)
	if err != nil {
		p.partitionMu.RUnlock()
		return errors.New("Failed to get segment index: " + err.Error())
	}

	segmentFile, err := OpenSegmentFile(brokerPort, topicName, partitionKey, lastPersistedSegment, debugMode, false)
	if err != nil {
		p.partitionMu.RUnlock()
		return errors.New("Failed to open log file for writing: " + err.Error())
	}

	for _, msg := range messagesToFlush {
		if msg.Header.Offset <= lastPersistedOffset {
			log.Println("Skipping already persisted message at offset (this shouldn't happen in normal operation)", msg.Header.Offset)
			continue // Skip already persisted messages
		}
		messageSize := int64(MessageHeaderSize) + int64(len(msg.Data))
		if segmentIndexEntry.Size+messageSize > SegmentSize {
			log.Println("Segment size limit reached, stopping flush for this segment and creating new segment")
			segmentFile.Close()
			segmentIndexEntry = p.createNewSegment(msg.Header.Offset)
			segmentFile, err = OpenSegmentFile(brokerPort, topicName, partitionKey, msg.Header.Offset, debugMode, false)

			if err != nil {
				p.partitionMu.RUnlock()
				return errors.New("Failed to create new segment file: " + err.Error())
			}
		}
		segmentFile.Write(msg)
		p.LastPersistedOffset = msg.Header.Offset
		segmentIndexEntry.Size += messageSize
	}
	p.Messages = []Message{} // Clear in-memory messages after flushing
	segmentFile.Close()
	p.partitionMu.RUnlock()
	return nil
}
