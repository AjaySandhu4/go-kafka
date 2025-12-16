package broker

import (
	"context"
	"errors"
	"go-kafka/cluster"
	consumerpb "go-kafka/proto/consumer"
	"io"
)

func (b *brokerServer) consumeMessages(ctx context.Context, req *consumerpb.ConsumeRequest) (*consumerpb.ConsumeResponse, error) {
	// Implement the logic to consume messages from the specified topic and partition
	// Steps:
	// 1. Validate topic and partition
	// 2. Use offset and message index to find the segment file to read from
	// 3. Read segment file at the right offset
	// 4. Read messages until either the message number threshold, the max size threshold, or end of segment
	// 5. If end of segment reached, check if there is a next segment and continue reading from beginning of that segment
	// 6. Return messages to consumer

	topic := req.Topic
	partitionKey := cluster.PartitionKey(req.PartitionKey)
	offset := req.Offset

	b.topicsMu.RLock()
	if b.Topics[topic] == nil {
		return nil, errors.New("Topic does not exist")
	}
	b.topicsMu.RUnlock()

	b.metadataMu.RLock()
	topicMeta := b.ClusterMetadata.TopicsMetadata.Topics[topic]
	b.metadataMu.RUnlock()
	if topicMeta == nil {
		return nil, errors.New("Topic metadata not found")
	}

	// Validate partition
	if topicMeta.Partitions[partitionKey] == 0 {
		return nil, errors.New("Partition does not exist")
	}
	if topicMeta.Partitions[partitionKey] != b.port {
		return nil, errors.New("Partition is not assigned to this broker")
	}
	topicData := b.Topics[topic]
	if topicData == nil {
		return nil, errors.New("Topic data not found")
	}

	topicData.topicMu.RLock()
	partition := topicData.Partitions[partitionKey]
	topicData.topicMu.RUnlock()
	if partition == nil {
		return nil, errors.New("Partition data not found")
	}
	partition.partitionMu.RLock()

	// Validate offset
	if offset < 0 || offset > int64(partition.LastPersistedOffset) {
		return nil, errors.New("Invalid offset")
	}

	segmentIndex, segmentMeta, err := partition.GetSegmentContainingOffset(offset)
	if err != nil {
		partition.partitionMu.RUnlock()
		return nil, err
	}
	partition.partitionMu.RUnlock()
	var messages []*consumerpb.Message
	var bytesRead int32 = 0
	var msgsRead int32 = 0
	var currMsg Message
	var lastOffset int64 = -1

	segmentFile, err := OpenSegmentFile(b.port, topic, partitionKey, segmentMeta.StartOffset, false, true)
	if err != nil {
		return nil, err
	}
	err = segmentFile.SeekToOffset(offset)
	if err != nil {
		return nil, err
	}

	for bytesRead < req.MaxBytes && msgsRead < req.MaxMessages {
		currMsg, err = segmentFile.Read()
		if err == io.EOF {
			// End of segment reached, check for next segment
			segmentIndex++
			partition.partitionMu.RLock()
			if segmentIndex >= len(partition.SegmentIndex) {
				// No more segments
				partition.partitionMu.RUnlock()
				break
			}
			nextSegmentMeta := partition.SegmentIndex[segmentIndex]
			partition.partitionMu.RUnlock()
			segmentFile, err = OpenSegmentFile(b.port, topic, partitionKey, nextSegmentMeta.StartOffset, false, true)
			if err != nil {
				return nil, err
			}
		}
		messages = append(messages, &consumerpb.Message{
			Header: &consumerpb.MessageHeader{
				Size:     currMsg.Header.Size,
				Offset:   currMsg.Header.Offset,
				Crc:      currMsg.Header.CRC,
				DataSize: currMsg.Header.DataSize,
			},
			Data: currMsg.Data,
		})
		bytesRead += int32(currMsg.Header.Size)
		msgsRead++
		lastOffset = currMsg.Header.Offset
	}

	return &consumerpb.ConsumeResponse{LastOffset: lastOffset, Messages: messages}, nil
}
