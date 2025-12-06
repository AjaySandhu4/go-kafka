package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"sync"

	producerpb "go-kafka/proto/producer"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (b *brokerServer) PublishMessage(ctx context.Context, req *producerpb.PublishRequest) (*producerpb.PublishResponse, error) {
	b.topicsMu.RLock()
	log.Printf("Received message for topic %s: %s", req.Topic, req.Message)
	if b.Topics[req.Topic] == nil {
		log.Printf("Topic %s does not exist", req.Topic)
		return &producerpb.PublishResponse{Success: false}, nil
	}
	defer b.topicsMu.RUnlock()

	b.metadataMu.RLock()
	partitionKey := PartitionKey(req.PartitionKey)
	if b.ClusterMetadata.TopicsMetadata.Topics[req.Topic].Partitions[partitionKey] != b.port {
		log.Printf("Partition %d is not assigned to this broker", partitionKey)
		return &producerpb.PublishResponse{Success: false}, nil
	}
	defer b.metadataMu.RUnlock()

	b.Topics[req.Topic].topicMu.Lock()
	defer b.Topics[req.Topic].topicMu.Unlock()

	log.Printf("Publishing to partition %d", partitionKey)
	partition := b.Topics[req.Topic].Partitions[partitionKey]
	if partition == nil {
		partition = &Partition{
			Key:                  partitionKey,
			SegmentIndex:         []*Segment{},
			NextOffset:           0,
			LastPersistedOffset:  -1, // No messages persisted yet
			LastPersistedSegment: -1, // No segments persisted yet
			Messages:             []Message{},
			partitionMu:          sync.RWMutex{},
		}
		b.Topics[req.Topic].Partitions[partitionKey] = partition
	}
	partition.partitionMu.Lock()
	defer partition.partitionMu.Unlock()
	// Append message
	messageBytes := []byte(req.Message)
	partition.Messages = append(partition.Messages, Message{
		Offset: partition.NextOffset,
		Data:   messageBytes,
	})
	log.Printf("Message published to topic %s partition %d at offset %d", req.Topic, partitionKey, partition.NextOffset)
	partition.NextOffset += len(messageBytes)

	return &producerpb.PublishResponse{Success: true}, nil
}

func (b *brokerServer) CreateTopic(ctx context.Context, req *producerpb.CreateTopicRequest) (*producerpb.CreateTopicResponse, error) {
	b.topicsMu.Lock()
	b.metadataMu.Lock()
	defer b.topicsMu.Unlock()
	defer b.metadataMu.Unlock()

	log.Println("Received CreateTopic request for topic:", req.Topic)

	if b.Topics[req.Topic] != nil {
		log.Printf("Topic %s already exists", req.Topic)
		return &producerpb.CreateTopicResponse{Success: false}, nil
	}

	newTopic := &TopicData{
		Name:          req.Topic,
		NumPartitions: int(req.NumPartitions),
		Partitions:    make(map[PartitionKey]*Partition),
		topicMu:       sync.RWMutex{},
	}

	if b.etcdClient == nil {
		log.Printf("etcd client is not initialized")
		return &producerpb.CreateTopicResponse{Success: false}, nil
	}

	// Assign partitions to brokers
	brokers := make([]Port, 0, len(b.ClusterMetadata.BrokersMetadata.Brokers))
	for addr := range b.ClusterMetadata.BrokersMetadata.Brokers {
		brokers = append(brokers, addr)
	}
	partitionAssignments := make(map[PartitionKey]Port)
	for i := range req.NumPartitions {
		if len(brokers) == 0 {
			log.Panic("Broker list is empty, cannot assign partitions")
		}
		brokerIndex := rand.Intn(len(brokers))
		partitionAssignments[PartitionKey(i)] = brokers[brokerIndex]
	}

	topicMeta := TopicMetadata{
		Topic:         req.Topic,
		NumPartitions: int(req.NumPartitions),
		Partitions:    partitionAssignments,
	}

	metaBytes, err := json.Marshal(topicMeta)
	if err != nil {
		log.Printf("Failed to marshal topic metadata: %v", err)
		return &producerpb.CreateTopicResponse{Success: false}, errors.New("Failed to marshal topic metadata")
	}

	_, err = b.etcdClient.Put(context.Background(), "/topic/"+req.Topic, string(metaBytes))
	if err != nil {
		log.Printf("Failed to create topic in etcd: %v", err)
		return &producerpb.CreateTopicResponse{Success: false}, errors.New("Failed to create topic in etcd")
	}

	b.Topics[req.Topic] = newTopic

	b.ClusterMetadata.TopicsMetadata.Topics[req.Topic] = &topicMeta

	log.Printf("Creating topic: %s", req.Topic)
	return &producerpb.CreateTopicResponse{Success: true}, nil
}

func (b *brokerServer) GetMetadata(ctx context.Context, req *emptypb.Empty) (*producerpb.ProducerMetadata, error) {
	b.metadataMu.RLock()
	defer b.metadataMu.RUnlock()

	log.Println("Received GetMetadata request")
	meta := &producerpb.ProducerMetadata{
		Topics:  []*producerpb.TopicMetadata{},
		Brokers: []*producerpb.BrokerMetadata{},
	}

	// Populate broker ports
	for port := range b.ClusterMetadata.BrokersMetadata.Brokers {
		meta.Brokers = append(meta.Brokers, &producerpb.BrokerMetadata{
			Port:    int32(port),
			Address: "localhost:" + strconv.Itoa(int(port)),
		})
	}

	// Populate topic metadata
	for _, topicInfo := range b.ClusterMetadata.TopicsMetadata.Topics {
		meta.Topics = append(meta.Topics, &producerpb.TopicMetadata{
			Topic:         topicInfo.Topic,
			NumPartitions: int32(topicInfo.NumPartitions),
			Partitions:    map[int32]int32{},
		})
		for partitionKey, port := range topicInfo.Partitions {
			meta.Topics[len(meta.Topics)-1].Partitions[int32(partitionKey)] = int32(port)
		}
	}

	// Enter controller port
	meta.ControllerPort = int32(b.ClusterMetadata.BrokersMetadata.Controller)

	log.Println("GetMetadata request processed")

	return meta, nil
}
