package broker

import (
	"context"
	producerpb "go-kafka/proto/producer"
	"log"
	"math/rand"
	"net"
	"sync"

	"google.golang.org/grpc"
)

type brokerServer struct {
	producerpb.UnimplementedProducerServiceServer
	Topics map[string]*Topic
	mu     sync.RWMutex
}

type Topic struct {
	Name          string
	NumPartitions int
	Partitions    []*Partition // Each partition holds a slice of messages
}

type Partition struct {
	Key        int
	Index      []IndexEntry
	NextOffset int
	Messages   [][]byte
}

type IndexEntry struct {
	Offset int
	Size   int
}

func NewBrokerServer() *brokerServer {
	return &brokerServer{
		Topics: make(map[string]*Topic), // Initialize the map
	}
}

func (b *brokerServer) PublishMessage(ctx context.Context, req *producerpb.PublishRequest) (*producerpb.PublishResponse, error) {
	log.Printf("Received message for topic %s: %s", req.Topic, req.Message)
	if b.Topics[req.Topic] == nil {
		log.Printf("Topic %s does not exist", req.Topic)
		return &producerpb.PublishResponse{Success: false}, nil
	}
	// Simple partitioning logic: random partition
	partitionKey := rand.Intn(b.Topics[req.Topic].NumPartitions)
	log.Printf("Publishing to partition %d", partitionKey)
	partition := b.Topics[req.Topic].Partitions[partitionKey]
	if partition == nil {
		partition = &Partition{
			Key:        partitionKey,
			Index:      []IndexEntry{},
			NextOffset: 0,
			Messages:   [][]byte{},
		}
		b.Topics[req.Topic].Partitions[partitionKey] = partition
	}
	// Append message
	messageBytes := []byte(req.Message)
	partition.Messages = append(partition.Messages, messageBytes)
	partition.Index = append(partition.Index, IndexEntry{
		Offset: partition.NextOffset,
		Size:   len(messageBytes),
	})
	partition.NextOffset += len(messageBytes)
	log.Printf("Message published to topic %s partition %d at offset %d", req.Topic, partitionKey, partition.Index[len(partition.Index)-1].Offset)

	return &producerpb.PublishResponse{Success: true}, nil
}

func (b *brokerServer) CreateTopic(ctx context.Context, req *producerpb.CreateTopicRequest) (*producerpb.CreateTopicResponse, error) {
	if b.Topics[req.Topic] != nil {
		log.Printf("Topic %s already exists", req.Topic)
		return &producerpb.CreateTopicResponse{Success: false}, nil
	}
	b.Topics[req.Topic] = &Topic{
		Name:          req.Topic,
		NumPartitions: int(req.NumPartitions),
		Partitions:    make([]*Partition, int(req.NumPartitions)),
	}

	log.Printf("Creating topic: %s", req.Topic)
	return &producerpb.CreateTopicResponse{Success: true}, nil
}

func (b *brokerServer) PrintTopic(topicName string) {
	topic, exists := b.Topics[topicName]
	if !exists {
		log.Printf("Topic %s does not exist", topicName)
		return
	}
	log.Printf("Topic: %s", topic.Name)
	for _, partition := range topic.Partitions {
		if partition == nil {
			continue
		}
		log.Printf("  Partition %d:", partition.Key)
		for i, msg := range partition.Messages {
			log.Printf("    Message %d: %s", i, string(msg))
		}
	}
}

func (b *brokerServer) StartBroker() {
	// Start the gRPC server
	server := grpc.NewServer()
	producerpb.RegisterProducerServiceServer(server, b)

	// Listen on port 8080
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Broker listening on :8080")
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
}
