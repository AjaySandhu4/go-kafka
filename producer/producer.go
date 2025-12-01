package producer

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"go-kafka/broker"
	producerpb "go-kafka/proto/producer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

var bootstrapServerPort = "8080"
var rpcRetries = 3

type Producer struct {
	// Add producer fields if necessary
	grpcConn       *grpc.ClientConn
	producerClient producerpb.ProducerServiceClient
	metadata       *broker.Metadata
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
}

func (p *Producer) StartProducer() {
	log.Println("Starting Producer...")

	// Connect to broker
	conn, err := grpc.NewClient("localhost:"+bootstrapServerPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to bootstrap broker: %v", err)
	}
	p.grpcConn = conn

	// Create producer with broker connection
	p.producerClient = producerpb.NewProducerServiceClient(conn)

	err = p.populateMetadata()
	if err != nil {
		log.Fatalf("Failed to get metadata: %v", err)
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())
	go p.refreshMetadataLoop(p.ctx)

	log.Println("Producer started")

}

func (p *Producer) ShutdownProducer() {
	log.Println("Shutting down Producer...")
	// Cleanup logic would go here
	if p.grpcConn != nil {
		p.grpcConn.Close()
	}
	p.cancel()
	log.Println("Producer shut down")

}

func (p *Producer) PublishMessage(topic string, message string) (string, error) {
	log.Printf("Producing message to topic %s: %s", topic, message)

	var err error
	for attempt := range rpcRetries {
		// Pick random partition to send message to
		partitionKey, err := p.getRandomPartition(topic)
		if err != nil {
			log.Printf("Failed to get random partition: %v", err)
			log.Printf("Attempt %d: Refreshing metadata", attempt+1)
			p.populateMetadata()
			continue
		}

		// Send message to broker
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		res, err := p.producerClient.PublishMessage(ctx, &producerpb.PublishRequest{Topic: topic, Message: message, PartitionKey: int32(partitionKey)})
		if err != nil {
			log.Printf("Attempt %d: Failed to publish message, refreshing metadata: %v", attempt, err)
			cancel()
			p.populateMetadata()
			continue
		}
		log.Printf("Sent: %s | Response: %s", message, res.String())
		return res.String(), nil
	}
	return "", err

}

func (p *Producer) CreateTopic(topic string, numPartitions int) (string, error) {
	log.Printf("Creating topic %s with %d partitions", topic, numPartitions)
	var err error
	for attempt := range rpcRetries {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		res, err := p.producerClient.CreateTopic(ctx, &producerpb.CreateTopicRequest{Topic: topic, NumPartitions: int32(numPartitions)})
		if err != nil {
			log.Printf("Attempt %d: Failed to create topic, refreshing metadata: %v", attempt, err)
			cancel()
			p.populateMetadata()
			continue
		}
		log.Printf("Created topic: %s | Response: %s", topic, res.String())
		return res.String(), nil
	}
	return "", err
}

func (p *Producer) populateMetadata() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	log.Println("Fetching producer metadata")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	res, err := p.producerClient.GetMetadata(ctx, &emptypb.Empty{})
	if err != nil {
		log.Printf("Failed to get metadata: %v", err)
		return err
	}
	meta := broker.Metadata{
		TopicInfo:  make(map[string]broker.TopicMetadata),
		BrokerInfo: make(map[broker.Port]struct{}),
	}
	for _, t := range res.Topics {
		topicMeta := broker.TopicMetadata{
			Topic:         t.Topic,
			NumPartitions: int(t.NumPartitions),
			Partitions:    make(map[broker.PartitionKey]broker.Port),
		}
		for partition, port := range t.Partitions {
			topicMeta.Partitions[broker.PartitionKey(partition)] = broker.Port(port)
		}
		meta.TopicInfo[t.Topic] = topicMeta
	}
	for _, b := range res.Brokers {
		meta.BrokerInfo[broker.Port(b.Port)] = struct{}{}
	}
	p.metadata = &meta
	log.Printf("Got metadata from broker")
	return nil
}

func (p *Producer) getRandomPartition(topic string) (broker.PartitionKey, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.metadata == nil {
		return -1, errors.New("producer metadata is not populated")
	}
	topicMeta, exists := p.metadata.TopicInfo[topic]
	if !exists {
		return -1, errors.New("topic does not exist in metadata")
	}
	// Simple random partition selection
	var partitionKeys []broker.PartitionKey
	for pk := range topicMeta.Partitions {
		partitionKeys = append(partitionKeys, pk)
	}
	if len(partitionKeys) == 0 {
		return -1, errors.New("no partitions available for topic")
	}
	selectedPartition := partitionKeys[rand.Intn(len(partitionKeys))]
	return selectedPartition, nil
}

func (p *Producer) refreshMetadataLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := p.populateMetadata()
			if err != nil {
				log.Printf("Failed to refresh metadata: %v", err)
			} else {
				log.Printf("Successfully refreshed metadata")
			}
		case <-ctx.Done():
			log.Println("Metadata refresh loop stopped")
			return
		}
	}
}
