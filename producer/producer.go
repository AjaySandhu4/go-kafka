package producer

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"go-kafka/broker"
	producerpb "go-kafka/proto/producer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

var bootstrapServerPort = 8080
var rpcRetries = 3

type Producer struct {
	// Add producer fields if necessary
	clientConn map[broker.Port]*ClientConn
	metadata   *broker.Metadata
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.RWMutex
}

type ClientConn struct {
	conn   *grpc.ClientConn
	client producerpb.ProducerServiceClient
}

func (p *Producer) StartProducer() {
	log.Println("Starting Producer...")
	p.clientConn = make(map[broker.Port]*ClientConn)

	// Connect to bootstrap broker
	conn, err := grpc.NewClient("localhost:"+strconv.Itoa(bootstrapServerPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to bootstrap broker: %v", err)
	}
	p.clientConn[broker.Port(bootstrapServerPort)] = &ClientConn{
		conn:   conn,
		client: producerpb.NewProducerServiceClient(conn),
	}

	err = p.populateMetadata(true)
	if err != nil {
		log.Fatalf("Failed to get metadata: %v", err)
	}

	err = p.populateClientConnections()

	p.ctx, p.cancel = context.WithCancel(context.Background())
	go p.refreshMetadataLoop(p.ctx)

	log.Println("Producer started")

}

func (p *Producer) populateClientConnections() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for port := range p.metadata.BrokerInfo {
		if _, exists := p.clientConn[port]; !exists {
			conn, err := grpc.NewClient("localhost:"+strconv.Itoa(int(port)), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to broker at port %d: %v", port, err)
				return err
			}
			p.clientConn[port] = &ClientConn{
				conn:   conn,
				client: producerpb.NewProducerServiceClient(conn),
			}
			log.Printf("Connected to broker at port %d", port)
		}
	}
	return nil
}

func (p *Producer) ShutdownProducer() {
	log.Println("Shutting down Producer...")
	// Cleanup logic would go here
	if p.clientConn == nil {
		return
	}
	for port, clientConn := range p.clientConn {
		if clientConn != nil && clientConn.conn != nil {
			clientConn.conn.Close()
		}
		delete(p.clientConn, port)
	}
	p.cancel()
	log.Println("Producer shut down")

}

func (p *Producer) PublishMessage(topic string, message string) error {
	log.Printf("Producing message to topic %s: %s", topic, message)

	for attempt := range rpcRetries {
		// Pick random partition to send message to
		partitionKey, err := p.getRandomPartition(topic)
		if err != nil {
			log.Printf("Failed to get random partition: %v", err)
			log.Printf("Attempt %d: Refreshing metadata", attempt+1)
			p.populateMetadata(false)
			time.Sleep(time.Millisecond * 100)
			continue
		}

		owningBrokerPort, ok := p.metadata.TopicInfo[topic].Partitions[partitionKey]
		if !ok {
			log.Printf("Failed to find owning broker for partition %d", partitionKey)
			log.Printf("Attempt %d: Refreshing metadata", attempt+1)
			p.populateMetadata(false)
			time.Sleep(time.Millisecond * 100)
			continue
		}

		// Send message to broker
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		res, err := p.clientConn[owningBrokerPort].client.PublishMessage(ctx, &producerpb.PublishRequest{Topic: topic, Message: message, PartitionKey: int32(partitionKey)})
		if err != nil {
			log.Printf("Attempt %d: Failed to publish message, refreshing metadata: %v", attempt, err)
			cancel()
			p.populateMetadata(false)
			time.Sleep(time.Millisecond * 100)
			continue
		}
		log.Printf("Sent: %s | Response: %s", message, res.String())
		return nil
	}
	log.Println("All attempts to publish message failed")
	return errors.New("Failed to publish message after retries")

}

// TODO: handle errors better for each case
func (p *Producer) CreateTopic(topic string, numPartitions int) (string, error) {
	log.Printf("Creating topic %s with %d partitions", topic, numPartitions)
	for attempt := range rpcRetries {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		log.Println("Creating topic via controller port")
		if p.metadata == nil || p.metadata.ControllerPort == 0 {
			log.Println("No valid controller port available")
			p.populateMetadata(true)
			time.Sleep(time.Millisecond * 100)
			continue
		}
		res, err := p.clientConn[p.metadata.ControllerPort].client.CreateTopic(ctx, &producerpb.CreateTopicRequest{Topic: topic, NumPartitions: int32(numPartitions)})
		if err != nil {
			log.Printf("Attempt %d: Failed to create topic, refreshing metadata: %v", attempt, err)
			cancel()
			p.populateMetadata(false)
			time.Sleep(time.Millisecond * 100)
			continue
		}
		log.Printf("Created topic: %s | Response: %s", topic, res.String())
		return res.String(), nil
	}
	return "", errors.New("Failed to create topic after retries")
}

func (p *Producer) populateMetadata(useBootstrapServer bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	brokerPort := broker.Port(bootstrapServerPort)
	if !useBootstrapServer && p.metadata != nil && p.metadata.ControllerPort != 0 {
		brokerPort = p.metadata.ControllerPort
	}
	log.Println("Fetching producer metadata")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	res, err := p.clientConn[brokerPort].client.GetMetadata(ctx, &emptypb.Empty{})
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
	meta.ControllerPort = broker.Port(res.ControllerPort)

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
			err := p.populateMetadata(false)
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
