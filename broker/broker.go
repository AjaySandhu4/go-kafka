package broker

import (
	"context"
	producerpb "go-kafka/proto/producer"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type brokerServer struct {
	producerpb.UnimplementedProducerServiceServer
	Topics     map[string]*Topic
	port       int
	etcdClient *clientv3.Client
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
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
	var err error

	// Initialize etcd client
	b.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}

	// Register broker in etcd
	b.registerBrokerInEtcd()

	// Start the gRPC server
	server := grpc.NewServer()
	producerpb.RegisterProducerServiceServer(server, b)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(b.port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Broker listening on :%d", b.port)
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	res, err := b.etcdClient.Get(context.Background(), "controller")
	if err != nil {
		log.Fatalf("Failed to get controller key from etcd: %v", err)
	}
	if len(res.Kvs) > 0 {
		log.Printf("Controller key found in etcd: %s", res.Kvs[0].Value)
	} else {
		log.Println("Controller key not found in etcd")
		// Create the controller key
		_, err := b.etcdClient.Put(context.Background(), "controller", strconv.Itoa(b.port))
		if err != nil {
			log.Fatalf("Failed to create controller key in etcd: %v", err)
		}
		log.Println("Controller key created in etcd")
	}

}

func (b *brokerServer) registerBrokerInEtcd() {
	brokerGetResp, err := b.etcdClient.Get(context.Background(), "/broker/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get broker keys from etcd: %v", err)
	}

	// Grant a lease (30 seconds TTL)
	leaseResp, err := b.etcdClient.Grant(context.Background(), 10)
	if err != nil {
		log.Fatalf("Failed to grant lease: %v", err)
	}

	b.port = 8080 + len(brokerGetResp.Kvs) // Assignment port number based on number of existing brokers
	_, err = b.etcdClient.Put(context.Background(), "/broker/"+strconv.Itoa(b.port), "", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		log.Fatalf("Failed to register broker in etcd: %v", err)
	}

	// Set up automatic lease renewal
	kaCh, kaErr := b.etcdClient.KeepAlive(context.Background(), leaseResp.ID)
	if kaErr != nil {
		log.Fatalf("Failed to setup lease keep alive: %v", kaErr)
	}

	// Handle lease renewal responses in background
	go func() {
		for ka := range kaCh {
			log.Printf("Lease renewed: ID=%d, TTL=%d", ka.ID, ka.TTL)
		}
		log.Println("Lease keep alive channel closed")
	}()
}

func (b *brokerServer) ShutdownBroker() {
	log.Println("Shutting down Broker...")
	// Cleanup logic would go here
	if b.etcdClient != nil {
		b.etcdClient.Close()
	}
}
