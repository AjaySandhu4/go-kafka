package broker

import (
	"context"
	"encoding/json"
	producerpb "go-kafka/proto/producer"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type brokerServer struct {
	producerpb.UnimplementedProducerServiceServer
	Topics       map[string]*Topic
	Metadata     Metadata
	port         Port
	etcdClient   *clientv3.Client
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	isController bool
}

type Topic struct {
	Name          string
	NumPartitions int
	Partitions    map[PartitionKey]*Partition // Each partition holds a slice of messages
}

type Partition struct {
	Key        PartitionKey
	Index      []IndexEntry
	NextOffset int
	Messages   [][]byte
}

type IndexEntry struct {
	Offset int
	Size   int
}

type Metadata struct {
	TopicInfo  map[string]TopicMetadata
	BrokerInfo map[Port]struct{}
}

type TopicMetadata struct {
	Topic         string
	NumPartitions int
	Partitions    map[PartitionKey]Port // map key to broker address
}

type Port int
type PartitionKey int

// TODO : Implement PartitionMetadata struct
// type PartitionMetadata struct {
// 	brokerPort int
// 	brokerAddr string
// }

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
	partitionKey := PartitionKey(req.PartitionKey)
	if b.Metadata.TopicInfo[req.Topic].Partitions[partitionKey] != b.port {
		log.Printf("Partition %d is not assigned to this broker", partitionKey)
		return &producerpb.PublishResponse{Success: false}, nil
	}
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

	newTopic := &Topic{
		Name:          req.Topic,
		NumPartitions: int(req.NumPartitions),
		Partitions:    make(map[PartitionKey]*Partition),
	}

	if b.etcdClient == nil {
		log.Printf("etcd client is not initialized")
		return &producerpb.CreateTopicResponse{Success: false}, nil
	}

	// Assign partitions to brokers
	brokers := make([]Port, 0, len(b.Metadata.BrokerInfo))
	for addr := range b.Metadata.BrokerInfo {
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
		return &producerpb.CreateTopicResponse{Success: false}, nil
	}

	_, err = b.etcdClient.Put(context.Background(), "/topic/"+req.Topic, string(metaBytes))
	if err != nil {
		log.Printf("Failed to create topic in etcd: %v", err)
		return &producerpb.CreateTopicResponse{Success: false}, nil
	}
	b.Topics[req.Topic] = newTopic

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

	b.ctx, b.cancel = context.WithCancel(context.Background())

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

	b.FetchMetadata()

	b.registerBrokerWatcher()
	b.registerTopicWatcher()

	// Start the gRPC server
	server := grpc.NewServer()
	producerpb.RegisterProducerServiceServer(server, b)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(int(b.port)))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Broker listening on :%d", b.port)
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

}

func (b *brokerServer) registerBrokerInEtcd() {
	brokerGetResp, err := b.etcdClient.Get(b.ctx, "/broker/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get broker keys from etcd: %v", err)
	}

	// Grant a lease (30 seconds TTL)
	leaseResp, err := b.etcdClient.Grant(b.ctx, 10)
	if err != nil {
		log.Fatalf("Failed to grant lease: %v", err)
	}

	b.port = Port(8080 + len(brokerGetResp.Kvs)) // Assignment port number based on number of existing brokers
	_, err = b.etcdClient.Put(b.ctx, "/broker/"+strconv.Itoa(int(b.port)), "", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		log.Fatalf("Failed to register broker in etcd: %v", err)
	}

	// Set up automatic lease renewal
	kaCh, kaErr := b.etcdClient.KeepAlive(b.ctx, leaseResp.ID)
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

	// Check for controller key
	res, err := b.etcdClient.Get(context.Background(), "controller")
	if err != nil {
		log.Fatalf("Failed to get controller key from etcd: %v", err)
	}
	if len(res.Kvs) > 0 {
		log.Printf("Controller key found in etcd: %s", res.Kvs[0].Value)
	} else {
		log.Println("Controller key not found in etcd")
		// Create the controller key
		_, err := b.etcdClient.Put(context.Background(), "controller", strconv.Itoa(int(b.port)))
		if err != nil {
			log.Fatalf("Failed to create controller key in etcd: %v", err)
		}
		b.isController = true
		log.Println("Controller key created in etcd")
	}
}

func (b *brokerServer) registerTopicWatcher() {
	topicWatchCh := b.etcdClient.Watch(b.ctx, "/topic/", clientv3.WithPrefix())
	go func() {
		for watchResp := range topicWatchCh {
			for _, ev := range watchResp.Events {
				b.FetchMetadata()
				log.Printf("Topic metadata refreshed due to etcd event: %s %q : %q", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}()
}

func (b *brokerServer) registerBrokerWatcher() {
	brokerWatchCh := b.etcdClient.Watch(b.ctx, "/broker/", clientv3.WithPrefix())
	go func() {
		for watchResp := range brokerWatchCh {
			for _, ev := range watchResp.Events {
				b.FetchMetadata()
				log.Printf("Broker metadata refreshed due to etcd event: %s %q : %q", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}()
}

func (b *brokerServer) ShutdownBroker() {
	log.Println("Shutting down Broker...")
	// Cleanup logic would go here
	if b.etcdClient != nil {
		b.etcdClient.Close()
	}
	b.cancel()
	log.Println("Broker shut down complete.")
}

// Fetch metadata from etcd and populate local Metadata struct
func (b *brokerServer) FetchMetadata() {
	log.Println("Fetching metadata...")

	// Fetch broker info
	brokerGetResp, err := b.etcdClient.Get(b.ctx, "/broker/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get broker keys from etcd: %v", err)
	}
	b.Metadata.BrokerInfo = make(map[Port]struct{})
	for _, kv := range brokerGetResp.Kvs {
		port, err := strconv.Atoi(string(kv.Key[len("/broker/"):]))
		if err != nil {
			log.Printf("Invalid broker port in etcd key: %s", kv.Key)
			continue
		}
		b.Metadata.BrokerInfo[Port(port)] = struct{}{}
	}
	// Fetch topic info
	topicGetResp, err := b.etcdClient.Get(b.ctx, "/topic/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get topic keys from etcd: %v", err)
	}
	b.Metadata.TopicInfo = make(map[string]TopicMetadata)
	for _, kv := range topicGetResp.Kvs {
		var meta TopicMetadata
		if err := json.Unmarshal(kv.Value, &meta); err != nil {
			log.Printf("Failed to unmarshal topic metadata: %v", err)
			continue
		}
		b.Metadata.TopicInfo[meta.Topic] = meta
	}
}

func (b *brokerServer) GetMetadata(ctx context.Context, req *emptypb.Empty) (*producerpb.ProducerMetadata, error) {
	log.Println("Received GetMetadata request")
	meta := &producerpb.ProducerMetadata{
		Topics:  []*producerpb.TopicMetadata{},
		Brokers: []*producerpb.BrokerMetadata{},
	}

	// Populate broker ports
	for port := range b.Metadata.BrokerInfo {
		meta.Brokers = append(meta.Brokers, &producerpb.BrokerMetadata{
			Port:    int32(port),
			Address: "localhost:" + strconv.Itoa(int(port)),
		})
	}

	// Populate topic metadata
	for _, topicInfo := range b.Metadata.TopicInfo {
		meta.Topics = append(meta.Topics, &producerpb.TopicMetadata{
			Topic:         topicInfo.Topic,
			NumPartitions: int32(topicInfo.NumPartitions),
			Partitions:    map[int32]int32{},
		})
		for partitionKey, port := range topicInfo.Partitions {
			meta.Topics[len(meta.Topics)-1].Partitions[int32(partitionKey)] = int32(port)
		}
	}

	return meta, nil
}

func (b *brokerServer) PrintBroker() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	log.Println("==================== BROKER STATE ====================")
	log.Printf("Port: %d", b.port)
	log.Printf("Is Controller: %v", b.isController)
	log.Printf("Context Active: %v", b.ctx.Err() == nil)

	// Print broker metadata
	log.Println("\n--- BROKER METADATA ---")
	log.Printf("Known Brokers (%d):", len(b.Metadata.BrokerInfo))
	for port := range b.Metadata.BrokerInfo {
		if port == b.port {
			log.Printf("  - Port %d (THIS BROKER)", port)
		} else {
			log.Printf("  - Port %d", port)
		}
	}

	// Print topic metadata
	log.Printf("\nTopic Metadata from etcd (%d topics):", len(b.Metadata.TopicInfo))
	for topicName, topicMeta := range b.Metadata.TopicInfo {
		log.Printf("  Topic: %s", topicName)
		log.Printf("    Partitions: %d", topicMeta.NumPartitions)
		log.Printf("    Partition Assignments:")
		for partitionKey, port := range topicMeta.Partitions {
			if port == b.port {
				log.Printf("      Partition %d -> Port %d (THIS BROKER)", partitionKey, port)
			} else {
				log.Printf("      Partition %d -> Port %d", partitionKey, port)
			}
		}
	}

	// Print local topics and messages
	log.Printf("\n--- LOCAL TOPICS (%d) ---", len(b.Topics))
	for topicName, topic := range b.Topics {
		log.Printf("\nTopic: %s", topicName)
		log.Printf("  Name: %s", topic.Name)
		log.Printf("  Num Partitions: %d", topic.NumPartitions)
		log.Printf("  Local Partitions: %d", len(topic.Partitions))

		for partKey, partition := range topic.Partitions {
			if partition == nil {
				log.Printf("    Partition %d: nil", partKey)
				continue
			}
			log.Printf("    Partition %d:", partKey)
			log.Printf("      Messages: %d", len(partition.Messages))
			log.Printf("      Index Entries: %d", len(partition.Index))
			log.Printf("      Next Offset: %d", partition.NextOffset)

			// Print first few messages for debugging
			maxMsgsToPrint := 3
			if len(partition.Messages) > 0 {
				log.Printf("      Sample Messages (showing first %d):", maxMsgsToPrint)
				for i, msg := range partition.Messages {
					if i >= maxMsgsToPrint {
						log.Printf("        ... and %d more messages", len(partition.Messages)-maxMsgsToPrint)
						break
					}
					log.Printf("        [%d] %s", i, string(msg))
				}
			}
		}
	}

	log.Println("\n=====================================================")
}

// Compact version for quick checks
func (b *brokerServer) PrintBrokerSummary() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	log.Printf("Broker Summary: Port=%d, Controller=%v, Topics=%d, Known Brokers=%d",
		b.port, b.isController, len(b.Topics), len(b.Metadata.BrokerInfo))

	totalMessages := 0
	for _, topic := range b.Topics {
		for _, partition := range topic.Partitions {
			if partition != nil {
				totalMessages += len(partition.Messages)
			}
		}
	}
	log.Printf("  Total messages stored locally: %d", totalMessages)
}

// Print specific topic details
func (b *brokerServer) PrintTopicDetails(topicName string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	log.Printf("========== TOPIC DETAILS: %s ==========", topicName)

	// Check local topic
	topic, exists := b.Topics[topicName]
	if !exists {
		log.Printf("Topic %s NOT FOUND in local storage", topicName)
	} else {
		log.Printf("Local Topic:")
		log.Printf("  Name: %s", topic.Name)
		log.Printf("  Num Partitions: %d", topic.NumPartitions)
		log.Printf("  Partitions stored locally: %d", len(topic.Partitions))

		for partKey, partition := range topic.Partitions {
			if partition == nil {
				log.Printf("    Partition %d: nil", partKey)
				continue
			}
			log.Printf("    Partition %d: %d messages, next offset: %d",
				partKey, len(partition.Messages), partition.NextOffset)
		}
	}

	// Check metadata
	meta, hasMeta := b.Metadata.TopicInfo[topicName]
	if !hasMeta {
		log.Printf("\nTopic metadata NOT FOUND in etcd cache")
	} else {
		log.Printf("\nMetadata (from etcd):")
		log.Printf("  Num Partitions: %d", meta.NumPartitions)
		log.Printf("  Partition Assignments:")
		for partKey, port := range meta.Partitions {
			owned := "no"
			if port == b.port {
				owned = "YES (THIS BROKER)"
			}
			log.Printf("    Partition %d -> Port %d [owned by this broker: %s]", partKey, port, owned)
		}
	}

	log.Println("========================================")
}
