package broker

import (
	"context"
	"encoding/json"
	"errors"
	producerpb "go-kafka/proto/producer"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type brokerServer struct {
	producerpb.UnimplementedProducerServiceServer
	Topics          map[string]*TopicData
	ClusterMetadata ClusterMetadata
	port            Port
	etcdClient      *clientv3.Client
	topicsMu        sync.RWMutex
	metadataMu      sync.RWMutex
	leaseID         clientv3.LeaseID
	ctx             context.Context
	cancel          context.CancelFunc
}

type TopicData struct {
	Name          string
	NumPartitions int
	Partitions    map[PartitionKey]*Partition // Each partition holds a slice of messages
	topicMu       sync.RWMutex
}

type ClusterMetadata struct {
	TopicsMetadata  *TopicsMetadata
	BrokersMetadata *BrokersMetadata
}

type TopicMetadata struct {
	Topic         string
	NumPartitions int
	Partitions    map[PartitionKey]Port // map key to broker address
}

type TopicsMetadata struct {
	Topics map[string]*TopicMetadata
}

type BrokerMetadata struct {
	Port Port
}

type BrokersMetadata struct {
	Brokers    map[Port]*BrokerMetadata
	Controller Port
}

type Port int
type PartitionKey int

const SegmentSize = 1024 * 10 // 10KB for testing
const MsgFlushThreshold = 100 // Flush after every 100 messages for testing
const FlushInterval = 5 * time.Second

// TODO : Implement PartitionMetadata struct
// type PartitionMetadata struct {
// 	brokerPort int
// 	brokerAddr string
// }

func NewBrokerServer() *brokerServer {
	log.Println("Creating new BrokerServer instance...")
	return &brokerServer{
		Topics: make(map[string]*TopicData), // Initialize the map
		ClusterMetadata: ClusterMetadata{
			TopicsMetadata:  &TopicsMetadata{Topics: make(map[string]*TopicMetadata)},
			BrokersMetadata: &BrokersMetadata{Brokers: make(map[Port]*BrokerMetadata)},
		},
	}
}

func (b *brokerServer) StartBroker() {
	log.Println("Starting Broker...")
	var err error

	b.ctx, b.cancel = context.WithCancel(context.Background())

	log.Println("Connecting to etcd...")
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

	b.fetchBrokerMetadata()
	b.fetchTopicMetadata()
	b.fetchOrElectController()

	b.registerControllerWatcher()
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

	// Start log flush ticker
	// go b.flushTicker()

	log.Println("Broker started")

}

func (b *brokerServer) registerBrokerInEtcd() {
	b.metadataMu.Lock()
	defer b.metadataMu.Unlock()

	brokerGetResp, err := b.etcdClient.Get(b.ctx, "/broker/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get broker keys from etcd: %v", err)
	}

	// Grant a lease (30 seconds TTL)
	leaseResp, err := b.etcdClient.Grant(b.ctx, 10)
	if err != nil {
		log.Fatalf("Failed to grant lease: %v", err)
	}
	b.leaseID = leaseResp.ID

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

	log.Printf("Broker registered in etcd with port %d", b.port)
}

func (b *brokerServer) registerTopicWatcher() {
	topicWatchCh := b.etcdClient.Watch(b.ctx, "/topic/", clientv3.WithPrefix())
	go func() {
		for watchResp := range topicWatchCh {
			for _, ev := range watchResp.Events {
				b.fetchTopicMetadata()
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
				b.fetchBrokerMetadata()
				log.Printf("Broker metadata refreshed due to etcd event: %s %q : %q", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}()
}

func (b *brokerServer) registerControllerWatcher() {
	controllerWatchCh := b.etcdClient.Watch(b.ctx, "/controller", clientv3.WithPrefix())
	go func() {
		for watchResp := range controllerWatchCh {
			for _, ev := range watchResp.Events {
				b.fetchOrElectController()
				log.Printf("Controller metadata refreshed due to etcd event: %s %q : %q", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
		}
	}()
}

func (b *brokerServer) ShutdownBroker() {
	b.metadataMu.Lock()
	defer b.metadataMu.Unlock()
	log.Println("Shutting down Broker...")
	// Cleanup logic would go here
	if b.etcdClient != nil {
		b.etcdClient.Close()
	}
	b.cancel()
	log.Println("Broker shut down complete...")
}

func (b *brokerServer) fetchBrokerMetadata() error {
	b.metadataMu.Lock()
	defer b.metadataMu.Unlock()
	log.Println("Fetching broker metadata...")
	// Fetch broker info
	if b.etcdClient == nil {
		log.Println("Etcd client is not initialized")
		return errors.New("Etcd client is not initialized")
	}
	brokerGetResp, err := b.etcdClient.Get(context.Background(), "/broker/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get broker keys from etcd: %v", err)
	}
	log.Println("Broker keys fetched from etcd")
	b.ClusterMetadata.BrokersMetadata = &BrokersMetadata{Brokers: make(map[Port]*BrokerMetadata)}
	for _, kv := range brokerGetResp.Kvs {
		port, err := strconv.Atoi(string(kv.Key[len("/broker/"):]))
		if err != nil {
			log.Printf("Invalid broker port in etcd key: %s", kv.Key)
			continue
		}
		b.ClusterMetadata.BrokersMetadata.Brokers[Port(port)] = &BrokerMetadata{Port: Port(port)}
	}

	// b.fetchOrElectController()

	return nil
}

func (b *brokerServer) fetchOrElectController() error {
	b.metadataMu.Lock()
	defer b.metadataMu.Unlock()
	log.Println("Fetching or electing controller...")
	if b.etcdClient == nil {
		log.Println("Etcd client is not initialized")
		return errors.New("Etcd client is not initialized")
	}
	// Create a transaction
	txn := b.etcdClient.Txn(context.Background())

	txnResp, err := txn.
		If(clientv3.Compare(clientv3.Version("controller"), "=", 0)).                                 // Test: key doesn't exist
		Then(clientv3.OpPut("controller", strconv.Itoa(int(b.port)), clientv3.WithLease(b.leaseID))). // Set: create with our port
		Else(clientv3.OpGet("controller")).                                                           // Get: retrieve existing controller
		Commit()

	if err != nil {
		return err
	}

	if txnResp.Succeeded {
		log.Printf("This broker (port %d) elected as controller", b.port)
		b.ClusterMetadata.BrokersMetadata.Controller = b.port
		return nil
	}
	getResp := txnResp.Responses[0].GetResponseRange()
	controllerPort, err := strconv.Atoi(string(getResp.Kvs[0].Value))
	log.Printf("A broker %d is already the controller (as seen by broker %d)", controllerPort, b.port)
	if err != nil {
		log.Printf("Invalid controller port in etcd: %s", getResp.Kvs[0].Value)
		return err
	}

	b.ClusterMetadata.BrokersMetadata.Controller = Port(controllerPort)
	return nil
}

func (b *brokerServer) fetchTopicMetadata() error {
	b.metadataMu.Lock()
	defer b.metadataMu.Unlock()
	log.Println("Fetching topic metadata...")
	// Fetch topic info
	if b.etcdClient == nil {
		log.Println("Etcd client is not initialized")
		return errors.New("Etcd client is not initialized")
	}
	topicGetResp, err := b.etcdClient.Get(context.Background(), "/topic/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get topic keys from etcd: %v", err)
	}
	b.ClusterMetadata.TopicsMetadata = &TopicsMetadata{Topics: make(map[string]*TopicMetadata)}
	for _, kv := range topicGetResp.Kvs {
		var meta TopicMetadata
		if err := json.Unmarshal(kv.Value, &meta); err != nil {
			log.Printf("Failed to unmarshal topic metadata: %v", err)
			continue
		}
		b.ClusterMetadata.TopicsMetadata.Topics[meta.Topic] = &meta
	}
	return nil
}

func (b *brokerServer) flushLogs(topicName string, partitionKey PartitionKey) error {
	b.topicsMu.RLock()
	topic, exists := b.Topics[topicName]
	b.topicsMu.RUnlock()
	if !exists {
		return errors.New("Topic does not exist")
	}

	topic.topicMu.RLock()
	partition, exists := topic.Partitions[partitionKey]
	topic.topicMu.RUnlock()
	if !exists {
		return errors.New("Partition does not exist")
	}

	partition.partitionMu.RLock()
	lastPersistedOffset := partition.LastPersistedOffset
	lastPersistedSegment := partition.LastPersistedSegment
	messagesToFlush := partition.Messages
	if len(messagesToFlush) == 0 {
		partition.partitionMu.RUnlock()
		log.Println("No messages to flush for topic", topicName, "partition", partitionKey)
		return nil // Nothing to flush
	}
	segmentIndexEntry, err := partition.GetSegment(lastPersistedSegment)
	if err != nil {
		partition.partitionMu.RUnlock()
		return errors.New("Failed to get segment index: " + err.Error())
	}

	segmentFile, err := createSegmentFile(b.port, topicName, partitionKey, lastPersistedSegment)
	if err != nil {
		partition.partitionMu.RUnlock()
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
			partition.createNewSegment(msg.Offset)
			segmentFile, err = createSegmentFile(b.port, topicName, partitionKey, msg.Offset)
			if err != nil {
				partition.partitionMu.RUnlock()
				return errors.New("Failed to create new segment file: " + err.Error())
			}
		}
		segmentFile.Write(msg.Data)
		segmentFile.Write([]byte("\n")) // Write a newline after each message (good for debugging but may be harder to parse later)
		partition.LastPersistedOffset = msg.Offset
		segmentIndexEntry.Size += len(msg.Data)
	}
	segmentFile.Close()
	partition.partitionMu.RUnlock()
	return nil
}

func (b *brokerServer) flushTicker() {
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Println("Flush ticker triggered - flushing all logs")
			b.flushAllLogs()
		case <-b.ctx.Done():
			log.Println("Flush ticker stopping due to broker shutdown")
			return
		}
	}
}

func (b *brokerServer) flushAllLogs() {
	for topicName, topic := range b.Topics {
		for partitionKey := range topic.Partitions {
			if err := b.flushLogs(topicName, partitionKey); err != nil {
				log.Printf("Error flushing logs for topic %s partition %d: %v", topicName, partitionKey, err)
			}
		}
	}
}
