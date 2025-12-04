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
	Topics          map[string]*Topic
	ClusterMetadata ClusterMetadata
	port            Port
	etcdClient      *clientv3.Client
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
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

// TODO : Implement PartitionMetadata struct
// type PartitionMetadata struct {
// 	brokerPort int
// 	brokerAddr string
// }

func NewBrokerServer() *brokerServer {
	log.Println("Creating new BrokerServer instance...")
	return &brokerServer{
		Topics: make(map[string]*Topic), // Initialize the map
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
	log.Println("Broker shut down complete...")
}

func (b *brokerServer) fetchBrokerMetadata() error {
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
	err = b.fetchController()
	if err != nil {
		log.Printf("Error fetching controller info: %v", err)
	}

	return nil
}

func (b *brokerServer) fetchController() error {
	// Fetch controller port
	controllerResp, err := b.etcdClient.Get(context.Background(), "controller")
	if err != nil {
		log.Fatalf("Failed to get controller key from etcd: %v", err)
	}
	if len(controllerResp.Kvs) > 0 {
		controllerPort, err := strconv.Atoi(string(controllerResp.Kvs[0].Value))
		if err != nil {
			log.Printf("Invalid controller port in etcd key: %s", controllerResp.Kvs[0].Value)
		} else {
			b.ClusterMetadata.BrokersMetadata.Controller = Port(controllerPort)
		}
	} else {
		log.Println("Controller key not found in etcd during metadata fetch")
		// Create the controller key
		_, err := b.etcdClient.Put(context.Background(), "controller", strconv.Itoa(int(b.port)))
		if err != nil {
			log.Printf("Failed to create controller key in etcd: %v", err)
		}
		log.Println("Controller key created in etcd")

	}
	return nil
}

func (b *brokerServer) fetchTopicMetadata() error {
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

// Fetch metadata from etcd and populate local ClusterMetadata struct
func (b *brokerServer) FetchMetadata() {
	log.Println("Fetching metadata...")

	// Fetch broker info
	if err := b.fetchBrokerMetadata(); err != nil {
		log.Printf("Error fetching broker metadata: %v", err)
	}

	// Fetch topic info
	if err := b.fetchTopicMetadata(); err != nil {
		log.Printf("Error fetching topic metadata: %v", err)
	}

	log.Println("ClusterMetadata fetch complete.")
}
