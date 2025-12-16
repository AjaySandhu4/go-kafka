// Mostly copilot generated tests for broker.go

package broker

import (
	"context"
	"go-kafka/cluster"
	producerpb "go-kafka/proto/producer"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Test NewBrokerServer constructor
func TestNewBrokerServer(t *testing.T) {
	broker := NewBrokerServer()

	if broker == nil {
		t.Fatal("NewBrokerServer() returned nil")
	}

	if broker.Topics == nil {
		t.Error("Topics map not initialized")
	}

	if len(broker.Topics) != 0 {
		t.Errorf("Expected empty Topics map, got %d topics", len(broker.Topics))
	}
}

// Test CreateTopic without etcd (should fail gracefully)
func TestCreateTopic_NoEtcd(t *testing.T) {
	broker := NewBrokerServer()

	req := &producerpb.CreateTopicRequest{
		Topic:         "test-topic",
		NumPartitions: 3,
	}

	resp, err := broker.CreateTopic(context.Background(), req)

	if err != nil {
		t.Logf("CreateTopic returned expected error without etcd: %v", err)
		return
	}

	if resp.Success {
		t.Error("Expected CreateTopic to fail without etcd client, but it succeeded")
	}
}

// Test CreateTopic with mock etcd setup
func TestCreateTopic_WithEtcd(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	req := &producerpb.CreateTopicRequest{
		Topic:         "test-topic",
		NumPartitions: 3,
	}

	resp, err := broker.CreateTopic(context.Background(), req)

	if err != nil {
		t.Fatalf("CreateTopic returned error: %v", err)
	}

	if !resp.Success {
		t.Error("CreateTopic failed")
	}

	// Verify topic was created
	topic, exists := broker.Topics[req.Topic]
	if !exists {
		t.Fatal("Topic was not created in broker")
	}

	if topic.Name != req.Topic {
		t.Errorf("Expected topic name %s, got %s", req.Topic, topic.Name)
	}

	if topic.NumPartitions != int(req.NumPartitions) {
		t.Errorf("Expected %d partitions, got %d", req.NumPartitions, topic.NumPartitions)
	}

	// Verify topic was stored in etcd
	resp2, err := broker.etcdClient.Get(context.Background(), "/topic/"+req.Topic)
	if err != nil {
		t.Fatalf("Failed to get topic from etcd: %v", err)
	}

	if len(resp2.Kvs) == 0 {
		t.Error("Topic not found in etcd")
	}
}

// Test CreateTopic with duplicate topic name
func TestCreateTopic_Duplicate(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	req := &producerpb.CreateTopicRequest{
		Topic:         "duplicate-topic",
		NumPartitions: 3,
	}

	// Create topic first time
	resp1, err := broker.CreateTopic(context.Background(), req)
	if err != nil {
		t.Fatalf("First CreateTopic returned error: %v", err)
	}
	if !resp1.Success {
		t.Error("First CreateTopic failed")
	}

	// Try to create same topic again
	resp2, err := broker.CreateTopic(context.Background(), req)
	if err != nil {
		t.Fatalf("Second CreateTopic returned error: %v", err)
	}
	if resp2.Success {
		t.Error("Expected second CreateTopic to fail, but it succeeded")
	}
}

// Test PublishMessage to non-existent topic
func TestPublishMessage_TopicNotFound(t *testing.T) {
	broker := NewBrokerServer()

	req := &producerpb.PublishRequest{
		Topic:        "non-existent-topic",
		PartitionKey: 0,
		Message:      "test message",
	}

	resp, err := broker.PublishMessage(context.Background(), req)

	if err != nil {
		t.Fatalf("PublishMessage returned error: %v", err)
	}

	if resp.Success {
		t.Error("Expected PublishMessage to fail for non-existent topic, but it succeeded")
	}
}

// Test PublishMessage successfully
func TestPublishMessage_Success(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic first
	topicName := "test-topic"
	numPartitions := int32(3)
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: numPartitions,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Get partition assignment for this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker in this test run")
	}

	// Publish message to assigned partition
	publishReq := &producerpb.PublishRequest{
		Topic:        topicName,
		PartitionKey: int32(assignedPartition),
		Message:      "Hello, Kafka!",
	}

	resp, err := broker.PublishMessage(context.Background(), publishReq)

	if err != nil {
		t.Fatalf("PublishMessage returned error: %v", err)
	}

	if !resp.Success {
		t.Error("PublishMessage failed")
	}

	// Verify message was stored
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	if partition == nil {
		t.Fatal("Partition was not created")
	}

	if len(partition.Messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(partition.Messages))
	}

	if string(partition.Messages[0].Data) != publishReq.Message {
		t.Errorf("Expected message %s, got %s", publishReq.Message, string(partition.Messages[0].Data))
	}
}

// Test PublishMessage to wrong partition (not owned by broker)
func TestPublishMessage_WrongPartition(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "test-topic"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 3,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Try to publish to a partition not owned by this broker
	var wrongPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port != broker.port {
			wrongPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("All partitions assigned to this broker in this test run")
	}

	publishReq := &producerpb.PublishRequest{
		Topic:        topicName,
		PartitionKey: int32(wrongPartition),
		Message:      "This should fail",
	}

	resp, err := broker.PublishMessage(context.Background(), publishReq)

	if err != nil {
		t.Fatalf("PublishMessage returned error: %v", err)
	}

	if resp.Success {
		t.Error("Expected PublishMessage to fail for wrong partition, but it succeeded")
	}
}

// Test multiple messages to same partition
func TestPublishMessage_MultipleMessages(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "test-topic"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 3,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish multiple messages
	messages := []string{"Message 1", "Message 2", "Message 3"}
	for _, msg := range messages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msg,
		}

		resp, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("PublishMessage returned error: %v", err)
		}
		if !resp.Success {
			t.Errorf("PublishMessage failed for message: %s", msg)
		}
	}

	// Verify all messages were stored
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	if len(partition.Messages) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(partition.Messages))
	}

	for i, msg := range messages {
		if string(partition.Messages[i].Data) != msg {
			t.Errorf("Message %d: expected %s, got %s", i, msg, string(partition.Messages[i].Data))
		}
	}

	// Segment index entries are only created when logs are flushed to disk
	// Before flushing, the segment index should be empty
	if len(partition.SegmentIndex) != 0 {
		t.Errorf("Expected 0 segment index entries before flush, got %d", len(partition.SegmentIndex))
	}
}

// Test GetMetadata
func TestGetMetadata(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create a couple of topics
	topics := []string{"topic1", "topic2"}
	for _, topicName := range topics {
		createReq := &producerpb.CreateTopicRequest{
			Topic:         topicName,
			NumPartitions: 3,
		}
		_, err := broker.CreateTopic(context.Background(), createReq)
		if err != nil {
			t.Fatalf("Failed to create topic %s: %v", topicName, err)
		}
	}

	// Get metadata
	resp, err := broker.GetMetadata(context.Background(), &emptypb.Empty{})

	if err != nil {
		t.Fatalf("GetMetadata returned error: %v", err)
	}

	// Verify topics in metadata
	if len(resp.Topics) != len(topics) {
		t.Errorf("Expected %d topics in metadata, got %d", len(topics), len(resp.Topics))
	}

	// Verify brokers in metadata
	if len(resp.Brokers) == 0 {
		t.Error("Expected at least one broker in metadata")
	}
}

// Test partition index tracking
func TestPartitionIndex(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "test-topic"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages of different sizes
	messages := []string{"short", "medium message", "this is a longer message"}
	expectedOffsets := []int64{}
	currentOffset := int64(0)

	for _, msg := range messages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msg,
		}

		expectedOffsets = append(expectedOffsets, currentOffset)
		currentOffset += int64(MessageHeaderSize) + int64(len(msg))

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("PublishMessage failed: %v", err)
		}
	}

	// Flush the logs to create segment index entries
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Verify segment index entry (should have one segment containing all messages)
	if len(partition.SegmentIndex) != 1 {
		t.Fatalf("Expected 1 segment index entry after flush, got %d", len(partition.SegmentIndex))
	}

	// Verify the single segment starts at offset 0
	if partition.SegmentIndex[0].StartOffset != 0 {
		t.Errorf("Expected segment start offset 0, got %d", partition.SegmentIndex[0].StartOffset)
	}

	// Verify the segment contains all messages
	expectedTotalSize := int64(0)
	for _, msg := range messages {
		expectedTotalSize += int64(MessageHeaderSize) + int64(len(msg))
	}
	if partition.SegmentIndex[0].Size != expectedTotalSize {
		t.Errorf("Expected segment size %d, got %d", expectedTotalSize, partition.SegmentIndex[0].Size)
	}

	// Verify NextOffset
	if partition.NextOffset != currentOffset {
		t.Errorf("Expected NextOffset %d, got %d", currentOffset, partition.NextOffset)
	}

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test concurrent message publishing
func TestPublishMessage_Concurrent(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "test-topic"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages concurrently
	numMessages := 50
	done := make(chan bool, numMessages)

	for i := 0; i < numMessages; i++ {
		go func(msgNum int) {
			publishReq := &producerpb.PublishRequest{
				Topic:        topicName,
				PartitionKey: int32(assignedPartition),
				Message:      "Message " + string(rune(msgNum)),
			}

			_, err := broker.PublishMessage(context.Background(), publishReq)
			if err != nil {
				t.Errorf("Concurrent PublishMessage failed: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numMessages; i++ {
		<-done
	}

	// Verify all messages were stored
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	if len(partition.Messages) != numMessages {
		t.Errorf("Expected %d messages after concurrent writes, got %d", numMessages, len(partition.Messages))
	}
}

// Test controller election - single broker becomes controller
func TestControllerElection_SingleBroker(t *testing.T) {
	broker := NewBrokerServer()

	// Connect to etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("Skipping test: etcd not available: %v", err)
	}
	defer etcdClient.Close()

	// Clean up any existing controller key
	_, err = etcdClient.Delete(context.Background(), "controller")
	if err != nil {
		t.Fatalf("Failed to clean up controller key: %v", err)
	}

	broker.etcdClient = etcdClient
	broker.ctx, broker.cancel = context.WithCancel(context.Background())
	defer broker.cancel()

	// Grant a lease for the broker
	leaseResp, err := etcdClient.Grant(context.Background(), 10)
	if err != nil {
		t.Fatalf("Failed to grant lease: %v", err)
	}
	broker.leaseID = leaseResp.ID
	broker.port = cluster.Port(8080)

	broker.ClusterMetadata = cluster.ClusterMetadata{
		TopicsMetadata:  &cluster.TopicsMetadata{Topics: make(map[string]*cluster.TopicMetadata)},
		BrokersMetadata: &cluster.BrokersMetadata{Brokers: make(map[cluster.Port]*cluster.BrokerMetadata)},
	}

	// Try to elect controller
	err = broker.fetchOrElectController()
	if err != nil {
		t.Fatalf("Failed to elect controller: %v", err)
	}

	if broker.ClusterMetadata.BrokersMetadata.Controller != broker.port {
		t.Errorf("Expected controller port to be %d, got %d", broker.port, broker.ClusterMetadata.BrokersMetadata.Controller)
	}

	// Verify controller key exists in etcd
	resp, err := etcdClient.Get(context.Background(), "controller")
	if err != nil {
		t.Fatalf("Failed to get controller key: %v", err)
	}

	if len(resp.Kvs) == 0 {
		t.Fatal("Controller key not found in etcd")
	}

	controllerPort, err := strconv.Atoi(string(resp.Kvs[0].Value))
	if err != nil {
		t.Fatalf("Failed to parse controller port: %v", err)
	}

	if cluster.Port(controllerPort) != broker.port {
		t.Errorf("Expected controller port in etcd to be %d, got %d", broker.port, controllerPort)
	}

	// Clean up
	_, _ = etcdClient.Delete(context.Background(), "controller")
}

// Test controller election - second broker doesn't become controller
func TestControllerElection_SecondBrokerFails(t *testing.T) {
	// Connect to etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("Skipping test: etcd not available: %v", err)
	}
	defer etcdClient.Close()

	// Clean up any existing controller key
	_, _ = etcdClient.Delete(context.Background(), "controller")

	// Create first broker and make it controller
	broker1 := NewBrokerServer()
	broker1.etcdClient = etcdClient
	broker1.ctx, broker1.cancel = context.WithCancel(context.Background())
	defer broker1.cancel()

	leaseResp1, err := etcdClient.Grant(context.Background(), 10)
	if err != nil {
		t.Fatalf("Failed to grant lease for broker1: %v", err)
	}
	broker1.leaseID = leaseResp1.ID
	broker1.port = cluster.Port(8080)
	broker1.ClusterMetadata = cluster.ClusterMetadata{
		TopicsMetadata:  &cluster.TopicsMetadata{Topics: make(map[string]*cluster.TopicMetadata)},
		BrokersMetadata: &cluster.BrokersMetadata{Brokers: make(map[cluster.Port]*cluster.BrokerMetadata)},
	}

	// First broker becomes controller
	err = broker1.fetchOrElectController()
	if err != nil {
		t.Fatalf("Broker1 failed to elect controller: %v", err)
	}

	// Create second broker
	broker2 := NewBrokerServer()
	broker2.etcdClient = etcdClient
	broker2.ctx, broker2.cancel = context.WithCancel(context.Background())
	defer broker2.cancel()

	leaseResp2, err := etcdClient.Grant(context.Background(), 10)
	if err != nil {
		t.Fatalf("Failed to grant lease for broker2: %v", err)
	}
	broker2.leaseID = leaseResp2.ID
	broker2.port = cluster.Port(8081)
	broker2.ClusterMetadata = cluster.ClusterMetadata{
		TopicsMetadata:  &cluster.TopicsMetadata{Topics: make(map[string]*cluster.TopicMetadata)},
		BrokersMetadata: &cluster.BrokersMetadata{Brokers: make(map[cluster.Port]*cluster.BrokerMetadata)},
	}

	// Second broker tries to become controller
	err = broker2.fetchOrElectController()
	if err != nil {
		t.Fatalf("Broker2 failed to check controller: %v", err)
	}

	if broker2.ClusterMetadata.BrokersMetadata.Controller == broker2.port {
		t.Error("Expected broker2 to NOT become controller when controller already exists")
	}

	// Verify broker2 knows who the controller is
	if broker2.ClusterMetadata.BrokersMetadata.Controller != broker1.port {
		t.Errorf("Expected broker2 to know controller is %d, got %d",
			broker1.port, broker2.ClusterMetadata.BrokersMetadata.Controller)
	}

	// Clean up
	_, _ = etcdClient.Delete(context.Background(), "controller")
}

// Test controller election with lease expiration
func TestControllerElection_LeaseExpiration(t *testing.T) {
	// Connect to etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("Skipping test: etcd not available: %v", err)
	}
	defer etcdClient.Close()

	// Clean up any existing controller key
	_, _ = etcdClient.Delete(context.Background(), "controller")

	// Create first broker with SHORT lease (2 seconds)
	broker1 := NewBrokerServer()
	broker1.etcdClient = etcdClient
	broker1.ctx, broker1.cancel = context.WithCancel(context.Background())

	leaseResp1, err := etcdClient.Grant(context.Background(), 2) // 2 second lease
	if err != nil {
		t.Fatalf("Failed to grant lease for broker1: %v", err)
	}
	broker1.leaseID = leaseResp1.ID
	broker1.port = cluster.Port(8080)
	broker1.ClusterMetadata = cluster.ClusterMetadata{
		TopicsMetadata:  &cluster.TopicsMetadata{Topics: make(map[string]*cluster.TopicMetadata)},
		BrokersMetadata: &cluster.BrokersMetadata{Brokers: make(map[cluster.Port]*cluster.BrokerMetadata)},
	}

	// First broker becomes controller
	err = broker1.fetchOrElectController()
	if err != nil {
		t.Fatalf("Broker1 failed to elect controller: %v", err)
	}

	if broker1.ClusterMetadata.BrokersMetadata.Controller != broker1.port {
		t.Fatal("Expected broker1 to become controller")
	}

	t.Log("Broker1 is controller, stopping broker (simulating crash)...")

	// Simulate broker1 crash by canceling context (stops keepalive)
	broker1.cancel()

	// Wait for lease to expire (2 seconds + some buffer)
	t.Log("Waiting for lease to expire (3 seconds)...")
	time.Sleep(3 * time.Second)

	// Verify controller key is gone
	resp, err := etcdClient.Get(context.Background(), "controller")
	if err != nil {
		t.Fatalf("Failed to get controller key: %v", err)
	}

	if len(resp.Kvs) > 0 {
		t.Error("Controller key should have been deleted after lease expiration")
	}

	// Create second broker
	t.Log("Starting broker2 after lease expiration...")
	broker2 := NewBrokerServer()
	broker2.etcdClient = etcdClient
	broker2.ctx, broker2.cancel = context.WithCancel(context.Background())
	defer broker2.cancel()

	leaseResp2, err := etcdClient.Grant(context.Background(), 10)
	if err != nil {
		t.Fatalf("Failed to grant lease for broker2: %v", err)
	}
	broker2.leaseID = leaseResp2.ID
	broker2.port = cluster.Port(8081)
	broker2.ClusterMetadata = cluster.ClusterMetadata{
		TopicsMetadata:  &cluster.TopicsMetadata{Topics: make(map[string]*cluster.TopicMetadata)},
		BrokersMetadata: &cluster.BrokersMetadata{Brokers: make(map[cluster.Port]*cluster.BrokerMetadata)},
	}

	// Second broker should become controller
	err = broker2.fetchOrElectController()
	if err != nil {
		t.Fatalf("Broker2 failed to elect controller: %v", err)
	}

	if broker2.ClusterMetadata.BrokersMetadata.Controller != broker2.port {
		t.Errorf("Expected controller port to be %d, got %d",
			broker2.port, broker2.ClusterMetadata.BrokersMetadata.Controller)
	}

	// Clean up
	_, _ = etcdClient.Delete(context.Background(), "controller")
}

// Test multiple brokers racing for controller election
func TestControllerElection_ConcurrentRace(t *testing.T) {
	// Connect to etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("Skipping test: etcd not available: %v", err)
	}
	defer etcdClient.Close()

	// Clean up any existing controller key
	_, _ = etcdClient.Delete(context.Background(), "controller")

	numBrokers := 5
	brokers := make([]*brokerServer, numBrokers)
	controllerCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Create multiple brokers concurrently trying to become controller
	for i := 0; i < numBrokers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			broker := NewBrokerServer()
			broker.etcdClient = etcdClient
			broker.ctx, broker.cancel = context.WithCancel(context.Background())

			leaseResp, err := etcdClient.Grant(context.Background(), 10)
			if err != nil {
				t.Errorf("Failed to grant lease for broker%d: %v", index, err)
				return
			}
			broker.leaseID = leaseResp.ID
			broker.port = cluster.Port(8080 + index)
			broker.ClusterMetadata = cluster.ClusterMetadata{
				TopicsMetadata:  &cluster.TopicsMetadata{Topics: make(map[string]*cluster.TopicMetadata)},
				BrokersMetadata: &cluster.BrokersMetadata{Brokers: make(map[cluster.Port]*cluster.BrokerMetadata)},
			}

			// Try to become controller
			err = broker.fetchOrElectController()
			if err != nil {
				t.Errorf("Broker%d failed election: %v", index, err)
				return
			}

			mu.Lock()
			brokers[index] = broker
			if broker.ClusterMetadata.BrokersMetadata.Controller == broker.port {
				controllerCount++
				t.Logf("Broker%d (port %d) became controller", index, broker.port)
			} else {
				t.Logf("Broker%d (port %d) did NOT become controller", index, broker.port)
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify only ONE broker became controller
	if controllerCount != 1 {
		t.Errorf("Expected exactly 1 controller, got %d", controllerCount)
	}

	// Verify all non-controller brokers know who the controller is
	var controllerPort cluster.Port
	for _, broker := range brokers {
		if broker == nil {
			continue
		}
		if broker.ClusterMetadata.BrokersMetadata.Controller != 0 {
			if controllerPort == 0 {
				controllerPort = broker.ClusterMetadata.BrokersMetadata.Controller
			} else if controllerPort != broker.ClusterMetadata.BrokersMetadata.Controller {
				t.Errorf("Brokers disagree on controller: %d vs %d",
					controllerPort, broker.ClusterMetadata.BrokersMetadata.Controller)
			}
		}
	}

	// Clean up
	for _, broker := range brokers {
		if broker != nil && broker.cancel != nil {
			broker.cancel()
		}
	}
	_, _ = etcdClient.Delete(context.Background(), "controller")
}

// Test log flushing - single partition
func TestFlushLogs_SinglePartition(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "flush-test-topic"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish some messages
	messages := []string{"Message 1", "Message 2", "Message 3"}
	for _, msg := range messages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msg,
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Flush logs
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Verify log file was created
	logPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition)) + "/segment_0.log"
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Errorf("Log file was not created at %s", logPath)
	}

	// Verify LastPersistedOffset was updated
	if partition.LastPersistedOffset < 0 {
		t.Errorf("LastPersistedOffset not updated after flush")
	}

	// Clean up log files
	os.RemoveAll("broker/broker_logs")
}

// Test log flushing - multiple partitions
func TestFlushLogs_MultiplePartitions(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic with multiple partitions
	topicName := "multi-partition-flush-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 3,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Publish messages to all partitions assigned to this broker
	assignedPartitions := []cluster.PartitionKey{}
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartitions = append(assignedPartitions, partKey)
		}
	}

	if len(assignedPartitions) == 0 {
		t.Skip("No partitions assigned to this broker")
	}

	for _, partKey := range assignedPartitions {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(partKey),
			Message:      "Test message for partition " + strconv.Itoa(int(partKey)),
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message to partition %d: %v", partKey, err)
		}
	}

	// Flush all logs
	broker.flushAllLogs()

	// Verify log files were created for each partition
	for _, partKey := range assignedPartitions {
		logPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(partKey)) + "/segment_0.log"
		if _, err := os.Stat(logPath); os.IsNotExist(err) {
			t.Errorf("Log file was not created for partition %d at %s", partKey, logPath)
		}
	}

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test log flushing - segment rotation
func TestFlushLogs_SegmentRotation(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "segment-rotation-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish many large messages to trigger segment rotation
	// SegmentSize is 10KB (10240 bytes)
	largeMessage := string(make([]byte, 3000)) // 3KB message
	numMessages := 5                           // 15KB total, should create 2 segments

	for i := 0; i < numMessages; i++ {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      largeMessage + strconv.Itoa(i),
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Flush logs
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Verify multiple segment files were created
	segmentCount := 0
	dirPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition))

	files, err := os.ReadDir(dirPath)
	if err != nil {
		t.Fatalf("Failed to read log directory: %v", err)
	}

	for _, file := range files {
		if !file.IsDir() && len(file.Name()) > 4 && file.Name()[len(file.Name())-4:] == ".log" {
			segmentCount++
		}
	}

	if segmentCount < 2 {
		t.Errorf("Expected at least 2 segment files due to rotation, got %d", segmentCount)
	}

	t.Logf("Created %d segment files", segmentCount)

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test log flushing - empty partition
func TestFlushLogs_EmptyPartition(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "empty-flush-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Try to flush logs without any messages
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	if partition == nil {
		// Partition doesn't exist yet (created on first message)
		t.Log("Partition not yet created, which is expected")
		return
	}

	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush empty partition: %v", err)
	}

	// Verify no log file was created
	logPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition)) + "/segment_0.log"
	if _, err := os.Stat(logPath); err == nil {
		t.Errorf("Log file should not be created for empty partition")
	}

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test log flushing - idempotent flushes
func TestFlushLogs_Idempotent(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "idempotent-flush-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages
	messages := []string{"Message 1", "Message 2", "Message 3"}
	for _, msg := range messages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msg,
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	partition := broker.Topics[topicName].Partitions[assignedPartition]

	// First flush
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("First flush failed: %v", err)
	}

	firstPersistedOffset := partition.LastPersistedOffset

	// Second flush (should be idempotent - not write messages again)
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Second flush failed: %v", err)
	}

	secondPersistedOffset := partition.LastPersistedOffset

	// Verify offset didn't change (no new messages were persisted)
	if firstPersistedOffset != secondPersistedOffset {
		t.Errorf("Expected LastPersistedOffset to remain %d after second flush, got %d",
			firstPersistedOffset, secondPersistedOffset)
	}

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test log flushing - concurrent flushes
func TestFlushLogs_Concurrent(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "concurrent-flush-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages
	for i := 0; i < 20; i++ {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      "Message " + strconv.Itoa(i),
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	partition := broker.Topics[topicName].Partitions[assignedPartition]

	// Perform concurrent flushes
	var wg sync.WaitGroup
	numFlushes := 5
	errors := make(chan error, numFlushes)

	for i := 0; i < numFlushes; i++ {
		wg.Add(1)
		go func(flushNum int) {
			defer wg.Done()
			err := partition.flushLogs(broker.port, topicName, assignedPartition)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent flush error: %v", err)
	}

	// Verify log file exists and contains data
	logPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition)) + "/segment_0.log"
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Errorf("Log file was not created at %s", logPath)
	}

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test flush ticker
func TestFlushTicker(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Initialize context for ticker
	broker.ctx, broker.cancel = context.WithCancel(context.Background())
	defer broker.cancel()

	// Create topic
	topicName := "ticker-flush-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages
	publishReq := &producerpb.PublishRequest{
		Topic:        topicName,
		PartitionKey: int32(assignedPartition),
		Message:      "Test message for ticker",
	}

	_, err = broker.PublishMessage(context.Background(), publishReq)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Start flush ticker with short interval for testing
	originalFlushInterval := FlushInterval
	// We can't modify the const, so we'll just test that flushTicker responds to context cancellation

	tickerDone := make(chan bool)
	go func() {
		broker.flushTicker()
		tickerDone <- true
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the context to stop the ticker
	broker.cancel()

	// Wait for ticker to stop
	select {
	case <-tickerDone:
		t.Log("Flush ticker stopped successfully")
	case <-time.After(2 * time.Second):
		t.Error("Flush ticker did not stop within timeout")
	}

	// Restore original interval (though it's const)
	_ = originalFlushInterval

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test flushAllLogs with multiple topics
func TestFlushAllLogs_MultipleTopics(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create multiple topics
	topics := []string{"topic-a", "topic-b", "topic-c"}
	for _, topicName := range topics {
		createReq := &producerpb.CreateTopicRequest{
			Topic:         topicName,
			NumPartitions: 2,
		}

		_, err := broker.CreateTopic(context.Background(), createReq)
		if err != nil {
			t.Fatalf("Failed to create topic %s: %v", topicName, err)
		}
	}

	// Publish messages to all topics
	for _, topicName := range topics {
		for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
			if port == broker.port {
				publishReq := &producerpb.PublishRequest{
					Topic:        topicName,
					PartitionKey: int32(partKey),
					Message:      "Message for " + topicName + " partition " + strconv.Itoa(int(partKey)),
				}

				_, err := broker.PublishMessage(context.Background(), publishReq)
				if err != nil {
					t.Fatalf("Failed to publish to %s partition %d: %v", topicName, partKey, err)
				}
			}
		}
	}

	// Flush all logs
	broker.flushAllLogs()

	// Verify log files were created
	flushedCount := 0
	for _, topicName := range topics {
		for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
			if port == broker.port {
				logPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(partKey)) + "/segment_0.log"
				if _, err := os.Stat(logPath); err == nil {
					flushedCount++
				}
			}
		}
	}

	if flushedCount == 0 {
		t.Error("Expected at least one log file to be created")
	}

	t.Logf("Successfully flushed %d partitions across %d topics", flushedCount, len(topics))

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test reading raw log files - verify content matches
func TestFlushLogs_VerifyRawLogContent(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "raw-log-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages with known content
	expectedMessages := []string{"First message", "Second message", "Third message"}
	for _, msg := range expectedMessages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msg,
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Flush logs
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Read raw log file
	logPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition)) + "/segment_0.log"
	rawData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read raw log file: %v", err)
	}

	// Verify total size matches expected (headers + data)
	expectedSize := int64(len(expectedMessages) * MessageHeaderSize)
	for _, msg := range expectedMessages {
		expectedSize += int64(len(msg))
	}

	if int64(len(rawData)) != expectedSize {
		t.Errorf("Expected raw log size %d bytes (with headers), got %d bytes", expectedSize, len(rawData))
	}

	// Read and verify messages from raw log using headers
	rawLogFile, err := os.OpenFile(logPath, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open raw log file for reading: %v", err)
	}
	defer rawLogFile.Close()

	sf := &SegmentFile{raw: rawLogFile}
	for i, expectedMsg := range expectedMessages {
		msg, err := sf.Read()
		if err != nil {
			t.Fatalf("Failed to read message %d from raw log: %v", i, err)
		}
		if string(msg.Data) != expectedMsg {
			t.Errorf("Message %d content mismatch. Expected '%s', got '%s'", i, expectedMsg, string(msg.Data))
		}
	}

	t.Logf("Raw log file verified: %d bytes containing %d messages", len(rawData), len(expectedMessages))

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test reading raw log files - verify offsets are correct
func TestFlushLogs_VerifyOffsets(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "offset-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages of different sizes to test offset calculation
	messages := []struct {
		content        string
		expectedOffset int64
	}{
		{"A", 0},                             // Offset 0, header(20) + size 1 = 21
		{"BC", int64(MessageHeaderSize + 1)}, // Offset 21, header(20) + size 2 = 42
		{"DEF", int64(2*MessageHeaderSize + 1 + 2)},                // Offset 43, header(20) + size 3 = 63
		{"GHIJ", int64(3*MessageHeaderSize + 1 + 2 + 3)},           // Offset 66, header(20) + size 4 = 86
		{"KLMNOPQRST", int64(4*MessageHeaderSize + 1 + 2 + 3 + 4)}, // Offset 90, header(20) + size 10 = 100
	}

	for _, msg := range messages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msg.content,
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Verify in-memory offsets before flush
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	for i, msg := range messages {
		if partition.Messages[i].Header.Offset != msg.expectedOffset {
			t.Errorf("Message %d: expected offset %d, got %d", i, msg.expectedOffset, partition.Messages[i].Header.Offset)
		}
		if string(partition.Messages[i].Data) != msg.content {
			t.Errorf("Message %d: expected content %s, got %s", i, msg.content, string(partition.Messages[i].Data))
		}
	}

	// Segment index is only created when logs are flushed
	if len(partition.SegmentIndex) != 0 {
		t.Errorf("Expected 0 segment index entries before flush, got %d", len(partition.SegmentIndex))
	}

	// Flush logs
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Verify segment index after flush (should have one segment)
	if len(partition.SegmentIndex) != 1 {
		t.Fatalf("Expected 1 segment index entry after flush, got %d", len(partition.SegmentIndex))
	}

	// Verify the segment starts at offset 0
	if partition.SegmentIndex[0].StartOffset != 0 {
		t.Errorf("Expected segment start offset 0, got %d", partition.SegmentIndex[0].StartOffset)
	}
	// Read raw log file and verify it contains all messages with headers
	// Note: The raw file now stores: [header][data][header][data]...
	logPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition)) + "/segment_0.log"
	rawData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read raw log file: %v", err)
	}

	// Calculate expected file size: sum of (header + data) for each message
	expectedFileSize := 0
	for _, msg := range messages {
		expectedFileSize += MessageHeaderSize + len(msg.content)
	}

	if len(rawData) != expectedFileSize {
		t.Errorf("Raw file size mismatch. Expected %d bytes, got %d", expectedFileSize, len(rawData))
	}

	// Verify we can extract each message using the segment file reader
	// Open file in read-only mode for reading
	rawLogFile, err := os.OpenFile(logPath, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open segment file for reading: %v", err)
	}
	defer rawLogFile.Close()

	segmentFile := &SegmentFile{raw: rawLogFile}

	// Read back each message and verify
	for i, expectedMsg := range messages {
		msg, err := segmentFile.Read()
		if err != nil {
			t.Fatalf("Failed to read message %d: %v", i, err)
		}

		// Verify logical offset
		if msg.Header.Offset != expectedMsg.expectedOffset {
			t.Errorf("Message %d: expected offset %d, got %d", i, expectedMsg.expectedOffset, msg.Header.Offset)
		}

		// Verify data content
		if string(msg.Data) != expectedMsg.content {
			t.Errorf("Message %d: expected content '%s', got '%s'", i, expectedMsg.content, string(msg.Data))
		}

		// Verify data size
		if msg.Header.DataSize != uint32(len(expectedMsg.content)) {
			t.Errorf("Message %d: expected data size %d, got %d", i, len(expectedMsg.content), msg.Header.DataSize)
		}
	}

	t.Logf("Successfully verified %d messages with correct offsets in raw log file", len(messages))

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test segment rotation with offset continuity
func TestFlushLogs_SegmentRotationOffsetContinuity(t *testing.T) {
	// Clean up any existing log files first
	os.RemoveAll("broker/broker_logs")

	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "rotation-offset-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages that will trigger segment rotation
	// SegmentSize is 10KB (10240 bytes)
	largeMessage := string(make([]byte, 3500)) // 3.5KB each
	numMessages := 4                           // 14KB total, should create 2 segments

	// With 3.5KB messages and 10KB segment size:
	// - Messages 0,1 fit in segment 0 (7KB total + 2*20 bytes header)
	// - Message 2 would exceed 10KB, so rotation happens
	// - Messages 2,3 go to next segment
	expectedSegmentOffsets := []int64{0, int64(2*MessageHeaderSize + 2*len(largeMessage))} // Two segments

	currentOffset := int64(0)
	for i := 0; i < numMessages; i++ {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      largeMessage,
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}

		currentOffset += int64(MessageHeaderSize) + int64(len(largeMessage))
	}

	// Flush logs
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Verify multiple segments were created
	dirPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition))

	files, err := os.ReadDir(dirPath)
	if err != nil {
		t.Fatalf("Failed to read log directory: %v", err)
	}

	segmentFiles := []string{}
	for _, file := range files {
		if !file.IsDir() && len(file.Name()) > 4 && file.Name()[len(file.Name())-4:] == ".log" {
			segmentFiles = append(segmentFiles, file.Name())
		}
	}

	if len(segmentFiles) < 2 {
		t.Fatalf("Expected at least 2 segment files, got %d", len(segmentFiles))
	}

	t.Logf("Created %d segment files: %v", len(segmentFiles), segmentFiles)

	// Verify NextOffset is correct
	expectedFinalOffset := currentOffset
	if partition.NextOffset != expectedFinalOffset {
		t.Errorf("Expected NextOffset %d, got %d", expectedFinalOffset, partition.NextOffset)
	}

	// Verify all segment index entries have correct offsets
	// (messages are cleared from memory after flush, so we verify via segment index)
	if len(partition.SegmentIndex) != len(segmentFiles) {
		t.Errorf("Expected %d segment index entries, got %d", len(segmentFiles), len(partition.SegmentIndex))
	}

	// Verify segment offsets match expected
	if len(partition.SegmentIndex) >= 2 {
		if partition.SegmentIndex[0].StartOffset != expectedSegmentOffsets[0] {
			t.Errorf("Segment 0: expected start offset %d, got %d",
				expectedSegmentOffsets[0], partition.SegmentIndex[0].StartOffset)
		}
		if partition.SegmentIndex[1].StartOffset != expectedSegmentOffsets[1] {
			t.Errorf("Segment 1: expected start offset %d, got %d",
				expectedSegmentOffsets[1], partition.SegmentIndex[1].StartOffset)
		}
	}

	t.Logf("Verified offset continuity across %d segments with final offset %d",
		len(segmentFiles), expectedFinalOffset)

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test reading raw log files and verifying offsets
func TestFlushLogs_ReadRawLogFile(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "raw-log-read-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages with known content
	messages := []string{"Hello", "World", "Kafka"}

	for _, msg := range messages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msg,
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Flush logs
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Read the raw log file
	logPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition)) + "/segment_0.log"
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	// Verify the size (3 messages with headers: 20 bytes header + data for each)
	expectedSize := int64(3*MessageHeaderSize + len("Hello") + len("World") + len("Kafka"))
	if int64(len(logData)) != expectedSize {
		t.Errorf("Expected log size %d bytes (3 headers + data), got %d", expectedSize, len(logData))
	}

	// Verify we can read messages back using the segment file
	segmentFile, err := OpenSegmentFile(broker.port, topicName, assignedPartition, 0, false, true)
	if err != nil {
		t.Fatalf("Failed to open segment file for reading: %v", err)
	}
	defer segmentFile.Close()

	// Verify segment index has correct offsets and sizes
	if len(partition.SegmentIndex) != 1 {
		t.Fatalf("Expected 1 segment, got %d", len(partition.SegmentIndex))
	}

	if partition.SegmentIndex[0].StartOffset != 0 {
		t.Errorf("Expected segment start offset 0, got %d", partition.SegmentIndex[0].StartOffset)
	}

	if partition.SegmentIndex[0].Size != expectedSize {
		t.Errorf("Expected segment size %d, got %d", expectedSize, partition.SegmentIndex[0].Size)
	}

	t.Logf("Successfully verified raw log file: %d bytes with headers", len(logData))

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test reading raw log files across multiple segments
func TestFlushLogs_ReadRawLogFileMultipleSegments(t *testing.T) {
	// Clean up any existing log files first
	os.RemoveAll("broker/broker_logs")

	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "multi-segment-raw-read"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish large messages to trigger segment rotation
	// SegmentSize is 10KB (10240 bytes)
	largeMessage := string(make([]byte, 3500)) // 3.5KB message
	numMessages := 4                           // 14KB total, should create 2 segments

	for i := 0; i < numMessages; i++ {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      largeMessage + strconv.Itoa(i),
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Flush logs
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Read and verify segment files
	dirPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition))

	// Read segment 0
	segment0Path := dirPath + "/segment_0.log"
	segment0Data, err := os.ReadFile(segment0Path)
	if err != nil {
		t.Fatalf("Failed to read segment 0: %v", err)
	}

	// Segment 0 should contain first 2 messages with headers
	// Each message: 20 bytes header + 3501 bytes data = 3521 bytes
	// Total: 7042 bytes for 2 messages
	expectedSegment0Size := int64(2 * (MessageHeaderSize + len(largeMessage) + 1))
	if int64(len(segment0Data)) != expectedSegment0Size {
		t.Errorf("Segment 0 size mismatch. Expected %d, got %d", expectedSegment0Size, len(segment0Data))
	}

	// Read and verify messages from segment 0 using headers
	segFile0, err := os.OpenFile(segment0Path, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open segment 0 for reading: %v", err)
	}
	defer segFile0.Close()

	sf0 := &SegmentFile{raw: segFile0}
	msg0, err := sf0.Read()
	if err != nil {
		t.Fatalf("Failed to read message 0 from segment 0: %v", err)
	}
	if string(msg0.Data) != largeMessage+"0" {
		t.Errorf("Message 0 content mismatch")
	}
	if msg0.Header.Offset != 0 {
		t.Errorf("Message 0 offset: expected 0, got %d", msg0.Header.Offset)
	}

	msg1, err := sf0.Read()
	if err != nil {
		t.Fatalf("Failed to read message 1 from segment 0: %v", err)
	}
	if string(msg1.Data) != largeMessage+"1" {
		t.Errorf("Message 1 content mismatch")
	}

	t.Logf("Segment 0 verified: %d bytes, 2 messages", len(segment0Data))

	// Calculate expected offset for second segment (after first 2 messages with headers)
	expectedSegment1Offset := int64(2 * (MessageHeaderSize + len(largeMessage) + 1))
	segment1Path := dirPath + "/segment_" + strconv.FormatInt(expectedSegment1Offset, 10) + ".log"
	segment1Data, err := os.ReadFile(segment1Path)
	if err != nil {
		t.Fatalf("Failed to read segment at offset %d: %v", expectedSegment1Offset, err)
	}

	// Segment 1 should contain last 2 messages with headers
	expectedSegment1Size := int64(2 * (MessageHeaderSize + len(largeMessage) + 1))
	if int64(len(segment1Data)) != expectedSegment1Size {
		t.Errorf("Segment 1 size mismatch. Expected %d, got %d", expectedSegment1Size, len(segment1Data))
	}

	// Read and verify messages from segment 1 using headers
	segFile1, err := os.OpenFile(segment1Path, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open segment 1 for reading: %v", err)
	}
	defer segFile1.Close()

	sf1 := &SegmentFile{raw: segFile1}
	msg2, err := sf1.Read()
	if err != nil {
		t.Fatalf("Failed to read message 2 from segment 1: %v", err)
	}
	if string(msg2.Data) != largeMessage+"2" {
		t.Errorf("Message 2 content mismatch")
	}
	if msg2.Header.Offset != expectedSegment1Offset {
		t.Errorf("Message 2 offset: expected %d, got %d", expectedSegment1Offset, msg2.Header.Offset)
	}

	msg3, err := sf1.Read()
	if err != nil {
		t.Fatalf("Failed to read message 3 from segment 1: %v", err)
	}
	if string(msg3.Data) != largeMessage+"3" {
		t.Errorf("Message 3 content mismatch")
	}

	t.Logf("Segment at offset %d verified: %d bytes, 2 messages", expectedSegment1Offset, len(segment1Data))

	// Verify segment index
	if len(partition.SegmentIndex) != 2 {
		t.Fatalf("Expected 2 segments in index, got %d", len(partition.SegmentIndex))
	}

	// First segment should start at offset 0
	if partition.SegmentIndex[0].StartOffset != 0 {
		t.Errorf("Segment 0: expected start offset 0, got %d", partition.SegmentIndex[0].StartOffset)
	}

	// Second segment should start at calculated offset
	if partition.SegmentIndex[1].StartOffset != expectedSegment1Offset {
		t.Errorf("Segment 1: expected start offset %d, got %d", expectedSegment1Offset, partition.SegmentIndex[1].StartOffset)
	}

	// Verify NextOffset - should be after all 4 messages with their headers
	expectedNextOffset := int64(4*MessageHeaderSize + 4*(len(largeMessage)+1))
	if partition.NextOffset != expectedNextOffset {
		t.Errorf("Expected NextOffset %d, got %d", expectedNextOffset, partition.NextOffset)
	}

	t.Logf("Successfully verified %d segments with total %d bytes", len(partition.SegmentIndex), len(segment0Data)+len(segment1Data))

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Test verifying human-readable logs match raw logs
func TestFlushLogs_HumanReadableLogsMatchRaw(t *testing.T) {
	// Temporarily enable debug mode for this test
	originalDebugMode := debugMode
	debugMode = true
	defer func() { debugMode = originalDebugMode }()

	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create topic
	topicName := "human-readable-test"
	createReq := &producerpb.CreateTopicRequest{
		Topic:         topicName,
		NumPartitions: 1,
	}

	_, err := broker.CreateTopic(context.Background(), createReq)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Find partition assigned to this broker
	var assignedPartition cluster.PartitionKey
	found := false
	for partKey, port := range broker.ClusterMetadata.TopicsMetadata.Topics[topicName].Partitions {
		if port == broker.port {
			assignedPartition = partKey
			found = true
			break
		}
	}

	if !found {
		t.Skip("No partition assigned to this broker")
	}

	// Publish messages
	messages := []string{"First message", "Second message", "Third message"}

	for _, msgContent := range messages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msgContent,
		}

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Flush logs
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	err = partition.flushLogs(broker.port, topicName, assignedPartition)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	// Read raw log file
	rawLogPath := "broker/broker_logs/logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition)) + "/segment_0.log"
	rawLogData, err := os.ReadFile(rawLogPath)
	if err != nil {
		t.Fatalf("Failed to read raw log file: %v", err)
	}

	// Read human-readable log file
	humanReadablePath := "broker/broker_logs/readable_logs/broker_" + strconv.Itoa(int(broker.port)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(assignedPartition)) + "/segment_0.log"
	humanReadableData, err := os.ReadFile(humanReadablePath)
	if err != nil {
		t.Fatalf("Failed to read human-readable log file: %v", err)
	}

	// Verify raw log contains all messages with headers (20 bytes per header)
	// Each message: 20 bytes header + message data
	expectedRawSize := int64(len(messages) * MessageHeaderSize)
	for _, msg := range messages {
		expectedRawSize += int64(len(msg))
	}
	if int64(len(rawLogData)) != expectedRawSize {
		t.Errorf("Raw log size mismatch. Expected %d bytes, got %d bytes", expectedRawSize, len(rawLogData))
	}

	// Read and verify messages from raw log using headers
	rawLogFile, err := os.OpenFile(rawLogPath, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open raw log file for reading: %v", err)
	}
	defer rawLogFile.Close()

	sf := &SegmentFile{raw: rawLogFile}
	expectedOffset := int64(0)
	for i, expectedMsg := range messages {
		msg, err := sf.Read()
		if err != nil {
			t.Fatalf("Failed to read message %d from raw log: %v", i, err)
		}
		if string(msg.Data) != expectedMsg {
			t.Errorf("Message %d content mismatch. Expected '%s', got '%s'", i, expectedMsg, string(msg.Data))
		}
		if msg.Header.Offset != expectedOffset {
			t.Errorf("Message %d offset mismatch. Expected %d, got %d", i, expectedOffset, msg.Header.Offset)
		}
		expectedOffset += int64(MessageHeaderSize) + int64(len(msg.Data))
	}

	// Verify human-readable log contains offset annotations with correct offsets
	humanReadableStr := string(humanReadableData)
	expectedOffset = int64(0)
	for _, msgContent := range messages {
		expectedLine := "Offset " + strconv.FormatInt(expectedOffset, 10) + ": " + msgContent
		if !contains(humanReadableStr, expectedLine) {
			t.Errorf("Human-readable log missing expected line: %s", expectedLine)
		}
		expectedOffset += int64(MessageHeaderSize) + int64(len(msgContent))
	}

	t.Logf("Raw log: %d bytes (with headers)", len(rawLogData))
	t.Logf("Human-readable log:\n%s", string(humanReadableData))

	// Clean up
	os.RemoveAll("broker/broker_logs")
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr || len(s) > len(substr) &&
			(s[:len(substr)] == substr || contains(s[1:], substr)))
}

// Helper function to setup broker with etcd for testing
func setupBrokerWithEtcd(t *testing.T) *brokerServer {
	broker := NewBrokerServer()

	// Connect to etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("Skipping test: etcd not available: %v", err)
	}

	broker.etcdClient = etcdClient
	broker.port = cluster.Port(8080)
	broker.ClusterMetadata = cluster.ClusterMetadata{
		TopicsMetadata:  &cluster.TopicsMetadata{Topics: make(map[string]*cluster.TopicMetadata)},
		BrokersMetadata: &cluster.BrokersMetadata{Brokers: map[cluster.Port]*cluster.BrokerMetadata{cluster.Port(8080): {Port: cluster.Port(8080)}}},
	}

	return broker
}

// Helper function to cleanup etcd after tests
func cleanupEtcd(t *testing.T, broker *brokerServer) {
	if broker.etcdClient != nil {
		// Clean up test data
		for topicName := range broker.Topics {
			_, err := broker.etcdClient.Delete(context.Background(), "/topic/"+topicName)
			if err != nil {
				t.Logf("Warning: failed to cleanup topic %s: %v", topicName, err)
			}
		}
		broker.etcdClient.Close()
	}
}

// Helper function to clean up controller key before tests
func cleanupControllerKey(t *testing.T) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Logf("Warning: Could not connect to etcd for cleanup: %v", err)
		return
	}
	defer etcdClient.Close()

	_, err = etcdClient.Delete(context.Background(), "controller")
	if err != nil {
		t.Logf("Warning: Failed to delete controller key: %v", err)
	} else {
		t.Log("Cleaned up controller key from etcd")
	}
}
