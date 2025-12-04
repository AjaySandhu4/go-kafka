package broker

import (
	"context"
	"encoding/json"
	producerpb "go-kafka/proto/producer"
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
	var assignedPartition PartitionKey
	found := false
	for partKey, port := range broker.Metadata.TopicInfo[topicName].Partitions {
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

	if string(partition.Messages[0]) != publishReq.Message {
		t.Errorf("Expected message %s, got %s", publishReq.Message, string(partition.Messages[0]))
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
	var wrongPartition PartitionKey
	found := false
	for partKey, port := range broker.Metadata.TopicInfo[topicName].Partitions {
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
	var assignedPartition PartitionKey
	found := false
	for partKey, port := range broker.Metadata.TopicInfo[topicName].Partitions {
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
		if string(partition.Messages[i]) != msg {
			t.Errorf("Message %d: expected %s, got %s", i, msg, string(partition.Messages[i]))
		}
	}

	// Verify index entries
	if len(partition.Index) != len(messages) {
		t.Errorf("Expected %d index entries, got %d", len(messages), len(partition.Index))
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
	var assignedPartition PartitionKey
	found := false
	for partKey, port := range broker.Metadata.TopicInfo[topicName].Partitions {
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
	expectedOffsets := []int{}
	currentOffset := 0

	for _, msg := range messages {
		publishReq := &producerpb.PublishRequest{
			Topic:        topicName,
			PartitionKey: int32(assignedPartition),
			Message:      msg,
		}

		expectedOffsets = append(expectedOffsets, currentOffset)
		currentOffset += len(msg)

		_, err := broker.PublishMessage(context.Background(), publishReq)
		if err != nil {
			t.Fatalf("PublishMessage failed: %v", err)
		}
	}

	// Verify index entries
	partition := broker.Topics[topicName].Partitions[assignedPartition]
	if len(partition.Index) != len(messages) {
		t.Fatalf("Expected %d index entries, got %d", len(messages), len(partition.Index))
	}

	for i, entry := range partition.Index {
		if entry.Offset != expectedOffsets[i] {
			t.Errorf("Index entry %d: expected offset %d, got %d", i, expectedOffsets[i], entry.Offset)
		}
		if entry.Size != len(messages[i]) {
			t.Errorf("Index entry %d: expected size %d, got %d", i, len(messages[i]), entry.Size)
		}
	}

	// Verify NextOffset
	if partition.NextOffset != currentOffset {
		t.Errorf("Expected NextOffset %d, got %d", currentOffset, partition.NextOffset)
	}
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
	var assignedPartition PartitionKey
	found := false
	for partKey, port := range broker.Metadata.TopicInfo[topicName].Partitions {
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
	broker.port = Port(8080)
	broker.Metadata = Metadata{
		TopicInfo:  make(map[string]TopicMetadata),
		BrokerInfo: map[Port]struct{}{Port(8080): {}},
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

// Test FetchMetadata
func TestFetchMetadata(t *testing.T) {
	broker := setupBrokerWithEtcd(t)
	defer cleanupEtcd(t, broker)

	// Create a test topic in etcd
	topicName := "metadata-test-topic"
	topicMeta := TopicMetadata{
		Topic:         topicName,
		NumPartitions: 3,
		Partitions: map[PartitionKey]Port{
			0: 8080,
			1: 8081,
			2: 8082,
		},
	}

	metaBytes, err := json.Marshal(topicMeta)
	if err != nil {
		t.Fatalf("Failed to marshal topic metadata: %v", err)
	}

	_, err = broker.etcdClient.Put(context.Background(), "/topic/"+topicName, string(metaBytes))
	if err != nil {
		t.Fatalf("Failed to put topic in etcd: %v", err)
	}

	// Fetch metadata
	broker.FetchMetadata()

	// Verify metadata was fetched
	fetchedMeta, exists := broker.Metadata.TopicInfo[topicName]
	if !exists {
		t.Fatal("Topic metadata was not fetched")
	}

	if fetchedMeta.Topic != topicName {
		t.Errorf("Expected topic name %s, got %s", topicName, fetchedMeta.Topic)
	}

	if fetchedMeta.NumPartitions != 3 {
		t.Errorf("Expected 3 partitions, got %d", fetchedMeta.NumPartitions)
	}

	if len(fetchedMeta.Partitions) != 3 {
		t.Errorf("Expected 3 partition assignments, got %d", len(fetchedMeta.Partitions))
	}
}
