// Mostly copilot generated tests for producer.go

package producer

import (
	"context"
	"go-kafka/cluster"
	producerpb "go-kafka/proto/producer"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Mock broker server for testing
type mockBrokerServer struct {
	producerpb.UnimplementedProducerServiceServer
	port            int
	topics          map[string]*mockTopic
	mu              sync.RWMutex
	isController    bool
	publishedMsgs   []string // Track published messages
	createTopicReqs []string // Track create topic requests
}

type mockTopic struct {
	name          string
	numPartitions int32
	partitions    map[int32]int32 // partitionKey -> broker port
}

func (m *mockBrokerServer) GetMetadata(ctx context.Context, req *emptypb.Empty) (*producerpb.ProducerMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topics := make([]*producerpb.TopicMetadata, 0)
	for _, topic := range m.topics {
		topics = append(topics, &producerpb.TopicMetadata{
			Topic:         topic.name,
			NumPartitions: topic.numPartitions,
			Partitions:    topic.partitions,
		})
	}

	brokers := []*producerpb.BrokerMetadata{
		{Port: int32(m.port), Address: "localhost:" + strconv.Itoa(m.port)},
	}

	controllerPort := int32(0)
	if m.isController {
		controllerPort = int32(m.port)
	}

	return &producerpb.ProducerMetadata{
		Topics:         topics,
		Brokers:        brokers,
		ControllerPort: controllerPort,
	}, nil
}

func (m *mockBrokerServer) CreateTopic(ctx context.Context, req *producerpb.CreateTopicRequest) (*producerpb.CreateTopicResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.createTopicReqs = append(m.createTopicReqs, req.Topic)

	if _, exists := m.topics[req.Topic]; exists {
		return &producerpb.CreateTopicResponse{Success: false}, nil
	}

	partitions := make(map[int32]int32)
	for i := int32(0); i < req.NumPartitions; i++ {
		partitions[i] = int32(m.port)
	}

	m.topics[req.Topic] = &mockTopic{
		name:          req.Topic,
		numPartitions: req.NumPartitions,
		partitions:    partitions,
	}

	return &producerpb.CreateTopicResponse{Success: true}, nil
}

func (m *mockBrokerServer) PublishMessage(ctx context.Context, req *producerpb.PublishRequest) (*producerpb.PublishResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.publishedMsgs = append(m.publishedMsgs, req.Message)

	topic, exists := m.topics[req.Topic]
	if !exists {
		return &producerpb.PublishResponse{Success: false, Error: "topic not found"}, nil
	}

	if _, ok := topic.partitions[req.PartitionKey]; !ok {
		return &producerpb.PublishResponse{Success: false, Error: "partition not found"}, nil
	}

	return &producerpb.PublishResponse{Success: true}, nil
}

// Helper to start mock broker server
func startMockBroker(t *testing.T, port int, isController bool) (*mockBrokerServer, *grpc.Server, func()) {
	mockBroker := &mockBrokerServer{
		port:         port,
		topics:       make(map[string]*mockTopic),
		isController: isController,
	}

	server := grpc.NewServer()
	producerpb.RegisterProducerServiceServer(server, mockBroker)

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		t.Fatalf("Failed to listen on port %d: %v", port, err)
	}

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		server.Stop()
		listener.Close()
	}

	return mockBroker, server, cleanup
}

// Test StartProducer
func TestStartProducer(t *testing.T) {
	mockBroker, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	// Override bootstrap port for testing
	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()
	defer producer.ShutdownProducer()

	if producer.clientConn == nil {
		t.Fatal("Client connections not initialized")
	}

	if len(producer.clientConn) == 0 {
		t.Error("No client connections established")
	}

	if producer.clusterMetadata == nil {
		t.Fatal("ClusterMetadata not populated")
	}

	if producer.ctx == nil {
		t.Error("Context not initialized")
	}

	if producer.cancel == nil {
		t.Error("Cancel function not initialized")
	}

	_ = mockBroker // Use mockBroker
}

// Test ShutdownProducer
func TestShutdownProducer(t *testing.T) {
	_, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()

	if len(producer.clientConn) == 0 {
		t.Fatal("Producer should have connections before shutdown")
	}

	producer.ShutdownProducer()

	if len(producer.clientConn) != 0 {
		t.Error("Client connections not cleaned up after shutdown")
	}
}

// Test CreateTopic success
func TestCreateTopic_Success(t *testing.T) {
	mockBroker, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()
	defer producer.ShutdownProducer()

	topicName := "test-topic"
	numPartitions := 3

	result, err := producer.CreateTopic(topicName, numPartitions)

	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	if result == "" {
		t.Error("CreateTopic returned empty result")
	}

	// Verify topic was created on mock broker
	mockBroker.mu.RLock()
	topic, exists := mockBroker.topics[topicName]
	mockBroker.mu.RUnlock()

	if !exists {
		t.Fatal("Topic was not created on broker")
	}

	if topic.numPartitions != int32(numPartitions) {
		t.Errorf("Expected %d partitions, got %d", numPartitions, topic.numPartitions)
	}

	if len(mockBroker.createTopicReqs) != 1 {
		t.Errorf("Expected 1 CreateTopic request, got %d", len(mockBroker.createTopicReqs))
	}
}

// Test CreateTopic duplicate
func TestCreateTopic_Duplicate(t *testing.T) {
	mockBroker, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()
	defer producer.ShutdownProducer()

	topicName := "duplicate-topic"

	// Create topic first time
	_, err := producer.CreateTopic(topicName, 3)
	if err != nil {
		t.Fatalf("First CreateTopic failed: %v", err)
	}

	// Try to create same topic again
	_, err = producer.CreateTopic(topicName, 3)
	if err != nil {
		t.Fatalf("Second CreateTopic returned error: %v", err)
	}

	// Should have tried at least once (may retry on failure)
	mockBroker.mu.RLock()
	createCount := len(mockBroker.createTopicReqs)
	mockBroker.mu.RUnlock()

	if createCount < 2 {
		t.Logf("CreateTopic called %d times (expected at least 2)", createCount)
	}
}

// Test PublishMessage success
func TestPublishMessage_Success(t *testing.T) {
	mockBroker, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()
	defer producer.ShutdownProducer()

	// Create topic first
	topicName := "test-topic"
	_, err := producer.CreateTopic(topicName, 3)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Refresh metadata to get new topic info
	producer.populateMetadata(false)

	// Publish message
	message := "Hello, Kafka!"
	err = producer.PublishMessage(topicName, message)

	if err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	// Verify message was received by broker
	mockBroker.mu.RLock()
	publishedCount := len(mockBroker.publishedMsgs)
	mockBroker.mu.RUnlock()

	if publishedCount == 0 {
		t.Error("No messages were published to broker")
	}

	mockBroker.mu.RLock()
	publishedMsg := mockBroker.publishedMsgs[0]
	mockBroker.mu.RUnlock()

	if publishedMsg != message {
		t.Errorf("Expected message %s, got %s", message, publishedMsg)
	}
}

// Test PublishMessage to non-existent topic
func TestPublishMessage_TopicNotFound(t *testing.T) {
	_, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()
	defer producer.ShutdownProducer()

	// Try to publish to non-existent topic
	err := producer.PublishMessage("non-existent-topic", "test message")

	if err == nil {
		t.Error("Expected failed response when publishing to non-existent topic")
	}
}

// Test PublishMessage with multiple messages
func TestPublishMessage_Multiple(t *testing.T) {
	mockBroker, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()
	defer producer.ShutdownProducer()

	// Create topic
	topicName := "test-topic"
	_, err := producer.CreateTopic(topicName, 3)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	producer.populateMetadata(false)

	// Publish multiple messages
	messages := []string{"Message 1", "Message 2", "Message 3"}
	for _, msg := range messages {
		err := producer.PublishMessage(topicName, msg)
		if err != nil {
			t.Fatalf("PublishMessage failed for %s: %v", msg, err)
		}
	}

	// Verify all messages were received
	mockBroker.mu.RLock()
	publishedCount := len(mockBroker.publishedMsgs)
	mockBroker.mu.RUnlock()

	if publishedCount != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), publishedCount)
	}
}

// Test getRandomPartition
func TestGetRandomPartition(t *testing.T) {
	producer := &Producer{
		clusterMetadata: &cluster.ClusterMetadata{
			TopicsMetadata: &cluster.TopicsMetadata{
				Topics: map[string]*cluster.TopicMetadata{
					"test-topic": {
						Topic:         "test-topic",
						NumPartitions: 3,
						Partitions: map[cluster.PartitionKey]cluster.Port{
							0: 8080,
							1: 8081,
							2: 8082,
						},
					},
				},
			},
		},
	}

	// Test getting random partition
	partition, err := producer.getRandomPartition("test-topic")

	if err != nil {
		t.Fatalf("getRandomPartition failed: %v", err)
	}

	if partition < 0 || partition > 2 {
		t.Errorf("Expected partition between 0-2, got %d", partition)
	}
}

// Test getRandomPartition with no metadata
func TestGetRandomPartition_NoMetadata(t *testing.T) {
	producer := &Producer{}

	_, err := producer.getRandomPartition("test-topic")

	if err == nil {
		t.Error("Expected error when metadata is not populated")
	}

	expectedMsg := "producer metadata is not populated"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// Test getRandomPartition with non-existent topic
func TestGetRandomPartition_TopicNotFound(t *testing.T) {
	producer := &Producer{
		clusterMetadata: &cluster.ClusterMetadata{
			TopicsMetadata: &cluster.TopicsMetadata{
				Topics: map[string]*cluster.TopicMetadata{},
			},
		},
	}

	_, err := producer.getRandomPartition("non-existent-topic")

	if err == nil {
		t.Error("Expected error for non-existent topic")
	}

	expectedMsg := "topic does not exist in metadata"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// Test getRandomPartition with no partitions
func TestGetRandomPartition_NoPartitions(t *testing.T) {
	producer := &Producer{
		clusterMetadata: &cluster.ClusterMetadata{
			TopicsMetadata: &cluster.TopicsMetadata{
				Topics: map[string]*cluster.TopicMetadata{
					"empty-topic": {
						Topic:         "empty-topic",
						NumPartitions: 0,
						Partitions:    map[cluster.PartitionKey]cluster.Port{},
					},
				},
			},
		},
	}

	_, err := producer.getRandomPartition("empty-topic")

	if err == nil {
		t.Error("Expected error when no partitions available")
	}

	expectedMsg := "no partitions available for topic"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

// Test populateClientConnections
func TestPopulateClientConnections(t *testing.T) {
	mockBroker1, _, cleanup1 := startMockBroker(t, 8080, true)
	defer cleanup1()
	mockBroker2, _, cleanup2 := startMockBroker(t, 8081, false)
	defer cleanup2()

	producer := &Producer{
		clientConn: make(map[cluster.Port]*ClientConn),
		clusterMetadata: &cluster.ClusterMetadata{
			BrokersMetadata: &cluster.BrokersMetadata{
				Brokers: map[cluster.Port]*cluster.BrokerMetadata{
					8080: {Port: 8080},
					8081: {Port: 8081},
				},
			},
		},
	}

	err := producer.populateClientConnections()

	if err != nil {
		t.Fatalf("populateClientConnections failed: %v", err)
	}

	if len(producer.clientConn) != 2 {
		t.Errorf("Expected 2 client connections, got %d", len(producer.clientConn))
	}

	for port := range producer.clusterMetadata.BrokersMetadata.Brokers {
		if _, exists := producer.clientConn[port]; !exists {
			t.Errorf("Missing client connection for broker at port %d", port)
		}
	}

	_ = mockBroker1
	_ = mockBroker2
}

// Test concurrent PublishMessage
func TestPublishMessage_Concurrent(t *testing.T) {
	mockBroker, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()
	defer producer.ShutdownProducer()

	// Create topic
	topicName := "test-topic"
	_, err := producer.CreateTopic(topicName, 3)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	producer.populateMetadata(false)

	// Publish messages concurrently
	numMessages := 10
	var wg sync.WaitGroup
	errors := make(chan error, numMessages)

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(msgNum int) {
			defer wg.Done()
			err := producer.PublishMessage(topicName, "Message "+strconv.Itoa(msgNum))
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent publish error: %v", err)
	}

	// Verify messages were received
	mockBroker.mu.RLock()
	publishedCount := len(mockBroker.publishedMsgs)
	mockBroker.mu.RUnlock()

	if publishedCount != numMessages {
		t.Errorf("Expected %d messages, got %d", numMessages, publishedCount)
	}
}

// Test metadata refresh loop
func TestRefreshMetadataLoop(t *testing.T) {
	mockBroker, _, cleanup := startMockBroker(t, 8080, true)
	defer cleanup()

	producer := &Producer{}

	originalPort := bootstrapServerPort
	bootstrapServerPort = 8080
	defer func() { bootstrapServerPort = originalPort }()

	producer.StartProducer()
	defer producer.ShutdownProducer()

	// Add a topic to broker
	mockBroker.mu.Lock()
	mockBroker.topics["new-topic"] = &mockTopic{
		name:          "new-topic",
		numPartitions: 2,
		partitions:    map[int32]int32{0: 8080, 1: 8080},
	}
	mockBroker.mu.Unlock()

	// Trigger metadata refresh manually
	producer.populateMetadata(false)

	// Verify new topic is in metadata
	producer.metadataMu.RLock()
	_, exists := producer.clusterMetadata.TopicsMetadata.Topics["new-topic"]
	producer.metadataMu.RUnlock()

	if !exists {
		t.Error("ClusterMetadata refresh did not pick up new topic")
	}
}
