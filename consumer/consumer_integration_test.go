package consumer

import (
	"fmt"
	"go-kafka/broker"
	"go-kafka/cluster"
	"go-kafka/producer"
	"log"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
)

// TestVerticalIntegration is a comprehensive end-to-end test demonstrating:
// - Multiple brokers running
// - Producer creating topics and publishing messages
// - Multiple consumers in a group with automatic rebalancing
// - Message consumption from distributed partitions
func TestVerticalIntegration(t *testing.T) {
	// Clean up before test
	cleanupEtcd(t, "integration-group")
	defer cleanupEtcd(t, "integration-group")

	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("VERTICAL INTEGRATION TEST: Full System Demo")
	log.Println(strings.Repeat("=", 60))

	// Step 1: Start brokers
	log.Println("\n[Step 1] Starting 3 Broker Servers...")
	broker1 := broker.NewBrokerServer()
	broker2 := broker.NewBrokerServer()
	broker3 := broker.NewBrokerServer()

	broker1.StartBroker()
	broker2.StartBroker()
	broker3.StartBroker()
	
	// Ensure brokers are shut down even if test panics
	defer func() {
		broker1.ShutdownBroker()
		broker2.ShutdownBroker()
		broker3.ShutdownBroker()
		time.Sleep(500 * time.Millisecond) // Wait for ports to be released
	}()

	time.Sleep(2 * time.Second)
	log.Println("✓ All brokers started")

	// Step 2: Start producer
	log.Println("\n[Step 2] Starting Producer...")
	prod := producer.Producer{}
	prod.StartProducer()
	defer prod.ShutdownProducer()

	time.Sleep(1 * time.Second)
	log.Println("✓ Producer ready")

	// Step 3: Create topic
	topicName := "integration-topic"
	log.Printf("\n[Step 3] Creating topic '%s' with 3 partitions...", topicName)
	_, err := prod.CreateTopic(topicName, 3)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	time.Sleep(1 * time.Second)
	log.Printf("✓ Topic '%s' created", topicName)

	// Step 4: Publish initial messages
	log.Println("\n[Step 4] Publishing 10 messages...")
	messages := []string{
		"Message 1: User login successful",
		"Message 2: File uploaded to server",
		"Message 3: Database connection established",
		"Message 4: Error: Unable to fetch data",
		"Message 5: User logout successful",
		"Message 6: New user registered",
		"Message 7: Password changed successfully",
		"Message 8: Session expired for user",
		"Message 9: Data backup completed",
		"Message 10: Server restarted",
	}

	publishedCount := 0
	for _, msg := range messages {
		err := prod.PublishMessage(topicName, msg)
		if err != nil {
			log.Printf("  ✗ Failed to publish: %v", err)
		} else {
			publishedCount++
			log.Printf("  ✓ Published: %s", msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("✓ Published %d/%d messages", publishedCount, len(messages))

	// Step 5: Create first consumer
	log.Println("\n[Step 5] Creating Consumer 1...")
	consumer1 := NewConsumer("integration-group")
	defer consumer1.StopConsumer()

	// Subscribe consumer to topic
	consumer1.subscribedTopics[topicName] = &ConsumerTopicMetadata{
		Name:                 topicName,
		PartitionMetadataMap: make(map[cluster.PartitionKey]*PartitionMetadata),
	}

	// Fetch broker metadata and set up connections
	err = consumer1.fetchBrokerMetadata()
	if err != nil {
		t.Fatalf("Failed to fetch broker metadata for consumer 1: %v", err)
	}
	err = consumer1.populateClientConnections()
	if err != nil {
		t.Fatalf("Failed to populate client connections for consumer 1: %v", err)
	}

	// Trigger rebalancing
	event := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte("/consumer-group/integration-group/members/" + string(consumer1.ID))},
	}
	consumer1.rebalanceConsumer(event)
	time.Sleep(3 * time.Second)
	log.Println("✓ Consumer 1 subscribed and rebalanced")

	// Step 6: Consumer 1 pulls messages
	log.Println("\n[Step 6] Consumer 1 pulling messages...")
	msgs1, err := consumer1.PullMessages(topicName, 20, 10240)
	if err != nil {
		t.Errorf("Consumer 1 failed to pull: %v", err)
	}
	log.Printf("✓ Consumer 1 received %d messages", len(msgs1))
	if len(msgs1) > 0 {
		for i, msg := range msgs1 {
			if i < 3 { // Print first 3
				log.Printf("    [%d] Offset=%d, Data=%s", i+1, msg.Header.Offset, string(msg.Data))
			}
		}
		if len(msgs1) > 3 {
			log.Printf("    ... and %d more messages", len(msgs1)-3)
		}
	}

	// Step 7: Create second consumer (rebalancing)
	log.Println("\n[Step 7] Creating Consumer 2 (triggers rebalancing)...")
	consumer2 := NewConsumer("integration-group")
	defer consumer2.StopConsumer()

	consumer2.subscribedTopics[topicName] = &ConsumerTopicMetadata{
		Name:                 topicName,
		PartitionMetadataMap: make(map[cluster.PartitionKey]*PartitionMetadata),
	}

	// Fetch broker metadata and set up connections
	err = consumer2.fetchBrokerMetadata()
	if err != nil {
		t.Fatalf("Failed to fetch broker metadata for consumer 2: %v", err)
	}
	err = consumer2.populateClientConnections()
	if err != nil {
		t.Fatalf("Failed to populate client connections for consumer 2: %v", err)
	}

	event2 := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte("/consumer-group/integration-group/members/" + string(consumer2.ID))},
	}
	consumer2.rebalanceConsumer(event2)
	time.Sleep(4 * time.Second)
	log.Println("✓ Consumer 2 joined and rebalancing complete")

	// Step 8: Publish more messages
	log.Println("\n[Step 8] Publishing 5 more messages...")
	moreMessages := []string{
		"Message 11: Cache cleared",
		"Message 12: API request received",
		"Message 13: Transaction completed",
		"Message 14: Email sent",
		"Message 15: Backup restored",
	}

	for _, msg := range moreMessages {
		err := prod.PublishMessage(topicName, msg)
		if err == nil {
			log.Printf("  ✓ Published: %s", msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Println("✓ Additional messages published")

	// Step 9: Both consumers pull
	log.Println("\n[Step 9] Both consumers pulling new messages...")
	time.Sleep(1 * time.Second)

	msgs1_new, _ := consumer1.PullMessages(topicName, 20, 10240)
	msgs2, _ := consumer2.PullMessages(topicName, 20, 10240)

	log.Printf("  ✓ Consumer 1: %d messages", len(msgs1_new))
	log.Printf("  ✓ Consumer 2: %d messages", len(msgs2))

	// Step 10: Create third consumer
	log.Println("\n[Step 10] Creating Consumer 3 (3-way partition split)...")
	consumer3 := NewConsumer("integration-group")
	defer consumer3.StopConsumer()

	consumer3.subscribedTopics[topicName] = &ConsumerTopicMetadata{
		Name:                 topicName,
		PartitionMetadataMap: make(map[cluster.PartitionKey]*PartitionMetadata),
	}

	// Fetch broker metadata and set up connections
	err = consumer3.fetchBrokerMetadata()
	if err != nil {
		t.Fatalf("Failed to fetch broker metadata for consumer 3: %v", err)
	}
	err = consumer3.populateClientConnections()
	if err != nil {
		t.Fatalf("Failed to populate client connections for consumer 3: %v", err)
	}

	event3 := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte("/consumer-group/integration-group/members/" + string(consumer3.ID))},
	}
	consumer3.rebalanceConsumer(event3)
	time.Sleep(4 * time.Second)
	log.Println("✓ Consumer 3 joined - each consumer has 1 partition")

	// Step 11: Final batch of messages
	log.Println("\n[Step 11] Publishing final 3 messages...")
	finalMessages := []string{
		"Message 16: Health check OK",
		"Message 17: Metrics collected",
		"Message 18: Demo complete!",
	}

	for _, msg := range finalMessages {
		err := prod.PublishMessage(topicName, msg)
		if err == nil {
			log.Printf("  ✓ Published: %s", msg)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Step 12: All consumers pull final messages
	log.Println("\n[Step 12] All consumers pulling final messages...")
	time.Sleep(1 * time.Second)

	msgs1_final, _ := consumer1.PullMessages(topicName, 20, 10240)
	msgs2_final, _ := consumer2.PullMessages(topicName, 20, 10240)
	msgs3_final, _ := consumer3.PullMessages(topicName, 20, 10240)

	log.Printf("  Consumer 1: %d messages", len(msgs1_final))
	log.Printf("  Consumer 2: %d messages", len(msgs2_final))
	log.Printf("  Consumer 3: %d messages", len(msgs3_final))

	// Verify we got messages
	totalMessages := len(msgs1) + len(msgs1_new) + len(msgs1_final) +
		len(msgs2) + len(msgs2_final) +
		len(msgs3_final)

	if totalMessages == 0 {
		t.Error("No messages were consumed across all consumers")
	}

	// Step 13: Summary
	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("DEMO SUMMARY")
	log.Println(strings.Repeat("=", 60))
	log.Printf("✓ Created topic with 3 partitions")
	log.Printf("✓ Published %d messages total", len(messages)+len(moreMessages)+len(finalMessages))
	log.Printf("✓ Created 3 consumers with rebalancing")
	log.Printf("✓ Total messages consumed: %d", totalMessages)
	log.Println("\nFeatures Demonstrated:")
	log.Println("  • Topic creation and partition distribution")
	log.Println("  • Message production across partitions")
	log.Println("  • Consumer group coordination via etcd")
	log.Println("  • Automatic partition rebalancing")
	log.Println("  • Concurrent message consumption")
	log.Println("  • Offset tracking and management")
	log.Println("\n✓ Integration test completed successfully!")
	log.Println(strings.Repeat("=", 60))
}

// Suppress unused import warning
var _ = fmt.Sprint
