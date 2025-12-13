package consumer

import (
	"context"
	"log"
	"testing"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Helper function to clean up etcd before tests
func cleanupEtcd(t *testing.T, groupID string) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Logf("Warning: Could not connect to etcd for cleanup: %v", err)
		return
	}
	defer etcdClient.Close()

	// Delete consumer group data
	_, err = etcdClient.Delete(context.Background(), "/consumer-group/"+groupID, clientv3.WithPrefix())
	if err != nil {
		t.Logf("Warning: Could not cleanup consumer group: %v", err)
	}
}

// Test creating a single consumer
func TestNewConsumer_Single(t *testing.T) {
	groupID := "test-group-1"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	consumer := NewConsumer(groupID)
	if consumer == nil {
		t.Fatal("NewConsumer returned nil")
	}

	// Verify consumer has an ID
	if consumer.ID == "" {
		t.Error("Consumer ID is empty")
	}

	// Verify consumer has correct group ID
	if consumer.GroupID != ConsumerGroupID(groupID) {
		t.Errorf("Expected GroupID %s, got %s", groupID, consumer.GroupID)
	}

	// Verify etcd client is initialized
	if consumer.etcdClient == nil {
		t.Error("Consumer etcd client is nil")
	}

	// Verify consumer is registered in etcd
	resp, err := consumer.etcdClient.Get(context.Background(), "/consumer-group/"+string(groupID)+"/members/"+string(consumer.ID))
	if err != nil {
		t.Fatalf("Failed to get consumer from etcd: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Error("Consumer not registered in etcd")
	}

	t.Logf("Consumer created successfully: ID=%s, GroupID=%s", consumer.ID, consumer.GroupID)

	// Cleanup
	consumer.StopConsumer()
}

// Test multiple consumers joining the same group
func TestNewConsumer_SameGroup(t *testing.T) {
	groupID := "test-group-same"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	// Create first consumer
	consumer1 := NewConsumer(groupID)
	if consumer1 == nil {
		t.Fatal("Failed to create consumer 1")
	}
	defer consumer1.StopConsumer()

	// Create second consumer in same group
	consumer2 := NewConsumer(groupID)
	if consumer2 == nil {
		t.Fatal("Failed to create consumer 2")
	}
	defer consumer2.StopConsumer()

	// Create third consumer in same group
	consumer3 := NewConsumer(groupID)
	if consumer3 == nil {
		t.Fatal("Failed to create consumer 3")
	}
	defer consumer3.StopConsumer()

	// Verify all consumers have different IDs
	if consumer1.ID == consumer2.ID || consumer1.ID == consumer3.ID || consumer2.ID == consumer3.ID {
		t.Error("Consumer IDs are not unique")
	}

	// Verify all consumers belong to same group
	if consumer1.GroupID != ConsumerGroupID(groupID) || consumer2.GroupID != ConsumerGroupID(groupID) || consumer3.GroupID != ConsumerGroupID(groupID) {
		t.Error("Not all consumers belong to the same group")
	}

	// Wait for registration to propagate
	time.Sleep(100 * time.Millisecond)

	// Verify all consumers are registered in etcd
	resp, err := consumer1.etcdClient.Get(context.Background(), "/consumer-group/"+string(consumer1.GroupID)+"/members/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to get group members from etcd: %v", err)
	}

	if len(resp.Kvs) != 3 {
		t.Errorf("Expected 3 members in group, got %d", len(resp.Kvs))
	}

	t.Logf("Successfully created 3 consumers in group %s", groupID)
	for _, kv := range resp.Kvs {
		t.Logf("  Member: %s", string(kv.Key))
	}
}

// Test consumers joining different groups
func TestNewConsumer_DifferentGroups(t *testing.T) {
	group1 := "test-group-diff-1"
	group2 := "test-group-diff-2"
	group3 := "test-group-diff-3"

	cleanupEtcd(t, group1)
	cleanupEtcd(t, group2)
	cleanupEtcd(t, group3)
	defer cleanupEtcd(t, group1)
	defer cleanupEtcd(t, group2)
	defer cleanupEtcd(t, group3)

	// Create consumers in different groups
	consumer1 := NewConsumer(group1)
	if consumer1 == nil {
		t.Fatal("Failed to create consumer 1")
	}
	defer consumer1.StopConsumer()

	consumer2 := NewConsumer(group2)
	if consumer2 == nil {
		t.Fatal("Failed to create consumer 2")
	}
	defer consumer2.StopConsumer()

	consumer3 := NewConsumer(group3)
	if consumer3 == nil {
		t.Fatal("Failed to create consumer 3")
	}
	defer consumer3.StopConsumer()

	// Verify consumers are in different groups
	if consumer1.GroupID == consumer2.GroupID || consumer1.GroupID == consumer3.GroupID {
		t.Error("Consumers should be in different groups")
	}

	// Wait for registration to propagate
	time.Sleep(100 * time.Millisecond)

	// Verify group 1 has only 1 member
	resp1, err := consumer1.etcdClient.Get(context.Background(), "/consumer-group/"+group1+"/members/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to get group 1 members: %v", err)
	}
	if len(resp1.Kvs) != 1 {
		t.Errorf("Group 1 should have 1 member, got %d", len(resp1.Kvs))
	}

	// Verify group 2 has only 1 member
	resp2, err := consumer2.etcdClient.Get(context.Background(), "/consumer-group/"+group2+"/members/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to get group 2 members: %v", err)
	}
	if len(resp2.Kvs) != 1 {
		t.Errorf("Group 2 should have 1 member, got %d", len(resp2.Kvs))
	}

	// Verify group 3 has only 1 member
	resp3, err := consumer3.etcdClient.Get(context.Background(), "/consumer-group/"+group3+"/members/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to get group 3 members: %v", err)
	}
	if len(resp3.Kvs) != 1 {
		t.Errorf("Group 3 should have 1 member, got %d", len(resp3.Kvs))
	}

	t.Logf("Successfully created consumers in 3 different groups:")
	t.Logf("  Group %s: %s", group1, consumer1.ID)
	t.Logf("  Group %s: %s", group2, consumer2.ID)
	t.Logf("  Group %s: %s", group3, consumer3.ID)
}

// Test mixed scenario: multiple groups with multiple consumers each
func TestNewConsumer_MixedScenario(t *testing.T) {
	groupA := "test-group-mixed-A"
	groupB := "test-group-mixed-B"

	cleanupEtcd(t, groupA)
	cleanupEtcd(t, groupB)
	defer cleanupEtcd(t, groupA)
	defer cleanupEtcd(t, groupB)

	// Create 2 consumers in group A
	consumerA1 := NewConsumer(groupA)
	defer consumerA1.StopConsumer()

	consumerA2 := NewConsumer(groupA)
	defer consumerA2.StopConsumer()

	// Create 3 consumers in group B
	consumerB1 := NewConsumer(groupB)
	defer consumerB1.StopConsumer()

	consumerB2 := NewConsumer(groupB)
	defer consumerB2.StopConsumer()

	consumerB3 := NewConsumer(groupB)
	defer consumerB3.StopConsumer()

	// Wait for registration
	time.Sleep(100 * time.Millisecond)

	// Verify group A has 2 members
	respA, err := consumerA1.etcdClient.Get(context.Background(), "/consumer-group/"+groupA+"/members/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to get group A members: %v", err)
	}
	if len(respA.Kvs) != 2 {
		t.Errorf("Group A should have 2 members, got %d", len(respA.Kvs))
	}

	// Verify group B has 3 members
	respB, err := consumerB1.etcdClient.Get(context.Background(), "/consumer-group/"+groupB+"/members/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("Failed to get group B members: %v", err)
	}
	if len(respB.Kvs) != 3 {
		t.Errorf("Group B should have 3 members, got %d", len(respB.Kvs))
	}

	t.Logf("Mixed scenario test passed:")
	t.Logf("  Group %s has %d members", groupA, len(respA.Kvs))
	t.Logf("  Group %s has %d members", groupB, len(respB.Kvs))
}

// Test consumer cleanup (StopConsumer removes from etcd)
func TestStopConsumer_RemovesFromEtcd(t *testing.T) {
	groupID := "test-group-cleanup"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	consumer := NewConsumer(groupID)
	if consumer == nil {
		t.Fatal("Failed to create consumer")
	}

	consumerID := string(consumer.ID)

	// Verify consumer is registered
	resp, err := consumer.etcdClient.Get(context.Background(), "/consumer-group/"+groupID+"/members/"+consumerID)
	if err != nil {
		t.Fatalf("Failed to check consumer registration: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Error("Consumer not registered before stop")
	}

	// Stop consumer
	consumer.StopConsumer()

	// Wait for lease to expire
	time.Sleep(12 * time.Second)

	// Verify consumer is removed (lease expired)
	etcdClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	defer etcdClient.Close()

	resp, err = etcdClient.Get(context.Background(), "/consumer-group/"+groupID+"/members/"+consumerID)
	if err != nil {
		t.Fatalf("Failed to check consumer after stop: %v", err)
	}
	if len(resp.Kvs) > 0 {
		t.Error("Consumer still registered after lease expiration")
	}

	t.Logf("Consumer successfully removed from etcd after stop")
}

// Test rebalancing with single consumer and single partition
func TestRebalance_SingleConsumerSinglePartition(t *testing.T) {
	groupID := "test-rebalance-1c15"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	// Create topic with 1 partition in etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	topicName := "test-topic"
	topicMetadata := `{"topic":"test-topic","partitions":{"0":8080}}`
	_, err = etcdClient.Put(context.Background(), "/topic/"+topicName+"/config", topicMetadata)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create consumer
	consumer := NewConsumer(groupID)
	if consumer == nil {
		t.Fatal("Failed to create consumer")
	}
	defer consumer.StopConsumer()

	// Subscribe to topic
	consumer.subscribedTopics = map[string]*ConsumerTopicMetadata{
		topicName: {Name: topicName},
	}

	time.Sleep(1 * time.Second)

	log.Println("Manual rebalance test started")

	// Trigger rebalance
	event := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte("/consumer-group/" + groupID + "/members/" + string(consumer.ID))},
	}
	consumer.rebalanceConsumer(event)

	// Wait for rebalancing to complete
	time.Sleep(1 * time.Second)

	// Verify partition assignment
	assignmentKey := "/consumer-group/" + groupID + "/" + topicName + "/0/assignment"
	resp, err := etcdClient.Get(context.Background(), assignmentKey)
	if err != nil {
		t.Fatalf("Failed to get partition assignment: %v", err)
	}

	if len(resp.Kvs) == 0 {
		t.Error("Partition not assigned to consumer")
	} else if string(resp.Kvs[0].Value) != string(consumer.ID) {
		t.Errorf("Expected partition assigned to %s, got %s", consumer.ID, string(resp.Kvs[0].Value))
	}

	t.Logf("Successfully assigned partition 0 to consumer %s", consumer.ID)
}

// Test rebalancing with 2 consumers and 4 partitions (even distribution)
func TestRebalance_TwoConsumersFourPartitions(t *testing.T) {
	groupID := "test-rebalance-2c4p"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	// Create topic with 4 partitions in etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	topicName := "test-topic-4p"
	topicMetadata := `{"topic":"test-topic-4p","partitions":{"0":8080,"1":8081,"2":8080,"3":8081}}`
	_, err = etcdClient.Put(context.Background(), "/topic/"+topicName+"/config", topicMetadata)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create two consumers
	consumer1 := NewConsumer(groupID)
	if consumer1 == nil {
		t.Fatal("Failed to create consumer 1")
	}
	defer consumer1.StopConsumer()

	consumer2 := NewConsumer(groupID)
	if consumer2 == nil {
		t.Fatal("Failed to create consumer 2")
	}
	defer consumer2.StopConsumer()

	// Subscribe both consumers to topic
	consumer1.subscribedTopics = map[string]*ConsumerTopicMetadata{
		topicName: {Name: topicName},
	}
	consumer2.subscribedTopics = map[string]*ConsumerTopicMetadata{
		topicName: {Name: topicName},
	}

	// Trigger rebalance on both consumers
	event := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte("/consumer-group/" + groupID + "/members/")},
	}

	consumer1.rebalanceConsumer(event)
	consumer2.rebalanceConsumer(event)

	// Wait for rebalancing to complete
	time.Sleep(2 * time.Second)

	// Count partition assignments for each consumer
	consumer1Partitions := 0
	consumer2Partitions := 0

	for i := 0; i < 4; i++ {
		assignmentKey := "/consumer-group/" + groupID + "/" + topicName + "/" + string(rune('0'+i)) + "/assignment"
		resp, err := etcdClient.Get(context.Background(), assignmentKey)
		if err != nil {
			t.Logf("Warning: Failed to get partition %d assignment: %v", i, err)
			continue
		}

		if len(resp.Kvs) > 0 {
			assignedTo := string(resp.Kvs[0].Value)
			if assignedTo == string(consumer1.ID) {
				consumer1Partitions++
			} else if assignedTo == string(consumer2.ID) {
				consumer2Partitions++
			}
			t.Logf("Partition %d assigned to %s", i, assignedTo)
		}
	}

	// Verify even distribution (2 partitions each)
	if consumer1Partitions != 2 {
		t.Errorf("Expected consumer 1 to have 2 partitions, got %d", consumer1Partitions)
	}
	if consumer2Partitions != 2 {
		t.Errorf("Expected consumer 2 to have 2 partitions, got %d", consumer2Partitions)
	}

	t.Logf("Partition distribution: Consumer1=%d, Consumer2=%d", consumer1Partitions, consumer2Partitions)
}

// Test rebalancing with 3 consumers and 4 partitions (uneven distribution)
func TestRebalance_ThreeConsumersFourPartitions(t *testing.T) {
	groupID := "test-rebalance-3c4p"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	// Create topic with 4 partitions
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	topicName := "test-topic-uneven"
	topicMetadata := `{"topic":"test-topic-uneven","partitions":{"0":8080,"1":8081,"2":8082,"3":8080}}`
	_, err = etcdClient.Put(context.Background(), "/topic/"+topicName+"/config", topicMetadata)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create three consumers
	consumers := make([]*Consumer, 3)
	for i := 0; i < 3; i++ {
		consumers[i] = NewConsumer(groupID)
		if consumers[i] == nil {
			t.Fatalf("Failed to create consumer %d", i)
		}
		defer consumers[i].StopConsumer()

		consumers[i].subscribedTopics = map[string]*ConsumerTopicMetadata{
			topicName: {Name: topicName},
		}
	}

	// Trigger rebalance
	event := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte("/consumer-group/" + groupID + "/members/")},
	}

	for _, c := range consumers {
		c.rebalanceConsumer(event)
	}

	// Wait for rebalancing
	time.Sleep(2 * time.Second)

	// Count partitions per consumer
	partitionCounts := make(map[string]int)

	for i := 0; i < 4; i++ {
		assignmentKey := "/consumer-group/" + groupID + "/" + topicName + "/" + string(rune('0'+i)) + "/assignment"
		resp, err := etcdClient.Get(context.Background(), assignmentKey)
		if err != nil {
			continue
		}

		if len(resp.Kvs) > 0 {
			assignedTo := string(resp.Kvs[0].Value)
			partitionCounts[assignedTo]++
			t.Logf("Partition %d assigned to %s", i, assignedTo)
		}
	}

	// Verify distribution: should be 1,1,2 or 1,2,1 or 2,1,1
	totalAssigned := 0
	for _, count := range partitionCounts {
		totalAssigned += count
		if count < 1 || count > 2 {
			t.Errorf("Invalid partition count: %d (should be 1 or 2)", count)
		}
	}

	if totalAssigned != 4 {
		t.Errorf("Expected 4 total partitions assigned, got %d", totalAssigned)
	}

	t.Logf("Partition distribution: %v", partitionCounts)
}

// Test rebalancing when consumer leaves (simulated by stopping consumer)
func TestRebalance_ConsumerLeaves(t *testing.T) {
	groupID := "test-rebalance-leave"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	// Create topic
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	topicName := "test-topic-leave"
	topicMetadata := `{"topic":"test-topic-leave","partitions":{"0":8080,"1":8081}}`
	_, err = etcdClient.Put(context.Background(), "/topic/"+topicName+"/config", topicMetadata)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create two consumers
	consumer1 := NewConsumer(groupID)
	if consumer1 == nil {
		t.Fatal("Failed to create consumer 1")
	}
	defer consumer1.StopConsumer()

	consumer2 := NewConsumer(groupID)
	if consumer2 == nil {
		t.Fatal("Failed to create consumer 2")
	}

	// Subscribe to topic
	consumer1.subscribedTopics = map[string]*ConsumerTopicMetadata{
		topicName: {Name: topicName},
	}
	consumer2.subscribedTopics = map[string]*ConsumerTopicMetadata{
		topicName: {Name: topicName},
	}

	// Initial rebalance
	event := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte("/consumer-group/" + groupID + "/members/")},
	}
	consumer1.rebalanceConsumer(event)
	consumer2.rebalanceConsumer(event)

	time.Sleep(2 * time.Second)

	// Consumer 2 leaves
	t.Logf("Consumer 2 (%s) leaving group", consumer2.ID)
	consumer2.StopConsumer()

	// Wait for lease to expire
	time.Sleep(12 * time.Second)

	// Consumer 1 rebalances (should take all partitions)
	consumer1.rebalanceConsumer(event)
	time.Sleep(2 * time.Second)

	// Verify consumer 1 has both partitions
	consumer1Partitions := 0
	for i := 0; i < 2; i++ {
		assignmentKey := "/consumer-group/" + groupID + "/" + topicName + "/" + string(rune('0'+i)) + "/assignment"
		resp, err := etcdClient.Get(context.Background(), assignmentKey)
		if err != nil {
			continue
		}

		if len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) == string(consumer1.ID) {
			consumer1Partitions++
			t.Logf("Partition %d now assigned to consumer 1", i)
		}
	}

	if consumer1Partitions != 2 {
		t.Errorf("Expected consumer 1 to have 2 partitions after consumer 2 left, got %d", consumer1Partitions)
	}

	t.Logf("Consumer 1 successfully took over all partitions after consumer 2 left")
}

// Test rebalancing with more consumers than partitions
func TestRebalance_MoreConsumersThanPartitions(t *testing.T) {
	groupID := "test-rebalance-more-consumers"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	// Create topic with 2 partitions
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	topicName := "test-topic-2p"
	topicMetadata := `{"topic":"test-topic-2p","partitions":{"0":8080,"1":8081}}`
	_, err = etcdClient.Put(context.Background(), "/topic/"+topicName+"/config", topicMetadata)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Create 4 consumers (more than partitions)
	consumers := make([]*Consumer, 4)
	for i := 0; i < 4; i++ {
		consumers[i] = NewConsumer(groupID)
		if consumers[i] == nil {
			t.Fatalf("Failed to create consumer %d", i)
		}
		defer consumers[i].StopConsumer()

		consumers[i].subscribedTopics = map[string]*ConsumerTopicMetadata{
			topicName: {Name: topicName},
		}
	}

	// Trigger rebalance
	event := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte("/consumer-group/" + groupID + "/members/")},
	}

	for _, c := range consumers {
		c.rebalanceConsumer(event)
	}

	time.Sleep(2 * time.Second)

	// Count how many consumers got partitions
	consumersWithPartitions := 0
	for _, c := range consumers {
		hasPartition := false
		for i := 0; i < 2; i++ {
			assignmentKey := "/consumer-group/" + groupID + "/" + topicName + "/" + string(rune('0'+i)) + "/assignment"
			resp, err := etcdClient.Get(context.Background(), assignmentKey)
			if err != nil {
				continue
			}

			if len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) == string(c.ID) {
				hasPartition = true
				break
			}
		}
		if hasPartition {
			consumersWithPartitions++
		}
	}

	// Only 2 consumers should have partitions
	if consumersWithPartitions != 2 {
		t.Errorf("Expected 2 consumers with partitions, got %d", consumersWithPartitions)
	}

	t.Logf("Correctly handled more consumers (%d) than partitions (2): %d consumers got partitions", len(consumers), consumersWithPartitions)
}

// Test creating consumer when etcd is unavailable
func TestNewConsumer_EtcdUnavailable(t *testing.T) {
	// This test will skip if etcd is unavailable
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Skip("Skipping test: etcd not available")
	}
	etcdClient.Close()

	// If we get here, etcd is available, so we can run the test
	groupID := "test-group-etcd-check"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	consumer := NewConsumer(groupID)
	if consumer == nil {
		t.Fatal("NewConsumer returned nil even though etcd is available")
	}
	defer consumer.StopConsumer()

	t.Log("Consumer created successfully with etcd available")
}
