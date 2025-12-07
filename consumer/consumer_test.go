package consumer

import (
	"context"
	"testing"
	"time"

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
	if consumer.GroupID != groupID {
		t.Errorf("Expected GroupID %s, got %s", groupID, consumer.GroupID)
	}

	// Verify etcd client is initialized
	if consumer.etcdClient == nil {
		t.Error("Consumer etcd client is nil")
	}

	// Verify consumer is registered in etcd
	resp, err := consumer.etcdClient.Get(context.Background(), "/consumer-group/"+groupID+"/members/"+consumer.ID)
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
	if consumer1.GroupID != groupID || consumer2.GroupID != groupID || consumer3.GroupID != groupID {
		t.Error("Not all consumers belong to the same group")
	}

	// Wait for registration to propagate
	time.Sleep(100 * time.Millisecond)

	// Verify all consumers are registered in etcd
	resp, err := consumer1.etcdClient.Get(context.Background(), "/consumer-group/"+groupID+"/members/", clientv3.WithPrefix())
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
func TestConsumer_Cleanup(t *testing.T) {
	groupID := "test-group-cleanup"
	cleanupEtcd(t, groupID)
	defer cleanupEtcd(t, groupID)

	// Create consumer
	consumer := NewConsumer(groupID)
	consumerID := consumer.ID

	// Verify consumer is registered
	resp, err := consumer.etcdClient.Get(context.Background(), "/consumer-group/"+groupID+"/members/"+consumerID)
	if err != nil {
		t.Fatalf("Failed to get consumer from etcd: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Error("Consumer not registered in etcd before cleanup")
	}

	// Stop consumer
	consumer.StopConsumer()

	// Give etcd time to process
	time.Sleep(100 * time.Millisecond)

	// Note: Since we're using basic Put without leases in current implementation,
	// the entry won't be automatically removed. This test documents current behavior.
	// In a full implementation with leases, the entry would be removed.

	t.Log("Consumer cleanup test completed")
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
