package consumer

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"

	"go-kafka/broker"
	"slices"
)

type ConsumerGroup struct {
	ID      string
	Members map[string]*Consumer
	topics  map[string][]int // topic to partition list
}

type ConsumerID string
type ConsumerGroupID string

type Consumer struct {
	ID               ConsumerID
	GroupID          ConsumerGroupID
	ctx              context.Context
	cancel           context.CancelFunc
	etcdClient       *clientv3.Client
	mu               sync.RWMutex
	group            *ConsumerGroup
	subscribedTopics map[string]*ConsumerTopicMetadata
	partitionLease   clientv3.LeaseID
}

type ConsumerTopicMetadata struct {
	Name                 string
	PartitionMetadataMap map[*broker.PartitionKey]*PartitionMetadata
}

type PartitionMetadata struct {
	Partition  broker.PartitionKey
	Broker     broker.Port
	LastOffset int
}

func NewConsumer(groupID string) *Consumer {
	log.Println("Creating new Consumer...")
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		ID:      ConsumerID(uuid.New().String()),
		GroupID: ConsumerGroupID(groupID),
		ctx:     ctx,
		cancel:  cancel,
	}
	var err error

	log.Println("Connecting to etcd...")
	// Initialize etcd client
	c.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}

	c.registerConsumerWatcher()

	err = c.joinGroup(groupID)
	if err != nil {
		log.Fatalf("Failed to join consumer group: %v", err)
	}
	return c
}

func (c *Consumer) StopConsumer() {
	log.Println("Stopping Consumer...")
	if c.cancel != nil {
		c.cancel()
	}
	if c.etcdClient != nil {
		c.etcdClient.Close()
	}
}

func (c *Consumer) joinGroup(groupID string) error {
	log.Println("Joining Consumer Group", groupID)
	var txn clientv3.Txn
	var kaCh <-chan *clientv3.LeaseKeepAliveResponse

	consumerLifetimeLease, err := c.etcdClient.Grant(context.Background(), 4)
	if err != nil {
		goto error_return
	}

	txn = c.etcdClient.Txn(context.Background())
	_, err = txn.
		If(clientv3.Compare(clientv3.Version("/consumer-group/"+groupID), "=", 0)).
		Then(clientv3.OpPut("/consumer-group/"+groupID, "")).
		Commit()
	if err != nil {
		goto error_return
	}

	txn = c.etcdClient.Txn(context.Background())
	_, err = txn.
		If(clientv3.Compare(clientv3.Version("/consumer-group/"+groupID+"/members/"+string(c.ID)), "=", 0)).
		Then(clientv3.OpPut("/consumer-group/"+groupID+"/members/"+string(c.ID), "", clientv3.WithLease(consumerLifetimeLease.ID))).
		Commit()
	if err != nil {
		goto error_return
	}
	// Set up automatic lease renewal
	kaCh, err = c.etcdClient.KeepAlive(c.ctx, consumerLifetimeLease.ID)
	if err != nil {
		goto error_return
	}
	// Handle lease renewal responses in background
	go func() {
		for ka := range kaCh {
			log.Printf("Lease renewed: ID=%d, TTL=%d", ka.ID, ka.TTL)
		}
		log.Println("Lease keep alive channel closed")
	}()
	log.Println("Consumer", c.ID, "successfully joined Consumer Group", groupID)
	return nil

error_return:
	log.Printf("Failed to join consumer group: %v", err)
	return err
}

func (c *Consumer) registerConsumerWatcher() {
	// Implement the logic to watch for changes in the consumer group
	watchChan := c.etcdClient.Watch(c.ctx, "/consumer-group/"+string(c.GroupID)+"/members/", clientv3.WithPrefix())
	go func() {
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				log.Printf("Consumer %s received event: %s %q\n", c.ID, event.Type, event.Kv.Key)
				c.rebalanceConsumer(event)
			}
		}
	}()
}

func (c *Consumer) rebalanceConsumer(event *clientv3.Event) {
	// Implement the logic to rebalance consumers in the group
	log.Printf("Rebalancing consumers for event: %s %q\n", event.Type, event.Kv.Key)
	// For example, if a new consumer joins or leaves, rebalance partitions

	// Let C_i be this consumer
	// Make C_i release (expire the lease) of all partitions it currently holds
	if c.partitionLease != 0 {
		_, err := c.etcdClient.Revoke(context.Background(), c.partitionLease)
		if err != nil {
			log.Printf("Consumer %s failed to release partition lease: %v", c.ID, err)
		} else {
			log.Printf("Consumer %s released partition lease", c.ID)
		}
		c.partitionLease = 0
	}

	// Recreate partition lease
	err := c.setupPartitionLease()
	if err != nil {
		log.Printf("Consumer %s failed to set up new partition lease: %v", c.ID, err)
		return
	}
	for c.partitionLease == 0 {
		log.Println("Waiting for partition lease to be established...")
		time.Sleep(100 * time.Millisecond)
	}

	// Read in array of consumer members (set C)
	res, err := c.etcdClient.Get(c.ctx, "/consumer-group/"+string(c.GroupID)+"/members/", clientv3.WithPrefix())
	if err != nil {
		log.Printf("Consumer %s failed to read consumer members: %v", c.ID, err)
		return
	}
	var consumers []ConsumerID
	for _, kv := range res.Kvs {
		fullPath := string(kv.Key)
		prefix := "/consumer-group/" + string(c.GroupID) + "/members/"
		member := fullPath[len(prefix):]
		consumers = append(consumers, ConsumerID(member))
	}

	if len(consumers) == 0 {
		log.Printf("No consumers found in group %s", c.GroupID)
		return
	}

	// Sort consumers by id
	slices.Sort(consumers)
	log.Printf("Consumer group %s members after sorting: %v", c.GroupID, consumers)

	// Take position of C_i and call it i
	consumerIndex := slices.Index(consumers, c.ID)
	if consumerIndex == -1 {
		log.Printf("Consumer %s not found in consumer list", c.ID)
		return
	}
	log.Printf("Consumer %s has index %d in consumer group %s", c.ID, consumerIndex, c.GroupID)

	if consumerIndex == len(consumers) || consumers[consumerIndex] != c.ID {
		log.Printf("Consumer %s not found in consumer list after re-reading members", c.ID)
		return
	}

	// Repeat partition assignment for each topic
	for topic := range c.subscribedTopics {
		partitionSet := make(map[broker.PartitionKey]struct{})
		// Read in array of partitions for topic (set P)
		topicRes, err := c.etcdClient.Get(c.ctx, "/topic/"+topic, clientv3.WithPrefix())
		if err != nil {
			log.Printf("Consumer %s failed to read partitions for topic %s: %v", c.ID, topic, err)
			continue
		}
		for _, kv := range topicRes.Kvs {
			var meta broker.TopicMetadata
			if err := json.Unmarshal(kv.Value, &meta); err != nil {
				log.Printf("Failed to unmarshal topic metadata: %v", err)
				continue
			}
			for pk := range meta.Partitions {
				partitionSet[pk] = struct{}{}
			}
		}
		partitions := make([]broker.PartitionKey, 0, len(partitionSet))
		for pk := range partitionSet {
			partitions = append(partitions, pk)
		}
		log.Printf("Consumer %s found %d partitions for topic %s", string(c.ID), len(partitions), topic)

		// Sort partitions by partition key
		slices.Sort(partitions)

		// Divide P into |C| disjoint groups G
		if len(consumers) > len(partitions) {
			log.Printf("More consumers (%d) than partitions (%d) for topic %s; some consumers will not get partitions", len(consumers), len(partitions), topic)
		}
		partitionsPerConsumer := len(partitions) / len(consumers)
		log.Println("Partitions per consumer:", partitionsPerConsumer)
		extraPartitions := len(partitions) % len(consumers)
		log.Println("Remainder partitions:", extraPartitions)
		var assignedPartitions []broker.PartitionKey
		if partitionsPerConsumer == 0 {
			if consumerIndex >= extraPartitions {
				log.Printf("Consumer %s will not be assigned any partitions for topic %s", c.ID, topic)
				continue
			}
			assignedPartitions = []broker.PartitionKey{partitions[consumerIndex]}
		} else if consumerIndex == len(consumers)-1 && extraPartitions > 0 {
			assignedPartitions = partitions[consumerIndex*partitionsPerConsumer:]
		} else {
			startIndex := consumerIndex * partitionsPerConsumer
			endIndex := startIndex + partitionsPerConsumer
			log.Printf("Consumer %s assigned partitions from index %d to %d for topic %s", c.ID, startIndex, endIndex, topic)
			assignedPartitions = partitions[startIndex:endIndex]
		}

		// Assign C_i the partitions in G_i
		for _, pk := range assignedPartitions {
			success := false
			attempts := 0
			maxAttempts := 20

			for !success && attempts < maxAttempts {
				attempts++
				success = c.assignedPartition(topic, pk)

				// If assignment failed, wait before retrying
				if !success {
					if attempts >= maxAttempts {
						log.Printf("Consumer %s exceeded maximum attempts (%d) to assign partition %d of topic %s",
							c.ID, maxAttempts, pk, topic)
						break
					}
					log.Printf("Consumer %s failed to assign partition %d of topic %s (attempt %d/%d), retrying...",
						c.ID, pk, topic, attempts, maxAttempts)
					time.Sleep(100 * time.Millisecond)
				}
			}

			if success {
				log.Printf("Consumer %s successfully assigned partition %d of topic %s after %d attempt(s)",
					c.ID, pk, topic, attempts)
			} else {
				log.Printf("Consumer %s failed to assign partition %d of topic %s after %d attempts",
					c.ID, pk, topic, attempts)
			}
		}
		log.Printf("Consumer %s completed partition assignment for topic %s", c.ID, topic)
	}
	log.Printf("Consumer %s completed handling consumer group change", c.ID)

}

func (c *Consumer) pullMessages(topicName string, maxMsgs int, maxBytes int) [][]byte {
	var messages [][]byte
	// Implement the logic to pull messages from the specified topic
	return messages
}

func (c *Consumer) setupPartitionLease() error {
	leaseResp, err := c.etcdClient.Grant(context.Background(), 4)
	if err != nil {
		log.Printf("Consumer %s failed to create new partition lease: %v", c.ID, err)
		return err
	}
	c.partitionLease = leaseResp.ID
	kaCh, err := c.etcdClient.KeepAlive(c.ctx, c.partitionLease)
	if err != nil {
		log.Printf("Consumer %s failed to set up lease keep alive: %v", c.ID, err)
		return err
	}
	// Handle lease renewal responses in background
	go func() {
		for range kaCh {
			// log.Printf("Consumer %s partition lease renewed: ID=%d, TTL=%d", c.ID, ka.ID, ka.TTL)
		}
		log.Printf("Consumer %s partition lease keep alive channel closed", c.ID)
	}()
	return nil
}

// assignedPartition attempts to atomically claim a partition for this consumer.
// It returns true if the partition was successfully claimed, false otherwise.
// It handles three cases:
// 1. Key doesn't exist -> claim it
// 2. Key exists but lease expired -> clean up and claim it
// 3. Key exists with valid lease -> return false
func (c *Consumer) assignedPartition(topic string, partition broker.PartitionKey) bool {
	assignmentKey := "/consumer-group/" + string(c.GroupID) + "/" + topic + "/" + strconv.Itoa(int(partition)) + "/assignment"

	// First, get the current assignment state
	getResp, err := c.etcdClient.Get(c.ctx, assignmentKey)
	if err != nil {
		log.Printf("Consumer %s failed to get assignment for partition %d of topic %s: %v", c.ID, partition, topic, err)
		return false
	}

	// Case 1: Key doesn't exist - try to claim it
	if len(getResp.Kvs) == 0 {
		txn := c.etcdClient.Txn(c.ctx)
		txnResp, err := txn.
			If(clientv3.Compare(clientv3.Version(assignmentKey), "=", 0)).
			Then(clientv3.OpPut(assignmentKey, string(c.ID), clientv3.WithLease(c.partitionLease))).
			Commit()
		if err != nil {
			log.Printf("Consumer %s failed to assign partition %d of topic %s: %v", c.ID, partition, topic, err)
			return false
		}
		if txnResp.Succeeded {
			log.Printf("Consumer %s successfully claimed partition %d of topic %s (key didn't exist)", c.ID, partition, topic)
			return true
		}
		log.Printf("Consumer %s failed to claim partition %d of topic %s (concurrent modification)", c.ID, partition, topic)
		return false
	}

	// Case 2 & 3: Key exists - check if lease is still valid
	kv := getResp.Kvs[0]
	currentLease := clientv3.LeaseID(kv.Lease)

	// Check if the lease is still valid
	leaseResp, err := c.etcdClient.TimeToLive(c.ctx, currentLease)
	if err != nil {
		log.Printf("Consumer %s failed to check lease for partition %d of topic %s: %v", c.ID, partition, topic, err)
		return false
	}

	// Case 2: Lease expired or doesn't exist - try to claim using CAS with ModRevision
	if leaseResp.TTL <= 0 {
		log.Printf("Consumer %s detected expired lease (TTL=%d) for partition %d of topic %s, attempting to claim",
			c.ID, leaseResp.TTL, partition, topic)

		// Use ModRevision-based CAS to safely claim the partition
		txn := c.etcdClient.Txn(c.ctx)
		txnResp, err := txn.
			If(clientv3.Compare(clientv3.ModRevision(assignmentKey), "=", kv.ModRevision)).
			Then(clientv3.OpPut(assignmentKey, string(c.ID), clientv3.WithLease(c.partitionLease))).
			Commit()
		if err != nil {
			log.Printf("Consumer %s failed to claim partition %d with expired lease: %v", c.ID, partition, err)
			return false
		}
		if txnResp.Succeeded {
			log.Printf("Consumer %s successfully claimed partition %d of topic %s (expired lease)", c.ID, partition, topic)
			return true
		}
		log.Printf("Consumer %s failed to claim partition %d of topic %s (concurrent modification during expired lease claim)", c.ID, partition, topic)
		return false
	}

	// Case 3: Lease is still valid - cannot claim
	currentOwner := string(kv.Value)
	if currentOwner == string(c.ID) {
		log.Printf("Consumer %s already owns partition %d of topic %s", c.ID, partition, topic)
		return true
	}
	log.Printf("Consumer %s cannot claim partition %d of topic %s (owned by %s with valid lease, TTL=%d)",
		c.ID, partition, topic, currentOwner, leaseResp.TTL)
	return false
}
