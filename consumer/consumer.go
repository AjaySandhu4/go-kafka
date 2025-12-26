package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"go-kafka/cluster"
	consumerpb "go-kafka/proto/consumer"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	brokersMetadata  *cluster.BrokersMetadata
	topicsMetadata   *cluster.TopicsMetadata
	clientConn       map[cluster.Port]*ClientConn
}

type ClientConn struct {
	conn   *grpc.ClientConn
	client consumerpb.ConsumerServiceClient
}

type ConsumerTopicMetadata struct {
	Name                 string
	PartitionMetadataMap map[cluster.PartitionKey]*PartitionMetadata
}

type PartitionMetadata struct {
	Partition  cluster.PartitionKey
	Broker     cluster.Port
	LastOffset int
	Persisted  bool // Indicates if most recent offset has been persisted
}

var OffsetUpdateInterval = 4 * time.Second

func NewConsumer(groupID string) *Consumer {
	log.Println("Creating new Consumer...")
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		ID:               ConsumerID(uuid.New().String()),
		GroupID:          ConsumerGroupID(groupID),
		ctx:              ctx,
		cancel:           cancel,
		subscribedTopics: make(map[string]*ConsumerTopicMetadata),
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
	c.registerBrokerWatcher()

	err = c.joinGroup(groupID)
	if err != nil {
		log.Fatalf("Failed to join consumer group: %v", err)
	}

	go c.updateOffsetsTicker()

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

func (c *Consumer) registerBrokerWatcher() {
	// Implement the logic to watch for changes in the broker metadata
	watchChan := c.etcdClient.Watch(c.ctx, "/broker/", clientv3.WithPrefix())
	go func() {
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				log.Printf("Consumer %s received broker event: %s %q\n", c.ID, event.Type, event.Kv.Key)
				c.fetchBrokerMetadata()
				c.populateClientConnections()
			}
		}
	}()
}

func (c *Consumer) rebalanceConsumer(event *clientv3.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
		// Initialize topic metadata if not already done
		if c.subscribedTopics[topic] == nil {
			c.subscribedTopics[topic] = &ConsumerTopicMetadata{
				Name:                 topic,
				PartitionMetadataMap: make(map[cluster.PartitionKey]*PartitionMetadata),
			}
		}
		partitionSet := make(map[cluster.PartitionKey]struct{})
		// Read in array of partitions for topic (set P)
		topicRes, err := c.etcdClient.Get(c.ctx, "/topic/"+topic, clientv3.WithPrefix())
		if err != nil {
			log.Printf("Consumer %s failed to read partitions for topic %s: %v", c.ID, topic, err)
			continue
		}
		for _, kv := range topicRes.Kvs {
			var meta cluster.TopicMetadata
			if err := json.Unmarshal(kv.Value, &meta); err != nil {
				log.Printf("Failed to unmarshal topic metadata: %v", err)
				continue
			}
			for pk := range meta.Partitions {
				partitionSet[pk] = struct{}{}
			}
		}
		partitions := make([]cluster.PartitionKey, 0, len(partitionSet))
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
		var assignedPartitions []cluster.PartitionKey
		if partitionsPerConsumer == 0 {
			if consumerIndex >= extraPartitions {
				log.Printf("Consumer %s will not be assigned any partitions for topic %s", c.ID, topic)
				continue
			}
			assignedPartitions = []cluster.PartitionKey{partitions[consumerIndex]}
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
				// Get broker port from topics metadata
				var brokerPort cluster.Port
				if c.topicsMetadata != nil && c.topicsMetadata.Topics[topic] != nil {
					brokerPort = c.topicsMetadata.Topics[topic].Partitions[pk]
				}
				c.subscribedTopics[topic].PartitionMetadataMap[pk] = &PartitionMetadata{
					Partition:  pk,
					Broker:     brokerPort,
					LastOffset: -1,
				}
				lastOffset, err := c.getPartitionOffset(topic, pk)
				if err != nil {
					log.Printf("Consumer %s failed to get last offset for partition %d of topic %s: %v", c.ID, pk, topic, err)
					lastOffset = 0 // Default to 0 on failure
				}
				c.subscribedTopics[topic].PartitionMetadataMap[pk].LastOffset = lastOffset
			} else {
				log.Printf("Consumer %s failed to assign partition %d of topic %s after %d attempts",
					c.ID, pk, topic, attempts)
			}
		}
		log.Printf("Consumer %s completed partition assignment for topic %s", c.ID, topic)
	}

	log.Printf("Consumer %s completed handling consumer group change", c.ID)

}

func (c *Consumer) PullMessages(topicName string, maxMsgs int, maxBytes int) ([]*consumerpb.Message, error) {
	if c.subscribedTopics[topicName] == nil {
		log.Printf("Consumer %s is not subscribed to topic %s", c.ID, topicName)
		return nil, errors.New("Not subscribed to topic")
	}
	if c.brokersMetadata == nil {
		log.Printf("Consumer %s has no broker metadata", c.ID)
		return nil, errors.New("No broker metadata")
	}
	c.mu.RLock()
	consumerTopicMeta, ok := c.subscribedTopics[topicName] // TODO: Use this to pull messages from assigned partitions
	c.mu.RUnlock()
	if !ok {
		log.Printf("Consumer %s is not subscribed to topic %s", c.ID, topicName)
		return nil, errors.New("Not subscribed to topic")
	}
	partitionMetaMap := consumerTopicMeta.PartitionMetadataMap
	if len(partitionMetaMap) == 0 {
		log.Printf("Consumer %s has no assigned partitions for topic %s", c.ID, topicName)
		return []*consumerpb.Message{}, nil
	}

	var messages []*consumerpb.Message
	var wg sync.WaitGroup
	var msgMu sync.Mutex
	for pk, partitionMeta := range partitionMetaMap {
		wg.Add(1)
		go func(partitionKey cluster.PartitionKey, pMeta *PartitionMetadata) {
			defer wg.Done()
			brokerPort := pMeta.Broker
			clientConn, ok := c.clientConn[brokerPort]
			if !ok {
				log.Printf("Consumer %s has no client connection to broker %d for partition %d of topic %s",
					c.ID, brokerPort, partitionKey, topicName)
				return
			}
			req := &consumerpb.ConsumeRequest{
				Topic:        topicName,
				PartitionKey: int32(partitionKey),
				Offset:       int64(pMeta.LastOffset + 1),
				MaxMessages:  int32(maxMsgs),
				MaxBytes:     int32(maxBytes),
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resp, err := clientConn.client.ConsumeMessages(ctx, req)
			if err != nil {
				log.Printf("Consumer %s failed to pull messages from broker %d for partition %d of topic %s: %v",
					c.ID, brokerPort, partitionKey, topicName, err)
				return
			}
			if len(resp.Messages) == 0 {
				log.Printf("Consumer %s received no messages from broker %d for partition %d of topic %s",
					c.ID, brokerPort, partitionKey, topicName)
				return
			}
			log.Printf("Consumer %s received %d messages from broker %d for partition %d of topic %s",
				c.ID, len(resp.Messages), brokerPort, partitionKey, topicName)

			// Update last offset
			c.mu.Lock()
			if pMeta.LastOffset < int(resp.LastOffset) {
				pMeta.LastOffset = int(resp.LastOffset)
				pMeta.Persisted = false
			}
			c.mu.Unlock()

			// Append messages to the result slice
			msgMu.Lock()
			defer msgMu.Unlock()
			for _, msg := range resp.Messages {
				messages = append(messages, msg)
			}
		}(pk, partitionMeta)
	}
	wg.Wait()
	return messages, nil
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
func (c *Consumer) assignedPartition(topic string, partition cluster.PartitionKey) bool {
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

func (c *Consumer) fetchBrokerMetadata() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	brokersMetadata, err := cluster.FetchBrokerMetadata(c.etcdClient)
	if err != nil {
		return err
	}
	c.brokersMetadata = brokersMetadata

	// Also fetch topic metadata
	topicsMetadata, err := cluster.FetchTopicMetadata(c.etcdClient)
	if err != nil {
		return err
	}
	c.topicsMetadata = topicsMetadata

	return nil
}

func (c *Consumer) populateClientConnections() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.clientConn == nil {
		c.clientConn = make(map[cluster.Port]*ClientConn)
	}

	for port := range c.brokersMetadata.Brokers {
		if _, exists := c.clientConn[port]; !exists {
			conn, err := grpc.NewClient("localhost:"+strconv.Itoa(int(port)), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to broker at port %d: %v", port, err)
				return err
			}
			c.clientConn[port] = &ClientConn{
				conn:   conn,
				client: consumerpb.NewConsumerServiceClient(conn),
			}
			log.Printf("Connected to broker at port %d", port)
		}
	}
	return nil
}

func (c *Consumer) getPartitionOffset(topic string, partition cluster.PartitionKey) (int, error) {
	offsetKey := "/topic/" + topic + "/partitions/" + strconv.Itoa(int(partition)) + "/offset"
	_, err := c.etcdClient.Txn(c.ctx).
		If(clientv3.Compare(clientv3.ModRevision(offsetKey), "=", 0)).
		Then(clientv3.OpPut(offsetKey, "0")).
		Commit()
	res, err := c.etcdClient.Get(c.ctx, offsetKey)
	if err != nil {
		log.Printf("Consumer %s failed to get offset for partition %d of topic %s: %v", c.ID, partition, topic, err)
		return -1, err
	}
	if len(res.Kvs) == 0 {
		log.Printf("No offset found for partition %d of topic %s in topic metadata", partition, topic)
		return -1, nil
	}
	var offset int
	if err := json.Unmarshal(res.Kvs[0].Value, &offset); err != nil {
		log.Printf("Consumer %s failed to unmarshal offset for partition %d of topic %s: %v", c.ID, partition, topic, err)
		return -1, err
	}
	return offset, nil
}

func (c *Consumer) updateOffsetsTicker() {
	ticker := time.NewTicker(OffsetUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Println("Flush ticker triggered - flushing all logs")
			c.updateOffsets()
		case <-c.ctx.Done():
			log.Println("Flush ticker stopping due to consumer shutdown")
			return
		}
	}
}

func (c *Consumer) updateOffsets() {
	for topic, consumerTopicMeta := range c.subscribedTopics {
		for partition, pMeta := range consumerTopicMeta.PartitionMetadataMap {
			if pMeta.Persisted {
				continue
			}

			// Update the offset in etcd
			// First get current value
			offsetKey := fmt.Sprintf("/consumer-group/%s/%s/%d/offset", c.ID, topic, partition)
			currentOffset, err := c.getPartitionOffset(topic, partition)
			if err != nil {
				log.Printf("Failed to get current offset for partition %d of topic %s: %v", partition, topic, err)
				continue
			}
			log.Printf("Current offset for partition %d of topic %s is %d", partition, topic, currentOffset)

			if pMeta.LastOffset <= currentOffset {
				log.Printf("No new offset to update for partition %d of topic %s (last offset: %d, current offset: %d)", partition, topic, pMeta.LastOffset, currentOffset)
				c.mu.Lock()
				pMeta.LastOffset = currentOffset
				pMeta.Persisted = true
				c.mu.Unlock()
				continue
			}

			// Update offset
			_, err = c.etcdClient.Put(context.Background(), offsetKey, fmt.Sprintf("%d", pMeta.LastOffset))
			if err != nil {
				log.Printf("Failed to update offset for partition %d of topic %s: %v", partition, topic, err)
				continue
			}
			pMeta.Persisted = true
		}
	}
}
