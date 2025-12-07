package consumer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ConsumerGroup struct {
	ID      string
	Members map[string]*Consumer
	topics  map[string][]int // topic to partition list
}

type Consumer struct {
	ID         string
	GroupID    string
	ctx        context.Context
	cancel     context.CancelFunc
	etcdClient *clientv3.Client
	mu         sync.RWMutex
	group      *ConsumerGroup
}

func NewConsumer(groupID string) *Consumer {
	log.Println("Creating new Consumer...")
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		ID:      uuid.New().String(),
		GroupID: groupID,
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
	// Implement the logic to join the consumer group
	txn := c.etcdClient.Txn(context.Background())
	_, err := txn.
		If(clientv3.Compare(clientv3.Version("/consumer-group/"+groupID), "=", 0)).
		Then(clientv3.OpPut("/consumer-group/"+groupID, "")).
		Commit()
	if err != nil {
		goto error_return
	}
	txn = c.etcdClient.Txn(context.Background())
	_, err = txn.
		If(clientv3.Compare(clientv3.Version("/consumer-group/"+groupID+"/members/"+c.ID), "=", 0)).
		Then(clientv3.OpPut("/consumer-group/"+groupID+"/members/"+c.ID, "")).
		Commit()
	if err != nil {
		goto error_return
	}
	log.Println("Consumer", c.ID, "successfully joined Consumer Group", groupID)
	return nil

error_return:
	log.Printf("Failed to join consumer group: %v", err)
	return err
}

func (c *Consumer) registerConsumerWatcher() {
	// Implement the logic to watch for changes in the consumer group
	watchChan := c.etcdClient.Watch(c.ctx, "/consumer-group/"+c.GroupID, clientv3.WithPrefix())
	go func() {
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				log.Printf("Consumer %s received event: %s %q\n", c.ID, event.Type, event.Kv.Key)
				c.handleConsumerGroupChange(event)
			}
		}
	}()
}

func (c *Consumer) handleConsumerGroupChange(event *clientv3.Event) {
	// Implement the logic to handle changes in the consumer group
	log.Printf("Handling event for Consumer %s: %s %q\n", c.ID, event.Type, event.Kv.Key)
	// For example, if a new consumer joins or leaves, rebalance partitions
}

// func (c *Consumer) pullMessages() {
// 	// TODO: Implement message pulling logic
// }
