package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Port int
type PartitionKey int

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

func FetchBrokerMetadata(client *clientv3.Client) (*BrokersMetadata, error) {
	log.Println("Fetching broker metadata...")
	// Fetch broker info
	if client == nil {
		log.Println("Etcd client is not initialized")
		return nil, errors.New("Etcd client is not initialized")
	}
	brokerGetResp, err := client.Get(context.Background(), "/broker/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get broker keys from etcd: %v", err)
	}
	log.Println("Broker keys fetched from etcd")
	brokersMetadata := &BrokersMetadata{Brokers: make(map[Port]*BrokerMetadata)}
	for _, kv := range brokerGetResp.Kvs {
		port, err := strconv.Atoi(string(kv.Key[len("/broker/"):]))
		if err != nil {
			log.Printf("Invalid broker port in etcd key: %s", kv.Key)
			continue
		}
		brokersMetadata.Brokers[Port(port)] = &BrokerMetadata{Port: Port(port)}
	}

	return brokersMetadata, nil
}

func FetchTopicMetadata(client *clientv3.Client) (*TopicsMetadata, error) {
	log.Println("Fetching topic metadata...")
	// Fetch topic info
	if client == nil {
		log.Println("Etcd client is not initialized")
		return nil, errors.New("Etcd client is not initialized")
	}
	topicGetResp, err := client.Get(context.Background(), "/topic/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("Failed to get topic keys from etcd: %v", err)
	}
	topicsMetadata := &TopicsMetadata{Topics: make(map[string]*TopicMetadata)}
	for _, kv := range topicGetResp.Kvs {
		var meta TopicMetadata
		if err := json.Unmarshal(kv.Value, &meta); err != nil {
			log.Printf("Failed to unmarshal topic metadata: %v", err)
			continue
		}
		topicsMetadata.Topics[meta.Topic] = &meta
	}
	return topicsMetadata, nil
}
