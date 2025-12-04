package broker

import (
	"log"
)

// Pretty-print the entire broker state for debugging

func (b *brokerServer) PrintBroker() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	log.Println("==================== BROKER STATE ====================")
	log.Printf("Port: %d", b.port)
	log.Printf("Context Active: %v", b.ctx.Err() == nil)

	// Print broker metadata
	log.Println("\n--- BROKER METADATA ---")
	log.Printf("Known Brokers (%d):", len(b.ClusterMetadata.BrokersMetadata.Brokers))
	for port := range b.ClusterMetadata.BrokersMetadata.Brokers {
		if port == b.port {
			log.Printf("  - Port %d (THIS BROKER)", port)
		} else {
			log.Printf("  - Port %d", port)
		}
	}

	// Print topic metadata
	log.Printf("\nTopic ClusterMetadata from etcd (%d topics):", len(b.ClusterMetadata.TopicsMetadata.Topics))
	for topicName, topicMeta := range b.ClusterMetadata.TopicsMetadata.Topics {
		log.Printf("  Topic: %s", topicName)
		log.Printf("    Partitions: %d", topicMeta.NumPartitions)
		log.Printf("    Partition Assignments:")
		for partitionKey, port := range topicMeta.Partitions {
			if port == b.port {
				log.Printf("      Partition %d -> Port %d (THIS BROKER)", partitionKey, port)
			} else {
				log.Printf("      Partition %d -> Port %d", partitionKey, port)
			}
		}
	}

	// Print local topics and messages
	log.Printf("\n--- LOCAL TOPICS (%d) ---", len(b.Topics))
	for topicName, topic := range b.Topics {
		log.Printf("\nTopic: %s", topicName)
		log.Printf("  Name: %s", topic.Name)
		log.Printf("  Num Partitions: %d", topic.NumPartitions)
		log.Printf("  Local Partitions: %d", len(topic.Partitions))

		for partKey, partition := range topic.Partitions {
			if partition == nil {
				log.Printf("    Partition %d: nil", partKey)
				continue
			}
			log.Printf("    Partition %d:", partKey)
			log.Printf("      Messages: %d", len(partition.Messages))
			log.Printf("      Index Entries: %d", len(partition.Index))
			log.Printf("      Next Offset: %d", partition.NextOffset)

			// Print first few messages for debugging
			maxMsgsToPrint := 3
			if len(partition.Messages) > 0 {
				log.Printf("      Sample Messages (showing first %d):", maxMsgsToPrint)
				for i, msg := range partition.Messages {
					if i >= maxMsgsToPrint {
						log.Printf("        ... and %d more messages", len(partition.Messages)-maxMsgsToPrint)
						break
					}
					log.Printf("        [%d] %s", i, string(msg))
				}
			}
		}
	}

	log.Println("\n=====================================================")
}

// Compact version for quick checks
func (b *brokerServer) PrintBrokerSummary() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	log.Printf("Broker Summary: Port=%d, Controller=%v, Topics=%d, Known Brokers=%d",
		b.port, b.ClusterMetadata.BrokersMetadata.Controller, len(b.Topics), len(b.ClusterMetadata.BrokersMetadata.Brokers))

	totalMessages := 0
	for _, topic := range b.Topics {
		for _, partition := range topic.Partitions {
			if partition != nil {
				totalMessages += len(partition.Messages)
			}
		}
	}
	log.Printf("  Total messages stored locally: %d", totalMessages)
}

// Print specific topic details
func (b *brokerServer) PrintTopicDetails(topicName string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	log.Printf("========== TOPIC DETAILS: %s ==========", topicName)

	// Check local topic
	topic, exists := b.Topics[topicName]
	if !exists {
		log.Printf("Topic %s NOT FOUND in local storage", topicName)
	} else {
		log.Printf("Local Topic:")
		log.Printf("  Name: %s", topic.Name)
		log.Printf("  Num Partitions: %d", topic.NumPartitions)
		log.Printf("  Partitions stored locally: %d", len(topic.Partitions))

		for partKey, partition := range topic.Partitions {
			if partition == nil {
				log.Printf("    Partition %d: nil", partKey)
				continue
			}
			log.Printf("    Partition %d: %d messages, next offset: %d",
				partKey, len(partition.Messages), partition.NextOffset)
		}
	}

	// Check metadata
	meta, hasMeta := b.ClusterMetadata.TopicsMetadata.Topics[topicName]
	if !hasMeta {
		log.Printf("\nTopic metadata NOT FOUND in etcd cache")
	} else {
		log.Printf("\nMetadata (from etcd):")
		log.Printf("  Num Partitions: %d", meta.NumPartitions)
		log.Printf("  Partition Assignments:")
		for partKey, port := range meta.Partitions {
			owned := "no"
			if port == b.port {
				owned = "YES (THIS BROKER)"
			}
			log.Printf("    Partition %d -> Port %d [owned by this broker: %s]", partKey, port, owned)
		}
	}

	log.Println("========================================")
}

func (b *brokerServer) PrintTopic(topicName string) {
	topic, exists := b.Topics[topicName]
	if !exists {
		log.Printf("Topic %s does not exist", topicName)
		return
	}
	log.Printf("Topic: %s", topic.Name)
	for _, partition := range topic.Partitions {
		if partition == nil {
			continue
		}
		log.Printf("  Partition %d:", partition.Key)
		for i, msg := range partition.Messages {
			log.Printf("    Message %d: %s", i, string(msg))
		}
	}
}
