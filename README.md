# go-kafka
A simple Kafka-like message broker system implemented in Go, using etcd for coordination.



# Instructions to run:
Start etcd cluster with "make run_cluster"


# etcd data structure:
/broker/<PORT> 
    - port serves as ID for broker
    - broker maintains this entry with leases

/consumer-group/<GROUP_ID>
    - Write this to create a new consumer group
    - Read to check if group exists
/consumer-group/<GROUP_ID>/config
    - Group configuration
/consumer-group/<GROUP_ID>/members/<CONSUMER_ID>
    - Consumer registration (with lease)
/consumer-group/<GROUP_ID>/<TOPIC>/<PARTITION_ID>/assignment
    - Which consumer owns this partition
/consumer-group/<GROUP_ID>/<TOPIC>/<PARTITION_ID>/offset
    - Last committed offset for partition

/topic/<TOPIC>/config/<num_partitions>
    - indicates that <TOPIC> has <num_partitions> partitions
/topic/<TOPIC>/partitions/<PARTITION_ID>/<PORT (broker port)>
    - indicates that broker at <PORT> is hosting partition <PARTITION_ID> of <TOPIC>


# Broker message file structure:
/broker/<PORT>/<TOPIC>/<PARTITION_KEY>/<SEGMENT-ID (offset)>
