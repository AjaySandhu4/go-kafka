# go-kafka

Start etcd cluster with "make run_cluster"

# etcd data:

/broker/<PORT> 
    - port serves as ID for broker
    - broker maintains this entry with leases

/topic/<TOPIC>/config/<num_partitions>
    - indicates that <TOPIC> has <num_partitions> partitions
/topic/<TOPIC>/partitions/<PARTITION_ID>/<PORT (broker port)>
    - indicates that broker at <PORT> is hosting partition <PARTITION_ID> of <TOPIC>


# Broker message files:
/broker/<PORT>/<TOPIC>/<PARTITION_KEY>/<SEGMENT-ID (offset)>
