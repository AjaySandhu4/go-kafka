# go-kafka

Start etcd cluster with "make run_cluster"

# etcd data:

/broker/<PORT> 
    - port serves as ID for broker
    - broker maintains this entry with leases

/controller