proto_gen:
	protoc --go_out=./ --go_opt=paths=source_relative \
		--go-grpc_out=./ --go-grpc_opt=paths=source_relative \
		./proto/producer/producer.proto

	# protoc --go_out=./proto/consumer --go_opt=paths=source_relative \
	# 	--go-grpc_out=./proto/consumer --go-grpc_opt=paths=source_relative \
	# 	./proto/consumer/consumer.proto

run_cluster:
	goreman -f etcd/Procfile start

clean_cluster:
	rm -rf etcd/cluster-data

clean_logs:
	rm -rf ./broker/logs

clean_data: clean_logs clean_cluster

clean_proto:
	rm -rf ./proto/producer/producer.pb.go
	rm -rf ./proto/producer/producer_grpc.pb.go
	rm -rf ./proto/consumer/consumer.pb.go
	rm -rf ./proto/consumer/consumer_grpc.pb.go

test_broker:
	go test -count=1 ./broker

test_producer:
	go test -count=5 ./producer

run_tests: test_broker test_producer
