package main

import (
	"go-kafka/broker"
	"go-kafka/producer"
)

func main() {
	prod := producer.Producer{}
	prod.StartProducer()
	defer prod.ShutdownProducer()

	b := broker.NewBrokerServer()
	b.StartBroker()
}
