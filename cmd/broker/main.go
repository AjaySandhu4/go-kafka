package main

import (
	"go-kafka/broker"
	"log"
	"time"
)

func main() {
	log.Println("In main.go... Starting Broker")
	b := broker.NewBrokerServer()
	b.StartBroker()
	// log.Println("Broker has stopped.")
	time.Sleep(time.Second * 15)
	b.PrintTopic("test-topic")
	b.PrintBroker()
	b.ShutdownBroker()
}
