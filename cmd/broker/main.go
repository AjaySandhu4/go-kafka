package main

import (
	"go-kafka/broker"
	"log"
)

func main() {
	log.Println("In main.go... Starting Broker")
	broker.StartBroker()
	log.Println("Broker has stopped.")
}
