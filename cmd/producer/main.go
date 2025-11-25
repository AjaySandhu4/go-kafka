package main

import (
	"context"
	"fmt"
	"log"
	"time"

	producerpb "go-kafka/proto/producer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("Starting Kafka Producer...")

	// Connect to broker
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	// Create producer with broker connection
	prod := producerpb.NewProducerServiceClient(conn)
	// Send some test messages
	topic := "test-topic"
	fmt.Printf("Producing messages to topic: %s\n", topic)
	for i := range 5 {
		message := fmt.Sprintf("Message %d from producer to topic: %s", i, topic)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		res, err := prod.PublishMessage(ctx, &producerpb.PublishRequest{Topic: topic, Message: message})
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			fmt.Printf("Sent: %s\n", message+" | Response: "+res.String())
		}
		cancel()
	}

	fmt.Println("Producer finished")
}
