package main

import (
	"fmt"
	"go-kafka/producer"
)

var fakeLogs = []string{
	"User login successful",
	"File uploaded to server",
	"Database connection established",
	"Error: Unable to fetch data",
	"User logout successful",
	"New user registered",
	"Password changed successfully",
	"Session expired for user",
	"Data backup completed",
	"Server restarted",
}

func main() {
	prod := producer.Producer{}
	prod.StartProducer()
	defer prod.ShutdownProducer()

	// Test the producer
	prod.CreateTopic("test-topic", 3)
	for _, logMsg := range fakeLogs {
		err := prod.PublishMessage("test-topic", logMsg)
		if err != nil {
			fmt.Printf("Error publishing message: %v\n", err)
		} else {
			fmt.Printf("Published message: %s\n", logMsg)
		}
	}

	fmt.Println("Producer finished")
}
