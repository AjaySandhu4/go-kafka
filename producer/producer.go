package producer

import (
	"context"
	"log"
	"time"

	producerpb "go-kafka/proto/producer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Producer struct {
	// Add producer fields if necessary
	grpcConn       *grpc.ClientConn
	producerClient producerpb.ProducerServiceClient
}

func TestFunction() {
	log.Println("Test function called")
}

func (p *Producer) StartProducer() {
	log.Println("Starting Producer...")

	// Connect to broker
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	p.grpcConn = conn

	// Create producer with broker connection
	p.producerClient = producerpb.NewProducerServiceClient(conn)
}

func (p *Producer) ShutdownProducer() {
	log.Println("Shutting down Producer...")
	// Cleanup logic would go here
	if p.grpcConn != nil {
		p.grpcConn.Close()
	}

}

func (p *Producer) PublishMessage(topic string, message string) (string, error) {
	log.Printf("Producing message to topic %s: %s", topic, message)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	res, err := p.producerClient.PublishMessage(ctx, &producerpb.PublishRequest{Topic: topic, Message: message})
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return "", err
	}
	log.Printf("Sent: %s | Response: %s", message, res.String())
	return res.String(), nil
}

func (p *Producer) CreateTopic(topic string, numPartitions int) (string, error) {
	log.Printf("Creating topic %s with %d partitions", topic, numPartitions)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	res, err := p.producerClient.CreateTopic(ctx, &producerpb.CreateTopicRequest{Topic: topic, NumPartitions: int32(numPartitions)})
	if err != nil {
		log.Printf("Failed to create topic: %v", err)
		return "", err
	}
	log.Printf("Created topic: %s | Response: %s", topic, res.String())
	return res.String(), nil
}
