package broker

import (
	"errors"
	"log"
	"os"
	"strconv"
)

func createSegmentFile(brokerPort Port, topicName string, partitionKey PartitionKey, segmentID int) (*os.File, error) {
	dirPath := "logs/broker_" + strconv.Itoa(int(brokerPort)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(partitionKey))
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return nil, errors.New("Failed to create directories for segment file: " + err.Error())
	}
	filePath := dirPath + "/segment_" + strconv.Itoa(segmentID) + ".log"
	if _, err := os.Stat(filePath); err == nil {
		log.Println("Segment file already exists")
	}
	segmentFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.New("Failed to create/open segment file: " + err.Error())
	}
	return segmentFile, nil
}
