package broker

import (
	"errors"
	"log"
	"os"
	"strconv"
)

type SegmentFile struct {
	raw           *os.File
	humanReadable *os.File
}

func OpenSegmentFile(brokerPort Port, topicName string, partitionKey PartitionKey, segmentID int, replicateHumanReadable bool) (*SegmentFile, error) {
	rawSegmentFile, err := openSegmentFile(brokerPort, topicName, partitionKey, segmentID, false)
	if err != nil {
		return nil, err
	}
	var humanReadableFile *os.File = nil
	if replicateHumanReadable {
		humanReadableFile, err = openSegmentFile(brokerPort, topicName, partitionKey, segmentID, true)
		if err != nil {
			return nil, err
		}
	}
	return &SegmentFile{raw: rawSegmentFile, humanReadable: humanReadableFile}, nil
}

func (file *SegmentFile) Close() error {
	if err := file.raw.Close(); err != nil {
		return err
	}
	if file.humanReadable != nil {
		if err := file.humanReadable.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (file *SegmentFile) Write(msg Message) error {
	if _, err := file.raw.Write(msg.Data); err != nil {
		return err
	}
	if file.humanReadable != nil {
		if _, err := file.humanReadable.WriteString("Offset " + strconv.Itoa(msg.Offset) + ": " + string(msg.Data) + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func openSegmentFile(brokerPort Port, topicName string, partitionKey PartitionKey, segmentID int, humanReadable bool) (*os.File, error) {
	var rootDir string
	if humanReadable {
		rootDir = "readable_logs"
	} else {
		rootDir = "logs"
	}
	dirPath := "broker/broker_logs/" + rootDir + "/broker_" + strconv.Itoa(int(brokerPort)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(partitionKey))

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
