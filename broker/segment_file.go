package broker

import (
	"encoding/binary"
	"errors"
	"go-kafka/cluster"
	"log"
	"os"
	"strconv"
)

type SegmentFile struct {
	raw           *os.File
	humanReadable *os.File
}

func OpenSegmentFile(brokerPort cluster.Port, topicName string, partitionKey cluster.PartitionKey, segmentID int64, replicateHumanReadable bool, readOnly bool) (*SegmentFile, error) {
	if replicateHumanReadable && readOnly {
		return nil, errors.New("Cannot open human readable segment file in read-only mode")
	}

	rawSegmentFile, err := openSegmentFile(brokerPort, topicName, partitionKey, segmentID, false, readOnly)
	if err != nil {
		return nil, err
	}
	var humanReadableFile *os.File = nil
	if replicateHumanReadable {
		humanReadableFile, err = openSegmentFile(brokerPort, topicName, partitionKey, segmentID, true, false)
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
	// Write header
	if err := binary.Write(file.raw, binary.BigEndian, msg.Header.Size); err != nil {
		return err
	}
	if err := binary.Write(file.raw, binary.BigEndian, msg.Header.Offset); err != nil {
		return err
	}
	if err := binary.Write(file.raw, binary.BigEndian, msg.Header.CRC); err != nil {
		return err
	}
	if err := binary.Write(file.raw, binary.BigEndian, msg.Header.DataSize); err != nil {
		return err
	}
	// Write data
	if _, err := file.raw.Write(msg.Data); err != nil {
		return err
	}

	// Write to human readable file if enabled
	if file.humanReadable != nil {
		if _, err := file.humanReadable.WriteString("Offset " + strconv.Itoa(int(msg.Header.Offset)) + ": " + string(msg.Data) + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func (file *SegmentFile) Read() (Message, error) {
	msg := Message{}
	// Read header
	if err := binary.Read(file.raw, binary.BigEndian, &msg.Header.Size); err != nil {
		return msg, err
	}
	if err := binary.Read(file.raw, binary.BigEndian, &msg.Header.Offset); err != nil {
		return msg, err
	}
	if err := binary.Read(file.raw, binary.BigEndian, &msg.Header.CRC); err != nil {
		return msg, err
	}
	if err := binary.Read(file.raw, binary.BigEndian, &msg.Header.DataSize); err != nil {
		return msg, err
	}
	// Read data
	msg.Data = make([]byte, msg.Header.DataSize)
	if _, err := file.raw.Read(msg.Data); err != nil {
		return msg, err
	}
	return msg, nil
}

func (file *SegmentFile) SeekToOffset(offset int64) error {
	_, err := file.raw.Seek(offset, 0)
	return err
}

func openSegmentFile(brokerPort cluster.Port, topicName string, partitionKey cluster.PartitionKey, segmentID int64, humanReadable bool, readOnly bool) (*os.File, error) {
	dirPath, filePath := getSegmentFilePath(brokerPort, topicName, partitionKey, segmentID, humanReadable)

	if readOnly {
		// For read-only mode, just check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return nil, errors.New("Segment file does not exist")
		}
	} else {
		// For write mode, ensure directory exists
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, errors.New("Failed to create directories for segment file: " + err.Error())
		}
		// Log if file already exists (will append to it)
		if _, err := os.Stat(filePath); err == nil {
			log.Println("Segment file already exists")
		}
	}

	// Set appropriate flags based on mode
	var openFlags int
	if readOnly {
		openFlags = os.O_RDONLY
	} else {
		openFlags = os.O_CREATE | os.O_APPEND | os.O_WRONLY
	}

	segmentFile, err := os.OpenFile(filePath, openFlags, 0644)
	if err != nil {
		return nil, errors.New("Failed to create/open segment file: " + err.Error())
	}
	return segmentFile, nil
}

func getSegmentFilePath(brokerPort cluster.Port, topicName string, partitionKey cluster.PartitionKey, segmentID int64, humanReadable bool) (string, string) {
	var rootDir string
	if humanReadable {
		rootDir = "readable_logs"
	} else {
		rootDir = "logs"
	}
	dirPath := "broker/broker_logs/" + rootDir + "/broker_" + strconv.Itoa(int(brokerPort)) + "/topic_" + topicName + "/partition_" + strconv.Itoa(int(partitionKey))
	filePath := dirPath + "/segment_" + strconv.Itoa(int(segmentID)) + ".log"
	return dirPath, filePath
}
