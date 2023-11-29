package storage

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
	"time"
)

type CommitLog struct {
	mu         sync.RWMutex
	Logs       []Log
	nextOffset int64
}

type Log struct {
	Offset int64
	Data   []byte
}

type CommitLogData struct {
	Logs       []Log
	NextOffset int64
}

func NewCommitLog() *CommitLog {
	return &CommitLog{
		Logs:       []Log{},
		nextOffset: 0,
	}
}

func (c *CommitLog) GetNextOffset() int64 {
	return c.nextOffset
}

func (c *CommitLog) Append(data []byte) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	offset := c.nextOffset
	c.Logs = append(c.Logs, Log{
		Offset: offset,
		Data:   data,
	})
	c.nextOffset++
	return offset
}

func (c *CommitLog) Read(offset int64) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if offset >= c.nextOffset || offset < 0 {
		return nil
	}
	return c.Logs[offset].Data
}

// Saving CommitLog to Disk
func (c *CommitLog) SaveToFile() error {
	commitLogData := CommitLogData{
		Logs:       c.Logs,
		NextOffset: c.nextOffset,
	}
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(commitLogData)
	if err != nil {
		return err
	}
	fileName := "test"
	err = os.WriteFile(fileName, buf.Bytes(), 0644)
	if err != nil {
		return err
	}
	return nil
}

func LoadCommitLogFromFile() (*CommitLog, error) {
	fileName := "test"
	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	var commitLogData CommitLogData
	decoder := gob.NewDecoder(bytes.NewReader(data))
	err = decoder.Decode(&commitLogData)
	if err != nil {
		return nil, err
	}
	return &CommitLog{
		Logs:       commitLogData.Logs,
		nextOffset: commitLogData.NextOffset,
	}, nil
}

func (c *CommitLog) PersistToDiskLoop() {
	for {
		//Saving Logic
		time.Sleep(5 * time.Second)
	}
}
