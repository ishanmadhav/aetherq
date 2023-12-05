package main

import (
	"log"
	"time"

	"github.com/ishanmadhav/aetherq/pkg/partition"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	p1, err := partition.NewPartition("test", 0, "localhost:8080", cli)
	if err != nil {
		log.Fatal(err)
	}
	_, err = partition.NewPartition("test", 0, "localhost:8081", cli)
	if err != nil {
		log.Fatal(err)
	}
	_, err = partition.NewPartition("test", 0, "localhost:8082", cli)
	if err != nil {
		log.Fatal(err)
	}

	p1.SetPartitionCoordinationData()
	// p2.SetPartitionCoordinationData()
	// p3.SetPartitionCoordinationData()
	out, err := p1.GetPartitionCoordinationData()
	if err != nil {
		log.Fatal(err)
	}
	log.Println(out)

}
