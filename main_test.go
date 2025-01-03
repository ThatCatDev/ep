package ep_test

import (
	"context"
	"fmt"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"testing"
	"time"
)

func WaitForKafka(broker string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		fmt.Println("Waiting for Kafka to be ready")
		select {
		case <-ctx.Done():
			return fmt.Errorf("Kafka did not become ready in time")
		default:
			_, err := kafka2.NewProducer(&kafka2.ConfigMap{"bootstrap.servers": broker})
			if err == nil {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func TestMain(m *testing.M) {
	// start kafka
	// wait for kafka to be ready
	err := WaitForKafka("localhost:9092", 30*time.Second)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("Kafka is ready")
	os.Exit(m.Run())
}
