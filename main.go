package main

import (
	"bytes"
	"context"
	"log"

	myapp "suraj/schema/models" // Import your generated models folder

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()

	// 1. Initialize Kafka Consumer
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("user-consumers"),
		kgo.ConsumeTopics("user"),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer kafkaClient.Close()

	log.Println("Listening for raw Avro messages...")

	for {
		fetches := kafkaClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Fatalf("Fetch error: %v", errs)
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			// 2. Pass the raw bytes directly to gogen-avro's reader
			// Since there is no Confluent 5-byte header, we read the whole slice
			reader := bytes.NewReader(record.Value)

			user, err := myapp.DeserializeUser(reader)
			if err != nil {
				log.Printf("Failed to deserialize raw Avro bytes: %v", err)
				continue
			}

			// 3. Access your type-safe data
			log.Printf("Consumed User -> ID: %s, Username: %s", user.ID, user.Username)
		}
	}
}
