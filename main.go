package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"crypto/rand"
    "encoding/base64"
)

// References:
// https://docs.confluent.io/kafka-clients/go/current/overview.html
// https://www.digitalocean.com/community/tutorials/how-to-make-an-http-server-in-go

func getRoot(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	headers := json.Marshal(r.Header)
	body := r.Body

	ident = GenerateRandomString(64)
	kafkaPayload = json.Marshal(Payload{ident, body, headers})
	
	// TODO: Move Kafka content to another module and reuse clients
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_HOSTS_PORTS"),
		"client.id": socket.gethostname(),
		"acks": "all"}) 
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    "host1:9092,host2:9092",
		"group.id":             "foo",
		"auto.offset.reset":    "smallest"})

	// avoid racing condition
	err = consumer.SubscribeTopics(ident, nil)
	// set this to only pick up new messages starting now
	
	// Publish to Kafka
	delivery_chan := make(chan kafka.Event, 10000)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: path, Partition: kafka.PartitionAny},
		Value: []byte(kafkaPayload)},
		delivery_chan,
	)

	// Consume from Kafka	
	ev := consumer.Poll(1000) // time out after 1 second - this is for high throughput
	switch e := ev.(type) {
	case *kafka.Message:
		io.WriteString(w, e.Value) // Respond with content from the Message
	case kafka.Error:
		fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		run = false
	default:
		fmt.Printf("Ignored %v\n", e)
	}
	

consumer.Close()
}

func main(){
	http.HandleFunc("/", getRoot)

	err := http.ListenAndServe(":3333", nil)
  	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}

type Payload Struct {
	id string
	body string
	headers string
}

func GenerateRandomBytes(n int) ([]byte, error) {
    b := make([]byte, n)
    _, err := rand.Read(b)
    // Note that err == nil only if we read len(b) bytes.
    if err != nil {
        return nil, err
    }

    return b, nil
}

func GenerateRandomString(s int) (string, error) {
    b, err := GenerateRandomBytes(s)
    return base64.URLEncoding.EncodeToString(b), err
}
