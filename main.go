package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	http.HandleFunc("/", getRoot)

	err := http.ListenAndServe(":3333", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}

// References:
// https://docs.confluent.io/kafka-clients/go/current/overview.html
// https://www.digitalocean.com/community/tutorials/how-to-make-an-http-server-in-go

func getRoot(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	headers, err := json.Marshal(r.Header)
	if err != nil {
		io.WriteString(w, "Failed to generate random identifier")
		return
	}
	var body string
	err = json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		w.WriteHeader(422)
		w.Write([]byte("Malformed JSON body"))
		return
	}

	ident, err := GenerateRandomString(64)
	if err != nil {
		io.WriteString(w, "Failed to generate random identifier")
		return
	}
	kafkaPayload, err := json.Marshal(Payload{ident, body, string(headers), r.Method, r.URL.Path})
	if err != nil {
		io.WriteString(w, "Failed to forward data")
		return
	}

	// TODO: Move Kafka content to another module and reuse clients
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_HOSTS_PORTS"),
		//"client.id":         socket.gethostname(),
		"acks": "all"})
	if err != nil {
		io.WriteString(w, "Failed to create producer")
		return
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "host1:9092,host2:9092",
		"group.id":          "foo",
		"auto.offset.reset": "smallest"})
	if err != nil {
		io.WriteString(w, "Cannot Set Up Data Retrieval")
		return
	}
	// avoid racing condition
	var idents []string
	idents[0] = ident
	err = consumer.SubscribeTopics(idents, nil)
	if err != nil {
		io.WriteString(w, "Cannot Retrieve Data")
		return
	}
	// set this to only pick up new messages starting now

	// Publish to Kafka
	delivery_chan := make(chan kafka.Event, 10000)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &path, Partition: kafka.PartitionAny},
		Value:          []byte(kafkaPayload)},
		delivery_chan,
	)
	if err != nil {
		io.WriteString(w, "Cannot Forward Data for Processing")
		return
	}

	// Consume from Kafka
	ev := consumer.Poll(1000) // time out after 1 second - this is for high throughput
	switch e := ev.(type) {
	case *kafka.Message:
		io.WriteString(w, string(e.Value)) // Respond with content from the Message
	case kafka.Error:
		fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
	default:
		fmt.Printf("Ignored %v\n", e)
	}

	consumer.Close()
}

type Payload struct {
	Id      string
	Body    string
	Headers string
	Verb    string
	Path    string
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
