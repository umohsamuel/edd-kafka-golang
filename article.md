# Building Fault-Tolerant, Event-Driven Kafka Pipelines in Go: Reliable Reprocessing & Dead Letter Queues

> A practical guide to building reliable event-driven systems in Go using Apache Kafka. Learn how to implement tiered retry strategies with delayed reprocessing, route permanently failed messages to dead letter queues in Golang with Sarama.

## Prerequisites

What do you need to follow along?

1. Working knowledge of Golang.
2. Go & Docker installed on your PC.

## What is an Event-Driven Architecture?

An Event-Driven Architecture (EDA) is a design approach where services communicate by producing and responding to events. Each service operates independently, producing or reacting to events as they happen.

### What are Events?

An event is a record of something that has happened in a system, typically representing a state change or a significant action. An event contains data (payload) describing what happened. An example of an event could be:

1. A user signing up for a service.
2. A user placing an order in your system.

### Components of an Event-Driven Architecture

To understand how events flow through a system, we need to know three key players:

**Event Producers**: They are the sources of events. They generate and publish events like signup events, order placed events, etc. Producers generate events and transmit them to the rest of the system. They do not know who is listening for or handling the events.

**Event Brokers**: They sit between producers and consumers, decoupling them so neither needs a direct connection to the other. Brokers receive event messages, maintain their chronological order, make them available for consumption, and route them to the right consumers. [Apache Kafka](https://kafka.apache.org/) is an example of an event broker, and it's the one we'll use throughout this guide.

**Event Consumers**: They handle the processing tasks. They listen on event channels and react when an event they are subscribed to is published, then they process the event, which can include making API calls, updating a database, triggering other events, or logging information.

### The Complete Flow

With those three pieces in place, the flow of an EDA looks like this:

- When an app or service performs an action that another part of the system needs to know about, it publishes a new event to the broker.
- The broker receives the event, maintains the event's order relative to others in the stream, and then routes it to the appropriate consumer.
- An event consumer then ingests the message in real time or at a later relevant instance and processes it to trigger another action.

## Apache Kafka, Defined

Apache Kafka is an open-source, distributed, event-streaming platform used to publish, store, process, and consume real-time data streams. It is based on a publish-subscribe messaging model and is designed to support fault-tolerant, scalable, high-throughput, and low-latency data pipelines, event-driven applications, and stream-processing systems.

### Key Apache Kafka Components

1. **Messages**: A unit of data comprised of two parts: a key and a value. The value is the actual content being sent, while the key identifies what the message relates to.

2. **Producers**: Applications that publish events or messages to Kafka topics. A producer can publish to one or more topics and can optionally choose the partition that stores the data.

3. **Topics**: A topic is a named channel or routing mechanism through which events are published and delivered to consumers. Applications write to and read from topics.

4. **Partitions**: Subdivisions of a topic that distribute records across multiple brokers. Partitions let Kafka scale horizontally while preserving record order within each partition.

5. **Brokers**: Kafka servers that store topic partitions, handle client requests, and replicate data across the cluster for fault tolerance.

6. **Consumers**: Applications that subscribe to topics and read records from partitions.

7. **Consumer Groups**: Allow multiple consumers to work together to process records from a topic.

8. **Offsets**: Unique sequential identifiers assigned to records within a partition. Consumers use offsets to track their reading position and replay data when needed.

Together, these components allow organizations to reliably stream, process, and distribute high-volume data in real time across complex, distributed environments.

## Setting Up Kafka Locally with Docker

Before we write any Go code, we need a running Kafka cluster. We will use Docker Compose to spin up Kafka locally:

```yaml
services:
  broker:
    image: apache/kafka:latest
    hostname: broker
    container_name: broker
    ports:
      - 9092:9092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      CLUSTER_ID: 59Uj60pTdxFKv5bUMDYZFA
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:29093
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

Since the introduction of [KRaft](https://docs.confluent.io/platform/current/kafka-metadata/kraft.html), Kafka no longer requires [Apache ZooKeeper®](https://zookeeper.apache.org/) for managing cluster metadata, it uses Kafka itself instead. One advantage of the new KRaft mode is that you can have a single Kafka broker to handle both metadata and client requests in a small, local development environment. The docker-compose.yml file for this tutorial uses this approach, leading to faster startup times and simpler configuration. Note that, in a production setting, you'll have distinct Kafka brokers for handling requests and operating as a cluster controller.

Start the cluster with:

```bash
docker compose up -d
```

## Creating the Producer

Now that Kafka is running, let's build the producer side of our pipeline. The producer is responsible for receiving an order request via a REST API and publishing that event to our Kafka broker.

> We'll be using Sarama throughout this guide, Sarama is an MIT-licensed Go client library for Apache Kafka.

### Connecting to Kafka

First, we need a function that establishes a connection to our Kafka broker and returns a synchronous producer:

```go
func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	sarama.MaxRequestSize = 100 * 1024 * 1024
	sarama.MaxResponseSize = 100 * 1024 * 1024
	config.Producer.MaxMessageBytes = 100 * 1024 * 1024

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Producer.Retry.Max = 3
	config.Net.MaxOpenRequests = 1

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
```

A few things worth noting here. `RequiredAcks = sarama.WaitForAll` means the producer will wait for all in-sync replicas to acknowledge the message before considering it successfully sent. We also set `Idempotent = true` to prevent duplicate messages in case of retries, and `MaxOpenRequests = 1` is required for idempotent producers to guarantee ordering.

### Publishing Messages

Next, our publisher function which produces a message to our broker:

```go
func PushOrderToQueue(producer sarama.SyncProducer, topic string, message []byte) error {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
	return nil
}
```

### The REST API Entry Point

We will use a basic REST API POST request to handle incoming user requests that will then trigger our producer to publish an event to the broker:

```go
func main() {

	brokersUrl := []string{"localhost:9092"}

	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer producer.Close()

	router := gin.Default()

	api := router.Group("/api/v1")

	api.POST("/order", createOrderHandler(producer))

	router.Run()
}
```

### The Order Handler

Now our `createOrderHandler`, which will produce the event on success:

```go
type Order struct {
	Text string `form:"text" json:"text"`
}

func createOrderHandler(producer sarama.SyncProducer) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := "orders"
		order := new(Order)

		if err := c.ShouldBindJSON(&order); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		orderInBytes, err := json.Marshal(order)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		err = PushOrderToQueue(producer, topic, orderInBytes)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "Order pushed successfully",
			"order":   order,
		})

	}
}
```

### Complete Producer Code

Here is the full producer code with everything wired together:

```go
// producer/producer.go

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

type Order struct {
	Text string `form:"text" json:"text"`
}

func main() {

	brokersUrl := []string{"localhost:9092"}

	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer producer.Close()

	router := gin.Default()

	api := router.Group("/api/v1")

	api.POST("/order", createOrderHandler(producer))

	router.Run()
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	sarama.MaxRequestSize = 100 * 1024 * 1024
	sarama.MaxResponseSize = 100 * 1024 * 1024
	config.Producer.MaxMessageBytes = 100 * 1024 * 1024

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Producer.Retry.Max = 3
	config.Net.MaxOpenRequests = 1

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil

}

func PushOrderToQueue(producer sarama.SyncProducer, topic string, message []byte) error {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
	return nil
}

func createOrderHandler(producer sarama.SyncProducer) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := "orders"
		order := new(Order)

		if err := c.ShouldBindJSON(&order); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		orderInBytes, err := json.Marshal(order)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		err = PushOrderToQueue(producer, topic, orderInBytes)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "Order pushed successfully",
			"order":   order,
		})

	}
}
```

## Creating the Consumer

Now for the other side of the pipeline. The consumer listens for events on topics, processes them, and handles failures by routing messages through a tiered reprocessing system.

### Defining Topics and Consumer Groups

First, let's define our topics and consumer groups. We have a main topic for fresh orders, three retry topics with increasing delays, and a dead letter queue (DLQ) for messages that have exhausted all retry attempts:

```go
type Topics string

const (
	ORDERS_CREATED Topics = "orders_created"
	RETRY_5M       Topics = "retry_5m"
	RETRY_20M      Topics = "retry_20m"
	RETRY_40M      Topics = "retry_40m"
	ORDERS_DLQ     Topics = "orders_dlq"
)

func (t Topics) String() string {
	return string(t)
}

type ConsumerGroup string

const (
	ORDERS_MAIN_GROUP      ConsumerGroup = "orders_main_group"
	ORDERS_5M_RETRY_GROUP  ConsumerGroup = "orders_retry_5m_group"
	ORDERS_20M_RETRY_GROUP ConsumerGroup = "orders_retry_20m_group"
	ORDERS_40M_RETRY_GROUP ConsumerGroup = "orders_retry_40m_group"
)

func (t ConsumerGroup) String() string {
	return string(t)
}
```

### Connecting Consumer Groups

Next, let's create our consumer group and producer connections. The consumer side also needs a producer because when a message fails processing, it needs to publish the message to the next retry topic:

```go
func ConnectConsumerGroup(brokersUrl []string, groupID string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	return sarama.NewConsumerGroup(brokersUrl, groupID, config)
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	return sarama.NewSyncProducer(brokersUrl, config)
}
```

Pay attention to `config.Consumer.Offsets.AutoCommit.Enable = false`. By default, Sarama will automatically commit offsets in the background on a timer, which means it could mark a message as "done" before your code has actually finished processing it. If your app crashes between the auto-commit and the actual processing, that message is lost. By disabling auto-commit, we take full control of when offsets get committed. This means we have to manually call `session.MarkMessage` and `session.Commit` in our handler to commit an offset.

### The Main Consumer Group Handler

This is where the processing logic lives. When a message is consumed, we will attempt to process it. If processing fails, we route the message to the next retry tier:

```go
type ConsumerGroupHandler struct {
	producer sarama.SyncProducer
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		fmt.Printf("Received message: key=%s, value=%s, partition=%d, offset=%d\n", string(msg.Key), string(msg.Value), msg.Partition, msg.Offset)

		err := processBusinessLogic(msg.Value)

		if err != nil {

			log.Printf("Processing failed on topic %s. Routing to next stage...", msg.Topic)

			errRoute := h.routeToNextStage(msg)
			if errRoute != nil {
				log.Printf("CRITICAL: Failed to route message: %v", errRoute)
				return errRoute
			}
		}

		session.MarkMessage(msg, "")
		session.Commit()
	}

	return nil
}
```

Notice how after we are done with a message, whether it was processed successfully or routed to the next retry stage, we call `session.MarkMessage(msg, "")` followed by `session.Commit()`. This is the manual offset commit we set up earlier when we disabled auto-commit. `MarkMessage` tells Sarama "I am done with this message" and `Commit` actually persists that offset to Kafka. This way, if the consumer crashes mid-processing, Kafka knows to redeliver the message because the offset was never committed.

### Routing Failed Messages

The `routeToNextStage` method is the heart of our retry strategy. It reads the `x-retry-count` header from the message to determine how many times this message has already been retried, then routes it to the next topic accordingly. After three failed attempts, the message lands in the DLQ:

```go
func (h *ConsumerGroupHandler) routeToNextStage(msg *sarama.ConsumerMessage) error {
	retryCount := 0
	for _, header := range msg.Headers {
		if string(header.Key) == "x-retry-count" {
			retryCount, _ = strconv.Atoi(string(header.Value))
		}
	}

	var nextTopic string
	switch retryCount {
	case 0:
		nextTopic = RETRY_5M.String()
	case 1:
		nextTopic = RETRY_20M.String()
	case 2:
		nextTopic = RETRY_40M.String()
	default:
		nextTopic = ORDERS_DLQ.String()
	}

	now := time.Now()
	headers := []sarama.RecordHeader{
		{Key: []byte("x-retry-count"), Value: []byte(strconv.Itoa(retryCount + 1))},
		{Key: []byte("x-failed-at"), Value: []byte(now.Format(time.RFC3339))},
		{Key: []byte("x-original-topic"), Value: []byte(msg.Topic)},
	}

	retryMsg := &sarama.ProducerMessage{
		Topic:   nextTopic,
		Key:     sarama.ByteEncoder(msg.Key),
		Value:   sarama.ByteEncoder(msg.Value),
		Headers: headers,
	}

	log.Printf("Routing message to topic: %s (Attempt #%d)", nextTopic, retryCount+1)
	_, _, err := h.producer.SendMessage(retryMsg)
	return err
}
```

We attach three headers to every retried message: `x-retry-count` so the next handler knows which attempt this is, `x-failed-at` so the retry handler can calculate the correct delay, and `x-original-topic` for traceability. The switch statement maps the retry count to the appropriate next topic, escalating through 5 minute, 20 minute, and 40 minute delays before finally landing in the DLQ.

### The Retry Consumer Group Handler

The retry handler works similarly to the main handler, but with one key difference: it enforces a delay before reprocessing the message. It reads the `x-failed-at` header to determine when the message originally failed, then calculates how much time has already passed. If the message arrived before the required delay has elapsed, it sleeps for the remaining duration. If processing fails again, it escalates to the next retry tier:

```go
type RetryConsumerGroupHandler struct {
	producer      sarama.SyncProducer
	delayDuration time.Duration
}

func (h *RetryConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *RetryConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *RetryConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {

		var failedAt time.Time
		for _, header := range msg.Headers {
			if string(header.Key) == "x-failed-at" {
				failedAt, _ = time.Parse(time.RFC3339, string(header.Value))
			}
		}

		timeElapsed := time.Since(failedAt)
		if timeElapsed < h.delayDuration {
			remainingDelay := h.delayDuration - timeElapsed
			log.Printf("[%s] Message arrived early. Sleeping for %v...", msg.Topic, remainingDelay)
			time.Sleep(remainingDelay)
		}

		err := processBusinessLogic(msg.Value)
		if err != nil {
			log.Printf("[%s] Retry failed. Escalating to next tier.", msg.Topic)

			mainHandler := &ConsumerGroupHandler{producer: h.producer}
			errRoute := mainHandler.routeToNextStage(msg)
			if errRoute != nil {
				return errRoute
			}
		}

		session.MarkMessage(msg, "")
		session.Commit()
	}
	return nil
}
```

### Simulating Business Logic

For testing purposes, here is a dummy function that simulates our business logic. You can toggle between returning `nil` (success) and returning an error (failure) to test both paths:

```go
func processBusinessLogic(data []byte) error {
	// return nil
	return fmt.Errorf("forced simulation error")
}
```

### Complete Consumer Code

Here is the full consumer code with everything wired together. We create a shared producer (for routing failed messages), spin up consumer groups for the main topic and each retry tier, and run them all concurrently in separate goroutines:

```go
// consumer/consumer.go

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Topics string

const (
	ORDERS_CREATED Topics = "orders_created"
	RETRY_5M       Topics = "retry_5m"
	RETRY_20M      Topics = "retry_20m"
	RETRY_40M      Topics = "retry_40m"
	ORDERS_DLQ     Topics = "orders_dlq"
)

func (t Topics) String() string {
	return string(t)
}

type ConsumerGroup string

const (
	ORDERS_MAIN_GROUP      ConsumerGroup = "orders_main_group"
	ORDERS_5M_RETRY_GROUP  ConsumerGroup = "orders_retry_5m_group"
	ORDERS_20M_RETRY_GROUP ConsumerGroup = "orders_retry_20m_group"
	ORDERS_40M_RETRY_GROUP ConsumerGroup = "orders_retry_40m_group"
)

func (t ConsumerGroup) String() string {
	return string(t)
}

func main() {
	brokersUrl := []string{"localhost:9092"}

	sharedProducer, err := ConnectProducer(brokersUrl)
	if err != nil {
		log.Fatalf("Failed to start DLQ Producer: %v", err)
	}
	defer sharedProducer.Close()

	mainGroup, err := ConnectConsumerGroup(brokersUrl, ORDERS_MAIN_GROUP.String())
	if err != nil {
		log.Fatalf("Failed to start Main Consumer Group: %v", err)
	}
	defer mainGroup.Close()

	retryGroup5m, err := ConnectConsumerGroup(brokersUrl, ORDERS_5M_RETRY_GROUP.String())
	if err != nil {
		log.Fatalf("Failed to start 5m Retry Group: %v", err)
	}
	defer retryGroup5m.Close()

	retryGroup20m, err := ConnectConsumerGroup(brokersUrl, ORDERS_20M_RETRY_GROUP.String())
	if err != nil {
		log.Fatalf("Failed to start 20m Retry Group: %v", err)
	}
	defer retryGroup20m.Close()

	retryGroup40m, err := ConnectConsumerGroup(brokersUrl, ORDERS_40M_RETRY_GROUP.String())
	if err != nil {
		log.Fatalf("Failed to start 40m Retry Group: %v", err)
	}
	defer retryGroup40m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Consumers have entered the fray...")

	go func() {
		handler := &ConsumerGroupHandler{
			producer: sharedProducer,
		}

		for {
			if err := mainGroup.Consume(ctx, []string{ORDERS_CREATED.String()}, handler); err != nil {
				log.Printf("Error from main consumer group: %v", err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	go func() {
		handler := &RetryConsumerGroupHandler{
			producer:      sharedProducer,
			delayDuration: 5 * time.Minute,
		}

		for {
			if err := retryGroup5m.Consume(ctx, []string{RETRY_5M.String()}, handler); err != nil {
				log.Printf("Error from %s group: %v", RETRY_5M.String(), err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	go func() {
		handler := &RetryConsumerGroupHandler{
			producer:      sharedProducer,
			delayDuration: 20 * time.Minute,
		}

		for {
			if err := retryGroup20m.Consume(ctx, []string{RETRY_20M.String()}, handler); err != nil {
				log.Printf("Error from %s group: %v", RETRY_20M.String(), err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	go func() {
		handler := &RetryConsumerGroupHandler{
			producer:      sharedProducer,
			delayDuration: 40 * time.Minute,
		}

		for {
			if err := retryGroup40m.Consume(ctx, []string{RETRY_40M.String()}, handler); err != nil {
				log.Printf("Error from %s group: %v", RETRY_40M.String(), err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-sigchan
	fmt.Println("Interruption detected, shutting down all groups gracefully...")
	cancel()

}

func ConnectConsumerGroup(brokersUrl []string, groupID string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	return sarama.NewConsumerGroup(brokersUrl, groupID, config)
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	return sarama.NewSyncProducer(brokersUrl, config)
}

func processBusinessLogic(data []byte) error {
	// return nil
	return fmt.Errorf("forced simulation error")
}

type ConsumerGroupHandler struct {
	producer sarama.SyncProducer
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		fmt.Printf("Received message: key=%s, value=%s, partition=%d, offset=%d\n", string(msg.Key), string(msg.Value), msg.Partition, msg.Offset)

		err := processBusinessLogic(msg.Value)

		if err != nil {

			log.Printf("Processing failed on topic %s. Routing to next stage...", msg.Topic)

			errRoute := h.routeToNextStage(msg)
			if errRoute != nil {
				log.Printf("CRITICAL: Failed to route message: %v", errRoute)
				return errRoute
			}
		}

		session.MarkMessage(msg, "")
		session.Commit()
	}

	return nil
}

func (h *ConsumerGroupHandler) routeToNextStage(msg *sarama.ConsumerMessage) error {
	retryCount := 0
	for _, header := range msg.Headers {
		if string(header.Key) == "x-retry-count" {
			retryCount, _ = strconv.Atoi(string(header.Value))
		}
	}

	var nextTopic string
	switch retryCount {
	case 0:
		nextTopic = RETRY_5M.String()
	case 1:
		nextTopic = RETRY_20M.String()
	case 2:
		nextTopic = RETRY_40M.String()
	default:
		nextTopic = ORDERS_DLQ.String()
	}

	now := time.Now()
	headers := []sarama.RecordHeader{
		{Key: []byte("x-retry-count"), Value: []byte(strconv.Itoa(retryCount + 1))},
		{Key: []byte("x-failed-at"), Value: []byte(now.Format(time.RFC3339))},
		{Key: []byte("x-original-topic"), Value: []byte(msg.Topic)},
	}

	retryMsg := &sarama.ProducerMessage{
		Topic:   nextTopic,
		Key:     sarama.ByteEncoder(msg.Key),
		Value:   sarama.ByteEncoder(msg.Value),
		Headers: headers,
	}

	log.Printf("Routing message to topic: %s (Attempt #%d)", nextTopic, retryCount+1)
	_, _, err := h.producer.SendMessage(retryMsg)
	return err
}

type RetryConsumerGroupHandler struct {
	producer      sarama.SyncProducer
	delayDuration time.Duration
}

func (h *RetryConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *RetryConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *RetryConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {

		var failedAt time.Time
		for _, header := range msg.Headers {
			if string(header.Key) == "x-failed-at" {
				failedAt, _ = time.Parse(time.RFC3339, string(header.Value))
			}
		}

		timeElapsed := time.Since(failedAt)
		if timeElapsed < h.delayDuration {
			remainingDelay := h.delayDuration - timeElapsed
			log.Printf("[%s] Message arrived early. Sleeping for %v...", msg.Topic, remainingDelay)
			time.Sleep(remainingDelay)
		}

		err := processBusinessLogic(msg.Value)
		if err != nil {
			log.Printf("[%s] Retry failed. Escalating to next tier.", msg.Topic)

			mainHandler := &ConsumerGroupHandler{producer: h.producer}
			errRoute := mainHandler.routeToNextStage(msg)
			if errRoute != nil {
				return errRoute
			}
		}

		session.MarkMessage(msg, "")
		session.Commit()
	}
	return nil
}
```

## Running the Application

To run the application, first start the Docker Compose cluster (if you already did this earlier, you can skip it), then run the producer and consumer in separate terminals:

```bash
docker compose up -d
```

```bash
cd producer && go run .
```

```bash
cd consumer && go run .
```

## Testing with cURL

You can test the pipeline by sending a POST request to the producer's API:

```bash
curl -X POST http://localhost:8080/api/v1/order \
  -H "Content-Type: application/json" \
  -d '{"text": "New order from Lagos"}'
```

<!-- To see the full retry flow in action, keep the `processBusinessLogic` function returning the dummy error. Send a request and watch the consumer logs as the message fails, gets routed to the 5 minute retry topic, fails again, moves to the 20 minute retry, then the 40 minute retry, and finally lands in the DLQ.

Once you have seen that, toggle `processBusinessLogic` to return `nil` instead, then restart the consumer service in the terminal, and send another request to confirm the success path works cleanly.

Also notice how when you hit `Ctrl+C`, the consumer shuts down all groups gracefully without dropping any in-flight messages. -->

To see the full retry flow in action, keep the `processBusinessLogic` function returning the dummy error. Send a request and watch the consumer logs as the message fails, gets routed to the 5 minute retry topic, fails again, moves to the 20 minute retry, then the 40 minute retry, and finally lands in the DLQ.

Once you have confirmed that flow, change `processBusinessLogic` to return `nil`, restart the consumer service, and send another request to confirm the success path works cleanly, with the message being marked and committed on the first pass.

Lastly, While the consumer service is running, try hitting `Ctrl+C` as well. You should see each consumer group shut down gracefully in turn, without dropping any in-flight messages.

## Demo

Here is a demo of how all these concepts come together:

<video src="https://res.cloudinary.com/db6nohcui/video/upload/v1786899332/c79e8f83-1540-4b9b-8262-d7b6798bea5a_edd-golang-kafka_tnlpk0.mp4" controls playsinline width="100%"></video>

## Conclusion

In this guide, we built a fault-tolerant event-driven pipeline using Apache Kafka and Go. The key takeaways are:

1. **Event-Driven Architecture** decouples your services, letting producers and consumers operate independently through a message broker.
2. **Tiered retry topics & reprocessing** with increasing delays (5m, 20m, 40m) give transient failures time to resolve before reprocessing, instead of hammering the same operation repeatedly.
3. **Dead letter queues** catch messages that have exhausted all retry attempts, so they can be inspected and handled manually without blocking the pipeline.
4. **Consumer groups** let you scale consumption horizontally while Kafka handles partition assignment and offset tracking.

These patterns apply to any Go service that needs reliable, asynchronous message processing, whether you are processing orders, sending notifications, or syncing data between services.

(If you want to see the full codebase, checkout the [repository](https://github.com/umohsamuel/edd-kafka-golang). Play around with it and lmk what you think)

## References

- [Uber Engineering: Reliable Reprocessing](https://www.uber.com/us/en/blog/reliable-reprocessing/)
- [Confluent: Kafka on Docker](https://developer.confluent.io/confluent-tutorials/kafka-on-docker/)
- [Tencent Cloud: Kafka Dead Letter Queue](https://www.tencentcloud.com/document/product/597/60360)
- [Confluent: Kafka Dead Letter Queue Overview](https://www.confluent.io/learn/kafka-dead-letter-queue/#overview)
- [IBM: Apache Kafka](https://www.ibm.com/think/topics/apache-kafka)
- [IBM: Event-Driven Architecture](https://www.ibm.com/think/topics/event-driven-architecture)
- [Apache Kafka: Getting Started](https://kafka.apache.org/43/getting-started/introduction/)
