# Building Fault-Tolerant, Event-Driven Kafka Pipelines in Go: Reliable Reprocessing & DLQs


> description

## Prerequisites

What do you need to follow along?

1. Working knowledge of Golang.
2. Joy in the Lord!

What is Event Driven Architecture (EDA) ?

An Event Driven Architecture (EDA) is a design approach where services communicate by producing and responding to events. This design system allows for services to be decoupled, allowing them to operate independently while still communicating between each other.

key definitions, before proceeding, in order to speak the language of EDA, we have to first learn its terms (that sounded way cooler in my head)

Events, what are they?
An event is a record of something that has happened in a system, typically representing a state change or a significant action.

An example of an event could be 
1. a user signing up for a service
2. a user placing an order in your system

an event contains data(payload) describing the event. 

key components of an event driven architecture: 
1. event producers: they are the sources of events, they generate and publish events, these events could be e.g signup event, order placed event, etc. Producers generate events and transmit them to the rest of the system. they do not know who is listening for or handling the events. 
   
2. event brokers: they sit between producers and consumers, decoupling them so neither needs a direct connection to the other. brokers receive event messages, maintain their chronological order, make them available for consumption and route them to the right consumers.
   
3. event consumers: they handle the processing tasks. They listen on event channels and react when an event they are subscribed to is published, then they process the event, which can include making API calls, updating a database, triggering other events, or logging information. 

so from this, the flow of an EDA is:
- When an app or service performs an action that another part of the system needs to know about, it publishes a new event to the broker. 
- The broker recieves the event & maintains the event’s order relative to others in the stream, then routes it to the appopriate consumer. 
- An event consumer then ingests the message in real time or at a later relevant instance and processes it to trigger another action.

now that we have briefly discussed EDA and understand the underlying architecture, lets understand how we can build event driven pipelines with kafka in golang

Apache Kafka, defined

Apache Kafka is an open-source, distributed, event-streaming platform used to publish, store, process and consume real-time data streams. It is based on a publish-subscribe messaging model and is designed to support fault-tolerant, scalable, high-throughput and low-latency data pipelines, event-driven applications and stream-processing systems.

Key Apache Kafka components

1. Messages: A unit of data comprised of two parts: a key and a value. The key is commonly used for data about the message and the value is the body of the message.

2. Producers: Applications that publish events or messages to Kafka topics. A producer can publish to one or more topics and can optionally choose the partition that stores the data.

3. Topics: A topic is a named channel or routing mechanism through which events are published and delivered to consumers. Applications write to and read from topics. 

4. Partitions: Subdivisions of a topic that distribute records across multiple brokers. Partitions let Kafka scale horizontally while preserving record order within each partition.

5. Brokers: Kafka servers that store topic partitions, handle client requests and replicate data across the cluster for fault tolerance.

6. Consumers: Applications that subscribe to topics and read records from partitions.

7. Consumer groups: Allow multiple consumers to work together to process records from a topic.

8. Offsets: Unique sequential identifiers assigned to records within a partition. Consumers use offsets to track their reading position and replay data when needed.

Together, these components allow organizations to reliably stream, process and distribute high-volume data in real time across complex, distributed environments.

Creating our Producer

ConnectProducer

```
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

our pushlisher function which produces a message to our broker

```
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

we will use a basic rest api post request to handle the user request comming in that will then trigger us to use our producer to publish an event to our broker  


```
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

now our createorderHandler which will produce the event on success
```
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


next our consumer
 first lets define our topcs and list our groups 
 
 ```
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


next lets create our consumer groups 

```
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

now our consumer group handler methods

```
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
```

next the retry group hander nethods

```
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

next a dummy function to simulate processing busness logic that we will use to test, success and failed cases 
```
func processBusinessLogic(data []byte) error {
	// return nil
	return fmt.Errorf("forced simulation error")
}
```

puting it all together in our main

```
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
```


Run the Application
We can run the application by running:

we have to first run the compose, then cd into prodcuer and consumer and run them, 


Testing with cURL


Demo
Here is a video showing the graceful shutdown in action:

Conclusion



(If you want to see the full codebase, check the repository. Play around with it and lmk what you think)


References
https://www.uber.com/us/en/blog/reliable-reprocessing/
https://developer.confluent.io/confluent-tutorials/kafka-on-docker/
https://www.tencentcloud.com/document/product/597/60360
https://www.confluent.io/learn/kafka-dead-letter-queue/#overview
https://www.ibm.com/think/topics/apache-kafka
https://www.ibm.com/think/topics/event-driven-architecture
https://kafka.apache.org/43/getting-started/introduction/