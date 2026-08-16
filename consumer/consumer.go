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
	"github.com/umohsamuel/edd-kafka-golang/pkg/topics"
)

func main() {
	brokersUrl := []string{"localhost:9092"}

	sharedProducer, err := ConnectProducer(brokersUrl)
	if err != nil {
		log.Fatalf("Failed to start DLQ Producer: %v", err)
	}
	defer sharedProducer.Close()

	mainGroup, err := ConnectConsumerGroup(brokersUrl, topics.ORDERS_MAIN_GROUP.String())
	if err != nil {
		log.Fatalf("Failed to start Main Consumer Group: %v", err)
	}
	defer mainGroup.Close()

	retryGroup5m, err := ConnectConsumerGroup(brokersUrl, topics.ORDERS_5M_RETRY_GROUP.String())
	if err != nil {
		log.Fatalf("Failed to start 5m Retry Group: %v", err)
	}
	defer retryGroup5m.Close()

	retryGroup20m, err := ConnectConsumerGroup(brokersUrl, topics.ORDERS_20M_RETRY_GROUP.String())
	if err != nil {
		log.Fatalf("Failed to start 20m Retry Group: %v", err)
	}
	defer retryGroup20m.Close()

	retryGroup40m, err := ConnectConsumerGroup(brokersUrl, topics.ORDERS_40M_RETRY_GROUP.String())
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
			if err := mainGroup.Consume(ctx, []string{topics.ORDERS_CREATED.String()}, handler); err != nil {
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
			if err := retryGroup5m.Consume(ctx, []string{topics.RETRY_5M.String()}, handler); err != nil {
				log.Printf("Error from %s group: %v", topics.RETRY_5M.String(), err)
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
			if err := retryGroup20m.Consume(ctx, []string{topics.RETRY_20M.String()}, handler); err != nil {
				log.Printf("Error from %s group: %v", topics.RETRY_20M.String(), err)
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
			if err := retryGroup40m.Consume(ctx, []string{topics.RETRY_40M.String()}, handler); err != nil {
				log.Printf("Error from %s group: %v", topics.RETRY_40M.String(), err)
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
		nextTopic = topics.RETRY_5M.String()
	case 1:
		nextTopic = topics.RETRY_20M.String()
	case 2:
		nextTopic = topics.RETRY_40M.String()
	default:
		nextTopic = topics.ORDERS_DLQ.String()
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
