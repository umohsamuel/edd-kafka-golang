# Fault-Tolerant Event-Driven Kafka Pipelines in Go

This repository contains the source code for building a reliable, event-driven pipeline in Go using Apache Kafka. It demonstrates how to implement tiered retry strategies, delayed reprocessing, and dead letter queues (DLQ) to handle message failures gracefully.

**Read the full tutorial here:** [Building Fault-Tolerant, Event-Driven Kafka Pipelines in Go: Reliable Reprocessing & Dead Letter Queues](https://www.umohsg.com/blog/building-fault-tolerant-event-driven-kafka-pipelines-in-go-reliable-reprocessing-dead-letter-queues-1f6874d4-270c-48e1-9798-324b494c7f0e)

## Architecture

The system consists of a REST API producer and a set of consumer groups working together to process messages. If a message fails to process, it is routed through a series of retry topics with increasing backoff delays before finally landing in a Dead Letter Queue.

- **Main Topic:** `orders_created`
- **Retry Tiers:** `retry_5m`, `retry_20m`, `retry_40m`
- **DLQ:** `orders_dlq`

## Prerequisites

- [Go](https://golang.org/doc/install) 1.20+
- [Docker](https://docs.docker.com/get-docker/) & Docker Compose

## Getting Started

1. **Start the Kafka cluster (KRaft mode):**
   ```bash
   docker compose up -d
   ```

2. **Start the Consumer:**
   In a new terminal, navigate to the consumer directory and start it:
   ```bash
   cd consumer
   go run .
   ```

3. **Start the Producer:**
   In another terminal, navigate to the producer directory and start the REST API:
   ```bash
   cd producer
   go run .
   ```

## Testing

You can trigger an event by sending a POST request to the producer API:

```bash
curl -X POST http://localhost:8080/api/v1/order \
  -H "Content-Type: application/json" \
  -d '{"text": "New order from Lagos"}'
```

Watch the consumer logs to see the message being processed. You can simulate failures by modifying the `processBusinessLogic` function in the consumer code to return an error, which will automatically route the message through the retry tiers and eventually to the DLQ.

## License

MIT
