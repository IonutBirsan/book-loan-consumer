# Book Loan Consumer

A Spring Boot microservice that consumes book loan events from Kafka and processes them in real-time.

## Features

- 📥 Kafka consumer - subscribes to `book-loans` topic
- 🔄 Real-time message processing
- 📊 Distributed tracing with OpenTelemetry and Jaeger
- 📝 Structured logging with trace context
- ❤️ Health checks and metrics via Spring Actuator

## Technologies

- Java 21
- Spring Boot 4.0.1
- Apache Kafka
- OpenTelemetry + Jaeger
- Jackson for JSON deserialization
- SLF4J for structured logging

## Prerequisites

- Java 21+
- Maven 3.6+
- Docker (for Kafka and Jaeger)
- Running Kafka broker on localhost:9092

## Getting Started

### 1. Ensure Kafka and Jaeger are running
```bash
docker-compose up -d
```

This should already be running from the producer setup:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Jaeger UI (http://localhost:16686)

### 2. Run the application
```bash
mvn spring-boot:run
```

The service will start on **http://localhost:8081**

### 3. Verify it's consuming

Check the console logs - you should see messages being consumed when the producer publishes them.

## How It Works

1. **Listens** to Kafka topic `book-loans`
2. **Deserializes** JSON messages into `BookLoan` objects
3. **Processes** each message (currently just logs them)
4. **Automatically commits** offsets after successful processing

## Configuration

Key configuration in `application.properties`:
```properties
# Server
server.port=8081

# Kafka Consumer
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=book-loan-consumer-group
spring.kafka.consumer.auto-offset-reset=earliest

# JSON Deserialization
spring.kafka.consumer.properties.spring.json.trusted.packages=*
spring.kafka.consumer.properties.spring.json.use.type.headers=false

# Tracing
spring.application.name=book-loan-consumer
management.tracing.sampling.probability=1.0
management.otlp.tracing.endpoint=http://localhost:4318/v1/traces
```

### Consumer Configuration Explained

| Property | Value | Description |
|----------|-------|-------------|
| `group-id` | `book-loan-consumer-group` | Consumer group for load balancing |
| `auto-offset-reset` | `earliest` | Start from beginning if no offset exists |
| `spring.json.trusted.packages` | `*` | Allow deserialization from any package |
| `spring.json.use.type.headers` | `false` | Ignore type info from producer |

## Observability

### Structured Logging

The consumer uses structured logging to track message processing:

**Log Levels:**
- `INFO` - Message received and processed successfully
- `DEBUG` - Detailed processing information
- `WARN` - Processing issues (non-fatal)
- `ERROR` - Processing failures

**Example logs:**
```
INFO [book-loan-consumer,,] - ========================================
INFO [book-loan-consumer,,] - 📚 Received BookLoan: BookLoan{loanId='L-1027', memberId='M-006', ...}
INFO [book-loan-consumer,,] - ========================================
```

**Note:** Consumer logs currently show empty trace IDs `[book-loan-consumer,,]` because Kafka messages are asynchronous and don't carry trace context from the producer. This is expected behavior for async messaging.

### Monitoring Endpoints

| Endpoint | Description |
|----------|-------------|
| `/actuator/health` | Health check |
| `/actuator/metrics` | Application metrics |
| `/actuator/info` | Application info |

Access at: `http://localhost:8081/actuator/health`

### Distributed Tracing

While the consumer creates its own traces (separate from producer), you can still view them in Jaeger:

1. Open http://localhost:16686
2. Select service: `book-loan-consumer` (if it appears)
3. View consumer-specific traces

**Note:** For async messaging like Kafka, producer and consumer traces are typically independent rather than part of one continuous trace.

## Testing

### 1. Start the consumer
```bash
mvn spring-boot:run
```

### 2. Publish messages from the producer
```bash
curl -X POST http://localhost:8080/api/loans/publish
```

### 3. Watch consumer logs
You should see logs like:
```
INFO [book-loan-consumer,,] - 📚 RECEIVED: BookLoan{loanId='L-1001', memberId='M-001', ...}
INFO [book-loan-consumer,,] - 📚 RECEIVED: BookLoan{loanId='L-1002', memberId='M-002', ...}
```

## Project Structure
```
book-loan-consumer/
├── src/
│   ├── main/
│   │   ├── java/com/bvd/consumer/
│   │   │   ├── kafka/           # Kafka listeners
│   │   │   ├── model/           # Domain models (BookLoan)
│   │   │   └── BookLoanConsumerApplication.java
│   │   └── resources/
│   │       └── application.properties
└── pom.xml
```

## Message Format

The consumer expects JSON messages with this structure:
```json
{
  "loanId": "L-1001",
  "memberId": "M-001",
  "loanDate": "2024-06-01",
  "bookTitle": "1984",
  "genre": "Dystopian",
  "author": "George Orwell",
  "daysLoaned": 14
}
```

## Consumer Group Behavior

- **Group ID**: `book-loan-consumer-group`
- **Single consumer**: Receives all messages from the topic
- **Multiple consumers**: Messages are load-balanced across consumers in the same group
- **Offset management**: Automatically tracks which messages have been processed


## Related Services

- **Producer**: [book-loan-service](../book-loan-service) - Publishes messages to Kafka

