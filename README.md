# Book Loan Consumer

A Spring Boot microservice that consumes book loan events from Kafka and processes them in real-time.

## Features

- 📥 Kafka consumer - subscribes to `book-loans` topic
- 🔄 Real-time message processing
- 📊 Distributed tracing with OpenTelemetry and Jaeger
- 📝 Structured logging with trace context (trace_id, span_id)
- ❤️ Health checks and metrics via Spring Actuator

## Technologies

- Java 21
- Spring Boot 4.0.1
- Apache Kafka
- OpenTelemetry Java Agent
- Jaeger for distributed tracing
- Jackson for JSON deserialization
- SLF4J + Logback for structured logging

## Prerequisites

- Java 21+
- Maven 3.6+
- Docker (for Kafka and Jaeger)
- Running Kafka broker on localhost:9092
- OpenTelemetry Java Agent (included in `agent/` directory)

## Getting Started

### 1. Ensure Kafka and Jaeger are running
```bash
docker-compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Jaeger UI (http://localhost:16686)
- Jaeger OTLP endpoint (port 4318)

### 2. Build the application
```bash
mvn clean package
```

### 3. Run with OpenTelemetry Agent

**Using the run script (recommended):**
```bash
./scripts/run-with-tracing.sh
```

**Or manually:**
```bash
java -javaagent:agent/opentelemetry-javaagent.jar \
  -Dotel.service.name=book-loan-consumer \
  -Dotel.traces.exporter=otlp \
  -Dotel.exporter.otlp.endpoint=http://localhost:4318 \
  -Dotel.metrics.exporter=none \
  -Dotel.logs.exporter=none \
  -Dotel.instrumentation.logback-mdc.enabled=true \
  -jar target/book-loan-consumer-0.0.1-SNAPSHOT.jar
```

**For development (Maven):**
```bash
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-javaagent:agent/opentelemetry-javaagent.jar -Dotel.service.name=book-loan-consumer -Dotel.traces.exporter=otlp -Dotel.exporter.otlp.endpoint=http://localhost:4318 -Dotel.metrics.exporter=none -Dotel.logs.exporter=none -Dotel.instrumentation.logback-mdc.enabled=true"
```

The service will start on **http://localhost:8081**

### 4. Verify it's consuming

Check the console logs - you should see messages being consumed with trace context:
```
INFO [book-loan-consumer,1dc6ae8...,abc123...] - 📚 RECEIVED: BookLoan{loanId='L-1001', ...}
```

## How It Works

1. **Listens** to Kafka topic `book-loans`
2. **Deserializes** JSON messages into `BookLoan` objects
3. **Processes** each message (currently just logs them)
4. **Automatically traced** - OpenTelemetry agent instruments Kafka consumers automatically
5. **Logs with context** - Each log includes trace_id and span_id
6. **Automatically commits** offsets after successful processing

## Configuration

### Application Configuration

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

### OpenTelemetry Agent Configuration

The OpenTelemetry Java Agent is configured via JVM arguments:

| Parameter | Value | Description |
|-----------|-------|-------------|
| `-javaagent` | `agent/opentelemetry-javaagent.jar` | Loads the OTel agent |
| `otel.service.name` | `book-loan-consumer` | Identifies this service in traces |
| `otel.traces.exporter` | `otlp` | Export traces via OTLP protocol |
| `otel.exporter.otlp.endpoint` | `http://localhost:4318` | Jaeger OTLP endpoint |
| `otel.metrics.exporter` | `none` | Disable metrics export |
| `otel.logs.exporter` | `none` | Disable log export (we only add context) |
| `otel.instrumentation.logback-mdc.enabled` | `true` | Add trace_id/span_id to logs |

**Alternative: Environment Variables**

You can also configure via environment variables:
```bash
export OTEL_SERVICE_NAME=book-loan-consumer
export OTEL_TRACES_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_METRICS_EXPORTER=none
export OTEL_LOGS_EXPORTER=none
export OTEL_INSTRUMENTATION_LOGBACK_MDC_ENABLED=true

java -javaagent:agent/opentelemetry-javaagent.jar -jar target/book-loan-consumer-0.0.1-SNAPSHOT.jar
```

### Consumer Configuration Explained

| Property | Value | Description |
|----------|-------|-------------|
| `group-id` | `book-loan-consumer-group` | Consumer group for load balancing |
| `auto-offset-reset` | `earliest` | Start from beginning if no offset exists |
| `spring.json.trusted.packages` | `*` | Allow deserialization from any package |
| `spring.json.use.type.headers` | `false` | Ignore type info from producer |

## Observability

### Distributed Tracing

The OpenTelemetry agent **automatically instruments**:
- ✅ Kafka message consumption
- ✅ HTTP requests (Spring Boot endpoints)
- ✅ Database calls (if any)
- ✅ External HTTP calls
- ✅ Method execution spans

**View traces in Jaeger:**

1. Open http://localhost:16686
2. Select service: `book-loan-consumer`
3. Click "Find Traces"
4. Click any trace to see:
    - Message consumption timing
    - Processing duration
    - Service dependencies
    - Error details (if any)

**Trace Propagation:**

When messages flow from `book-loan-service` → Kafka → `book-loan-consumer`, you'll see:
- Complete end-to-end trace across both services
- Message publish → consume latency
- Full request lifecycle in a single trace view

### Structured Logging with Trace Context

Logs now automatically include trace correlation:

**Before (without agent):**
```
INFO [book-loan-consumer,,] - 📚 RECEIVED: BookLoan{loanId='L-1001', ...}
```

**After (with agent):**
```
INFO [book-loan-consumer,1dc6ae8f4b2c3a1e,abc123def456,01] - 📚 RECEIVED: BookLoan{loanId='L-1001', ...}
                          ^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^  ^^
                          trace_id          span_id       sampled
```

**Benefits:**
- 🔍 Search logs by trace_id to see all related logs
- 🔗 Click trace_id in Jaeger to jump to relevant logs
- 📊 Correlate logs with distributed traces
- 🐛 Debug issues across services easily

**Log Levels:**
- `INFO` - Message received and processed successfully
- `DEBUG` - Detailed processing information
- `WARN` - Processing issues (non-fatal)
- `ERROR` - Processing failures

### Monitoring Endpoints

Spring Boot Actuator provides health and monitoring endpoints:

| Endpoint | Description | URL |
|----------|-------------|-----|
| `/actuator/health` | Health check | http://localhost:8081/actuator/health |
| `/actuator/metrics` | Application metrics | http://localhost:8081/actuator/metrics |
| `/actuator/info` | Application info | http://localhost:8081/actuator/info |

## Testing

### 1. Start the consumer with tracing
```bash
./scripts/run-with-tracing.sh
```

### 2. Publish messages from the producer
```bash
curl -X POST http://localhost:8080/api/loans/publish
```

### 3. Watch consumer logs with trace context
You should see logs like:
```
INFO [book-loan-consumer,1dc6ae8...,abc123...] - ========================================
INFO [book-loan-consumer,1dc6ae8...,abc123...] - 📚 RECEIVED: BookLoan{loanId='L-1001', memberId='M-001', ...}
INFO [book-loan-consumer,1dc6ae8...,abc123...] - ========================================
```

### 4. View traces in Jaeger
1. Open http://localhost:16686
2. Select `book-loan-consumer` from the service dropdown
3. Click "Find Traces"
4. See the complete message processing flow

### 5. Search logs by trace ID
Copy the trace_id from logs (e.g., `1dc6ae8f4b2c3a1e`) and:
- Search your log aggregation tool
- Find all related logs across all services
- Debug issues end-to-end

## Project Structure
```
book-loan-consumer/
├── agent/
│   ├── opentelemetry-javaagent.jar          # OTel Java agent
│   └── README.md                             # Agent version info
├── scripts/
│   └── run-with-tracing.sh                   # Helper script to run with agent
├── src/
│   ├── main/
│   │   ├── java/com/bvd/consumer/
│   │   │   ├── kafka/                        # Kafka listeners
│   │   │   ├── model/                        # Domain models (BookLoan)
│   │   │   └── BookLoanConsumerApplication.java
│   │   └── resources/
│   │       ├── application.properties        # Spring configuration
│   │       └── logback-spring.xml            # Logging configuration (optional)
│   └── test/
├── docker-compose.yml                        # Kafka + Jaeger setup
├── pom.xml                                   # Maven dependencies
└── README.md                                 # This file
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
- **Jaeger**: http://localhost:16686 - Distributed tracing UI
- **Kafka**: localhost:9092 - Message broker
