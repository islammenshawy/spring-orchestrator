# orchestrator-starter

A Spring Boot starter for building resilient, multi-step workflow orchestrations backed by Kafka and MongoDB.

Define your steps as simple `StepHandler` beans. The library handles everything else: Kafka retry topics with exponential backoff and jitter, two-layer idempotency, dead letter handling, stale flow recovery, and container crash resilience.

---

## Quick Start

### 1. Add the dependency

```xml
<dependency>
    <groupId>com.enigio.orchestrator</groupId>
    <artifactId>orchestrator-starter</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

### 2. Define your flow entity

Your flow entity carries your domain-specific fields and implements `OrchestratorFlow` for the library to track progress.

```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "payment_flows")
public class PaymentFlow implements OrchestratorFlow {

    // --- Required by the library ---
    @Id private String id;
    @Indexed(unique = true) private String correlationId;
    private String currentStep;
    @Builder.Default private FlowStatus status = FlowStatus.PENDING;
    @Builder.Default private int retryCount = 0;
    @Builder.Default private int backoffSeconds = 0;
    private Instant nextRetryAt;
    private String errorMessage;
    @Builder.Default private Instant updatedAt = Instant.now();
    @Version private Long version;

    // --- Your domain fields ---
    private String orderId;
    private BigDecimal amount;
    private String currency;
    private String paymentGatewayId;
    private String receiptUrl;
}
```

### 3. Define your repository

Extend `OrchestratorFlowRepository` with your flow type. No methods needed — the library's queries are inherited.

```java
public interface PaymentFlowRepository extends OrchestratorFlowRepository<PaymentFlow> {
}
```

### 4. Implement your steps

Each step is a `@Component` that implements `StepHandler<YourFlow>`. The library discovers them at startup, sorts by `getOrder()`, and executes them sequentially via Kafka.

```java
@Component
@RequiredArgsConstructor
public class ChargePaymentStep implements StepHandler<PaymentFlow> {

    private final PaymentGatewayClient gateway;

    @Override public String getStepName() { return "CHARGE_PAYMENT"; }
    @Override public int getOrder() { return 1; }

    @Override
    public boolean isAlreadyCompleted(PaymentFlow flow) {
        // Idempotency guard: if we already have a gateway ID,
        // this step ran on a previous attempt. Don't charge again.
        return flow.getPaymentGatewayId() != null;
    }

    @Override
    public void execute(PaymentFlow flow) {
        try {
            var result = gateway.charge(flow.getAmount(), flow.getCurrency());
            flow.setPaymentGatewayId(result.getId());
        } catch (GatewayTimeoutException e) {
            // Retryable: will go to Kafka retry topic with backoff
            throw new RetryableStepException("Gateway timeout", e);
        } catch (InvalidCardException e) {
            // Non-retryable: flow marked FAILED immediately
            throw new NonRetryableStepException("Card declined", e);
        }
    }
}
```

### 5. Start a flow

Inject `FlowOrchestrator<YourFlow>` and call `startFlow()`. The library saves the flow, publishes the first step command to Kafka, and the pipeline begins.

```java
@RestController
@RequiredArgsConstructor
public class PaymentController {

    private final FlowOrchestrator<PaymentFlow> orchestrator;
    private final PaymentFlowRepository repository;

    @PostMapping("/payments")
    public PaymentFlow createPayment(@RequestBody CreatePaymentRequest request) {
        PaymentFlow flow = PaymentFlow.builder()
                .correlationId(UUID.randomUUID().toString())
                .orderId(request.getOrderId())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .build();
        flow = repository.save(flow);
        return orchestrator.startFlow(flow);
    }
}
```

### 6. Configure

```yaml
spring:
  data:
    mongodb:
      uri: mongodb://localhost:27017/my_database
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      properties:
        enable.idempotence: true

orchestrator:
  kafka:
    command-topic: payments.commands      # your Kafka topic
  retry:
    max-attempts: 4                      # 1 initial + 3 retries
    initial-interval-ms: 2000            # first retry delay
    multiplier: 2.0                      # exponential: 2s -> 4s -> 8s
    max-interval-ms: 30000               # cap at 30s
    jitter-factor: 0.5                   # 0.0=none, 0.5=equal, 1.0=full
  recovery:
    scan-interval-ms: 30000              # poll for stale flows every 30s
    stale-threshold-minutes: 5           # re-publish after 5 min stuck
```

That's it. No orchestrator code, no outbox, no retry logic, no recovery service. The library handles all of it.

---

## What the library creates automatically

### Kafka Topics

For `command-topic: payments.commands`, the library auto-creates:

| Topic | Purpose |
|-------|---------|
| `payments.commands` | Main step commands |
| `payments.commands-retry-0` | 1st retry (2s backoff + jitter) |
| `payments.commands-retry-1` | 2nd retry (4s backoff + jitter) |
| `payments.commands-retry-2` | 3rd retry (8s backoff + jitter) |
| `payments.commands-dlt` | Dead letter (retries exhausted) |

### MongoDB Collections

| Collection | Purpose | Managed by |
|-----------|---------|-----------|
| `payment_flows` (your name) | Your flow documents | You |
| `orchestrator_outbox` | Transactional outbox — pending Kafka publishes | Library |
| `orchestrator_processed_events` | Consumer-side idempotency keys | Library |

---

## Architecture

### Step execution flow

```
POST /payments
    |
    v
FlowOrchestrator.startFlow()
    |-- save flow to MongoDB (status=IN_PROGRESS, currentStep=CHARGE_PAYMENT)
    |-- publish StepCommandMessage to payments.commands
    v
KafkaConsumer receives message
    |-- Layer 1 idempotency: check processed_events (fast skip if already done)
    |-- Load flow from MongoDB
    |-- Layer 2 idempotency: handler.isAlreadyCompleted() (skip API if result exists)
    |-- handler.execute() — calls your vendor/service
    |-- Save flow (with results like paymentGatewayId)
    |-- Save outbox event (same MongoDB — next step command)
    |-- Layer 1 idempotency: mark as processed (AFTER completion)
    v
OutboxPublisher polls (every 500ms)
    |-- Finds unpublished outbox event
    |-- Sends to payments.commands Kafka topic
    |-- Marks event as published
    v
Next step executes... until last step completes -> status=COMPLETED
```

### On failure (RetryableStepException)

```
handler.execute() throws RetryableStepException
    |-- Update flow: status=WAITING_RETRY, retryCount++, backoffSeconds
    |-- Re-throw exception
    v
Spring Kafka catches exception
    |-- Routes message to payments.commands-retry-0
    |-- Waits 2s + jitter
    |-- Redelivers to consumer
    |-- If still failing: retry-1 (4s), retry-2 (8s)
    |-- After all retries: payments.commands-dlt
    v
DLT handler: flow.status = FAILED, errorMessage = "[DLT] Exhausted all retry attempts"
```

### On container crash

```
Step executes -> API called -> result saved to MongoDB
    <- CONTAINER CRASHES ->
    processed_events not written, Kafka offset not committed
    v
New container picks up partition (CooperativeStickyAssignor)
    |-- Message redelivered from last committed offset
    |-- Layer 1: processed_events check -> not found -> proceed
    |-- Layer 2: handler.isAlreadyCompleted() -> result already in MongoDB -> skip API
    |-- Complete normally -> mark processed
```

If crash happens between MongoDB save and Kafka publish:

```
Step succeeds -> flow saved to MongoDB -> outbox event saved to MongoDB
    <- CONTAINER CRASHES ->
    Kafka publish never happened
    v
Container restarts -> OutboxPublisher polls -> finds unpublished event
    |-- Sends to Kafka -> consumer picks it up -> flow continues
    (no 5-minute wait — outbox polls every 500ms)
```

As a safety net, `StaleFlowRecoveryService` also scans for flows stuck in `IN_PROGRESS` for longer than `stale-threshold-minutes` and re-publishes their step commands.

---

## Two-Layer Idempotency

| Layer | Where | Checks | Protects against |
|-------|-------|--------|-----------------|
| **Layer 1** | `orchestrator_processed_events` | `eventId` from Kafka message | Duplicate Kafka messages (redelivery after crash/rebalance) |
| **Layer 2** | Your `StepHandler.isAlreadyCompleted()` | Domain result fields | Crash between API call and processed_events write |

Layer 1 is the **fast path** — skips re-entering the step entirely.
Layer 2 is the **safe path** — even if re-entered, your vendor API is never called twice.

The key design choice: Layer 1 marks processed **after** step completion, not before. This ensures a crash mid-step results in redelivery and re-execution, where Layer 2 prevents the duplicate API call.

---

## Jitter

Without jitter, if 100 flows fail simultaneously, all 100 retry at t+2s, t+6s, t+14s — a thundering herd that likely overwhelms the vendor again.

The library applies jitter using the formula:

```
actual_delay = base * (1 - jitterFactor) + random(0, base * jitterFactor)
```

| `jitter-factor` | Behavior | 4s base delay becomes |
|----------------|----------|----------------------|
| `0.0` | No jitter | always 4000ms |
| `0.5` | Equal jitter (recommended) | 2000-4000ms |
| `1.0` | Full jitter | 0-4000ms |

---

## Kafka Rebalancing

The library configures `CooperativeStickyAssignor` so that when containers scale up/down or crash:

- Only affected partitions are revoked (others keep processing)
- In-flight messages have their offsets not yet committed
- New consumer re-reads from last committed offset
- Two-layer idempotency prevents duplicate execution

For multi-instance deployments, ensure your command topic has multiple partitions:

```bash
kafka-topics --alter --topic payments.commands --partitions 6
```

---

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `orchestrator.kafka.command-topic` | `orchestrator.commands` | Kafka topic for step commands |
| `orchestrator.retry.max-attempts` | `4` | Total attempts (1 initial + N-1 retries) |
| `orchestrator.retry.initial-interval-ms` | `2000` | First retry delay in ms |
| `orchestrator.retry.multiplier` | `2.0` | Exponential multiplier |
| `orchestrator.retry.max-interval-ms` | `30000` | Maximum retry delay cap |
| `orchestrator.retry.jitter-factor` | `0.5` | Jitter: 0.0=none, 0.5=equal, 1.0=full |
| `orchestrator.recovery.scan-interval-ms` | `30000` | How often to scan for stale flows |
| `orchestrator.recovery.stale-threshold-minutes` | `5` | Mark flow as stale after N minutes |

---

## What you write vs what the library provides

| You write | Library provides |
|-----------|-----------------|
| Flow entity (`implements OrchestratorFlow`) | `FlowOrchestrator` — step sequencing engine |
| Repository (`extends OrchestratorFlowRepository`) | Transactional outbox — atomic DB+Kafka guarantee |
| Step handlers (`implements StepHandler`) | Kafka retry topics with jittered exponential backoff |
| REST controller (optional) | Two-layer idempotency (consumer + handler) |
| `application.yml` config | DLT handler — marks flows FAILED after retries |
| | `StaleFlowRecoveryService` — container crash recovery |
| | `StepRegistry` — auto-discovers and orders your steps |
| | `CooperativeStickyAssignor` rebalancing support |

---

## Transactional Outbox

The library uses the transactional outbox pattern internally. When a step completes and the next step needs to be triggered:

```
WITHOUT outbox (direct Kafka publish):
  1. Save flow result to MongoDB     ✓
  <- CONTAINER CRASH ->
  2. Publish to Kafka                ✗ LOST — flow stuck

WITH outbox (what this library does):
  1. Save flow result to MongoDB     ✓
  2. Save outbox event to MongoDB    ✓  (same DB, microseconds apart)
  <- CONTAINER CRASH ->
  3. OutboxPublisher polls           → finds unpublished event → sends to Kafka
```

Both writes go to the same MongoDB instance. On a replica set with `@Transactional`, they're fully atomic. On standalone MongoDB, they're two sequential writes to the same DB — the gap is microseconds vs the milliseconds of a cross-system MongoDB→Kafka write.

The `orchestrator_outbox` collection is managed entirely by the library. You never interact with it.

---

## Dependencies

The library is intentionally minimal. It pulls only what it needs:

| Dependency | Why | Transitively brings |
|-----------|-----|-------------------|
| `spring-boot-starter-data-mongodb` | Flow persistence, outbox, idempotency | MongoDB driver, Spring Data |
| `spring-kafka` | Retry topics, consumer, producer | Kafka clients |
| `spring-boot-autoconfigure` | Auto-configuration | Spring context |
| `spring-retry` | Jittered backoff policy | — |

**Not pulled** (your app provides these if needed):
- `spring-boot-starter-web` — only if you have REST endpoints
- `spring-boot-starter-webflux` — only if you use WebClient for vendor calls
- `jackson-databind` — provided scope, your app's Spring Boot includes it

---

## Requirements

- Java 21+
- Spring Boot 3.2+
- MongoDB (standalone or replica set)
- Kafka

For full transactional atomicity between flow state + outbox writes, use a MongoDB replica set.
