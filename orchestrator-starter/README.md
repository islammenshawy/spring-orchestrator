# orchestrator-starter

A Spring Boot starter implementing the **Saga + Transactional Outbox** pattern for resilient, multi-step workflow orchestration backed by Kafka and MongoDB.

Define your entire flow in one annotated class. The library handles Kafka retry topics with jittered exponential backoff, transactional outbox, two-layer idempotency, compensation (rollback), step audit logging, and container crash recovery.

---

## Quick Start

### 1. Add dependency

```xml
<dependency>
    <groupId>com.enigio.orchestrator</groupId>
    <artifactId>orchestrator-starter</artifactId>
</dependency>
```

### 2. Define your flow (one class, all steps)

```java
@Component
@Flow(topic = "orders.commands")
@RetryOn(httpStatus = {500, 502, 503, 429})       // class-level default
@FailOn(httpStatus = {400, 403})                   // class-level default
public class OrderFlow extends FlowDefinition<OrderFlow> {

    @Autowired private PaymentClient paymentClient;
    @Autowired private ShippingClient shippingClient;

    @Step(order = 1, completedWhen = "paymentId != null")
    @RecoverOn(httpStatus = 409, message = "already charged", action = RecoverAction.SKIP)
    public void chargePayment(OrderFlowEntity flow) {
        var result = paymentClient.charge(flow.getAmount());
        flow.setPaymentId(result.getId());
    }

    @Compensate(step = "chargePayment")
    public void refundPayment(OrderFlowEntity flow) {
        paymentClient.refund(flow.getPaymentId());
    }

    @Step(order = 2, completedWhen = "trackingNumber != null")
    public void shipOrder(OrderFlowEntity flow) {
        var result = shippingClient.ship(flow.getAddress());
        flow.setTrackingNumber(result.getTracking());
    }

    @Compensate(step = "shipOrder")
    public void cancelShipment(OrderFlowEntity flow) {
        shippingClient.cancel(flow.getTrackingNumber());
    }
}
```

### 3. Define flow entity + repository

```java
@Document(collection = "order_flows")
public class OrderFlowEntity implements OrchestratorFlow {
    @Id private String id;
    private String correlationId;
    private String currentStep;
    private FlowStatus status = FlowStatus.PENDING;
    private int retryCount, backoffSeconds;
    private Instant nextRetryAt, updatedAt;
    private String errorMessage;
    private Set<String> completedParallelSteps = new HashSet<>();
    @Version private Long version;

    // Your domain fields
    private BigDecimal amount;
    private String paymentId;
    private String trackingNumber;
    private String address;
    // ... getters/setters (or use Lombok)
}

public interface OrderFlowRepository extends OrchestratorFlowRepository<OrderFlowEntity> {}
```

### 4. Configure

```yaml
orchestrator:
  kafka:
    command-topic: orders.commands
  retry:
    max-attempts: 4           # 1 initial + 3 retries
    initial-interval-ms: 2000 # 2s → 4s → 8s (exponential)
    multiplier: 2.0
    jitter-factor: 0.5        # equal jitter (prevents thundering herd)

spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
    listener:
      concurrency: 3
      ack-mode: RECORD
```

---

## How It Works

### Step Execution Flow

```
POST /orders                          ← your REST controller
  │
  ▼
FlowOrchestrator.startFlow()
  ├── Save flow to MongoDB (status=IN_PROGRESS)
  ├── Save outbox event to MongoDB (same DB)
  │
  ▼
OutboxPublisher (polls every 500ms)
  ├── Reads unpublished events
  ├── Sends to Kafka topic: orders.commands
  │
  ▼
KafkaConsumer receives message
  ├── Layer 1: check processed_events (fast skip if duplicate)
  ├── Load flow from MongoDB
  ├── Layer 2: completedWhen check (skip API if result exists)
  ├── Execute step (calls your vendor/service)
  ├── Save result to flow
  ├── Save outbox event for next step
  ├── Mark as processed (Layer 1)
  │
  ▼
Next step... until last step → status=COMPLETED
```

### Retry with Jittered Exponential Backoff

```
Step fails (vendor returns HTTP 500)
  │
  ├──→ orders.commands              attempt 1 (fail)
  ├──→ orders.commands-retry-0      2s + jitter (fail)
  ├──→ orders.commands-retry-1      4s + jitter (fail)
  ├──→ orders.commands-retry-2      8s + jitter (fail)
  └──→ orders.commands-dlt          dead letter
         │
         ▼
       DLT Handler
         ├── Mark flow FAILED
         └── Run @Compensate methods in reverse order
               ├── cancelShipment()   ← undo step 2
               └── refundPayment()    ← undo step 1
```

**Jitter** prevents the thundering herd — when 100 flows fail simultaneously, retries spread across the backoff window instead of all hitting at the same instant:

```
jitter-factor=0.0:  2000ms → 4000ms → 8000ms    (all aligned = thundering herd)
jitter-factor=0.5:  1000-2000ms → 2000-4000ms    (equal jitter, recommended)
jitter-factor=1.0:  0-2000ms → 0-4000ms          (full jitter, maximum spread)
```

### Container Crash Recovery

```
Pod A: step succeeds → flow saved → outbox event saved
       ← POD CRASHES →
       Kafka offset not committed

Recovery path 1 — Outbox (fast, ~500ms):
  OutboxPublisher on any pod → polls → sends to Kafka → flow continues

Recovery path 2 — Kafka redelivery (30s):
  Rebalance → Pod B gets partition → message redelivered
  Layer 2: completedWhen → result already set → skip API → advance
```

---

## Parallel Execution + Join

Steps at the same order with `@Parallel` execute concurrently. `@JoinOn` waits for all to complete.

```java
@Step(order = 1, completedWhen = "documentId != null")
public void createDocument(MyFlow flow) { ... }

@Step(order = 2, completedWhen = "attachmentId != null")
@Parallel(group = "prep")
public void uploadAttachment(MyFlow flow) { ... }

@Step(order = 2, completedWhen = "signatureRequestId != null")
@Parallel(group = "prep")
public void requestSignature(MyFlow flow) { ... }

@Step(order = 3)
@JoinOn(group = "prep")
public void verifyBoth(MyFlow flow) {
    // Only runs when BOTH parallel steps have completed
}
```

```
Step 1: createDocument
         │
    ┌────┴────┐
    ▼         ▼
Step 2a:   Step 2b:         (concurrent via separate Kafka messages)
upload     requestSig
    │         │
    └────┬────┘
         ▼
Step 3: verifyBoth           (@JoinOn waits for both)
```

Each parallel step has its own retry/idempotency. If one fails and retries, the other keeps its completed state. The join step only executes when all `completedWhen` conditions in the group are satisfied.

---

## Annotations Reference

### Class-level

| Annotation | Purpose |
|-----------|---------|
| `@Flow(topic = "...")` | Marks class as a flow definition |
| `@RetryOn(httpStatus = {...})` | Default: which HTTP codes trigger Kafka retry |
| `@FailOn(httpStatus = {...})` | Default: which HTTP codes fail immediately |

### Method-level

| Annotation | Purpose |
|-----------|---------|
| `@Step(order, completedWhen)` | Marks method as a step. `completedWhen` is SpEL for idempotency. |
| `@RecoverOn(httpStatus, message, action)` | Auto-recover on vendor "already exists" (e.g., HTTP 409) |
| `@Compensate(step = "methodName")` | Rollback method, called in reverse on failure |
| `@Parallel(group = "name")` | Execute concurrently with other steps in same group |
| `@JoinOn(group = "name")` | Wait for all parallel steps in group to complete |

Method-level annotations override class-level defaults.

### Error handling resolution order

1. `@RecoverOn` match → treat as success (skip to next step)
2. `@FailOn` match → `NonRetryableStepException` → FAILED + compensation
3. `@RetryOn` match → `RetryableStepException` → Kafka retry topics
4. Manual `throw RetryableStepException` / `NonRetryableStepException`
5. Default: any unhandled exception → retryable

---

## Two-Layer Idempotency

| Layer | Where | Protects against |
|-------|-------|-----------------|
| **Layer 1** | `orchestrator_processed_events` | Duplicate Kafka messages (redelivery after crash/rebalance) |
| **Layer 2** | `completedWhen` SpEL on `@Step` | Crash between API call success and processed_events write |

**Key design**: Layer 1 marks processed **after** step completion, not before. A crash mid-step → redelivery → Layer 2 checks result field → skips API call → completes safely.

---

## Startup Validation

The library validates at startup (fail-fast, clear error messages):

- `@Compensate(step = "X")` must reference an existing `@Step` method
- `@JoinOn(group = "X")` must reference an existing `@Parallel` group
- `@Parallel` steps must have `completedWhen` (needed for join verification)
- No duplicate step orders or names within a `@Flow`
- `@Step` and `@Compensate` methods must accept exactly one `OrchestratorFlow` parameter

---

## MongoDB Collections

| Collection | Purpose | Managed by |
|-----------|---------|-----------|
| Your flow collection | Flow state + domain fields | You |
| `orchestrator_outbox` | Transactional outbox events | Library |
| `orchestrator_processed_events` | Consumer idempotency keys | Library |
| `orchestrator_step_log` | Step execution audit trail | Library |

Enable `orchestrator.mongodb.transactions-enabled=true` on replica sets for full atomicity between flow save + outbox write.

---

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `orchestrator.kafka.command-topic` | `orchestrator.commands` | Kafka topic for step commands |
| `orchestrator.retry.max-attempts` | `4` | 1 initial + N-1 retries |
| `orchestrator.retry.initial-interval-ms` | `2000` | First retry delay |
| `orchestrator.retry.multiplier` | `2.0` | Exponential multiplier |
| `orchestrator.retry.max-interval-ms` | `30000` | Max retry delay cap |
| `orchestrator.retry.jitter-factor` | `0.5` | 0.0=none, 0.5=equal, 1.0=full |
| `orchestrator.recovery.scan-interval-ms` | `30000` | Stale flow scan interval |
| `orchestrator.recovery.stale-threshold-minutes` | `5` | Consider stale after N min |
| `orchestrator.mongodb.transactions-enabled` | `false` | Atomic outbox on replica set |

---

## Dependencies

| Pulled by library | Not pulled (your app provides) |
|------------------|-------------------------------|
| `spring-boot-starter-data-mongodb` | `spring-boot-starter-web` |
| `spring-kafka` | `spring-boot-starter-webflux` |
| `spring-retry` | `jackson-databind` (provided scope) |
| `spring-boot-autoconfigure` | |

---

## Requirements

- Java 21+
- Spring Boot 3.2+
- MongoDB (standalone or replica set)
- Kafka
