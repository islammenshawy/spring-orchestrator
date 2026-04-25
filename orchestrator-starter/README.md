# orchestrator-starter

Saga + Transactional Outbox pattern as a Spring Boot starter library.
Kafka retry topics with jittered exponential backoff. MongoDB persistence. Annotation-driven.

> For the polished HTML version with dark theme, download and open [docs/index.html](../docs/index.html) locally.

---

## Contents

- [The Pattern](#the-pattern)
- [Quick Start](#quick-start)
- [Annotations Reference](#annotations-reference)
- [Flow Types: Sequential, Parallel, Join](#flow-types)
- [Retry with Jittered Backoff](#retry-with-jittered-exponential-backoff)
- [Error Handling](#error-handling)
- [Two-Layer Idempotency](#two-layer-idempotency)
- [The Unavoidable Gap: API Call + Crash](#the-unavoidable-gap-api-call--crash)
- [Transactional Outbox](#transactional-outbox)
- [Saga Compensation (Rollback)](#saga-compensation-rollback)
- [Container Crash Recovery](#container-crash-recovery)
- [Kafka Rebalancing](#kafka-rebalancing)
- [Configuration Reference](#configuration-reference)
- [MongoDB Collections](#mongodb-collections)
- [Dependencies](#dependencies)
- [Startup Validation](#startup-validation)

---

## The Pattern

The library implements the **Saga Orchestrator** pattern with a **Transactional Outbox** for reliable message delivery. Each workflow step is a separate Kafka message, executed by a single consumer with crash-resilient retry.

```
Your Application                     Library (auto-configured)
─────────────                        ─────────────────────────

@Flow class                          FlowOrchestrator
  @Step methods      ──────────►       ├── Step Registry
  @Compensate                          ├── Outbox Writer
  @RetryOn                             ├── Kafka Consumer
  @RecoverOn                           ├── Retry Topic Config
  @FailOn                              ├── Idempotency Service
  @Parallel                            ├── Step Audit Logger
  @JoinOn                              ├── Stale Flow Recovery
                                       └── Compensation Engine

Flow Entity          ──────────►     MongoDB (flow state)
  implements
  OrchestratorFlow                   Kafka Topics (auto-created)
                                       ├── {topic}
                                       ├── {topic}-retry-0
                                       ├── {topic}-retry-1
                                       ├── {topic}-retry-2
                                       └── {topic}-dlt
```

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
@Flow  // topic from orchestrator.kafka.command-topic in yml
public class OrderFlow extends FlowDefinition<OrderEntity> {

    @Autowired private PaymentClient paymentClient;

    @Step(order = 1, completedWhen = "paymentId != null")
    public void chargePayment(OrderEntity flow) {
        var result = paymentClient.charge(flow.getAmount());
        flow.setPaymentId(result.getId());
    }

    @Step(order = 2, completedWhen = "trackingNumber != null")
    public void shipOrder(OrderEntity flow) {
        var result = shippingClient.ship(flow.getAddress());
        flow.setTrackingNumber(result.getTracking());
    }
}
```

That's the **minimum**. Two annotations (`@Flow`, `@Step`) and your business logic. Everything else is optional:

```java
// Add ONLY when you need them:
@RecoverOn(httpStatus = 409, action = SKIP)  // vendor returns "already exists"
@Compensate(step = "chargePayment")          // rollback on failure
@Parallel(group = "prep")                    // concurrent execution
@JoinOn(group = "prep")                      // wait for parallel steps

// Rarely needed — built-in defaults handle the common case:
@RetryOn(httpStatus = {500, 502, 503, 429})  // DEFAULT: all 5xx + 429 retry
@FailOn(httpStatus = {400, 403})             // DEFAULT: all 4xx (except 429) fail
```

### 3. Flow entity + repository

```java
@Document(collection = "order_flows")
public class OrderEntity implements OrchestratorFlow {
    @Id private String id;
    private String correlationId, currentStep, errorMessage;
    private FlowStatus status = FlowStatus.PENDING;
    private int retryCount, backoffSeconds;
    private Instant nextRetryAt, updatedAt;
    private Set<String> completedParallelSteps = new HashSet<>();
    @Version private Long version;

    // Your domain fields
    private BigDecimal amount;
    private String paymentId, trackingNumber, address;
}

public interface OrderRepository extends OrchestratorFlowRepository<OrderEntity> {}
```

### 4. Configure

```yaml
orchestrator:
  kafka:
    command-topic: orders.commands
  retry:
    max-attempts: 4
    initial-interval-ms: 2000
    multiplier: 2.0
    jitter-factor: 0.5

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

> **That's it.** No orchestrator code, no outbox tables, no Kafka consumer classes, no retry logic.

---

## Annotations Reference

| Annotation | Required? | Default if omitted | Where |
|-----------|-----------|-------------------|-------|
| `@Flow` | **Yes** | `topic` from `orchestrator.kafka.command-topic` in yml | Class |
| `@Step(order, completedWhen)` | **Yes** | `name` = method name as UPPER_SNAKE. `completedWhen` empty = always execute | Method |
| `@RetryOn` | No | **Built-in: HTTP 5xx + 429 → retry via Kafka topics** | Class or method |
| `@FailOn` | No | **Built-in: HTTP 4xx (except 429) → fail immediately** | Class or method |
| `@RecoverOn` | No | No recovery — add for vendor-specific cases (409, etc.) | Class or method |
| `@Compensate(step)` | No | No rollback for that step | Method |
| `@Parallel(group)` | No | Sequential execution | Method |
| `@JoinOn(group)` | No | No join point | Method |

Method-level annotations override class-level. `@RetryOn`/`@FailOn` only needed to override the built-in defaults.

---

## Flow Types

### Sequential (default)

```
Step 1 ──► Step 2 ──► Step 3 ──► COMPLETED
```

### Parallel + Join

```
Step 1
  │
  ├──► Step 2a  (@Parallel group="prep")     concurrent Kafka messages
  ├──► Step 2b  (@Parallel group="prep")
  │
  └──► Step 3   (@JoinOn group="prep")       waits for both
       │
       ▼
  COMPLETED
```

```java
@Step(order = 2, completedWhen = "attachmentId != null")
@Parallel(group = "prep")
public void uploadAttachment(MyFlow flow) { ... }

@Step(order = 2, completedWhen = "signatureId != null")
@Parallel(group = "prep")
public void requestSignature(MyFlow flow) { ... }

@Step(order = 3)
@JoinOn(group = "prep")
public void verifyBoth(MyFlow flow) {
    // only runs when BOTH parallel steps completed
}
```

---

## Retry with Jittered Exponential Backoff

```
Step fails (HTTP 500 from vendor)
  │
  ├──► orders.commands              attempt 1     FAIL
  │         │
  │    ┌────▼──────────────────────────────────────────┐
  │    │  orders.commands-retry-0    2s + jitter   FAIL │
  │    │  orders.commands-retry-1    4s + jitter   FAIL │
  │    │  orders.commands-retry-2    8s + jitter   FAIL │
  │    └────┬──────────────────────────────────────────┘
  │         │
  │    ┌────▼────┐
  │    │  -dlt   │  Dead Letter Topic
  │    └────┬────┘
  │         ▼
  │    DLT Handler:
  │      ├── flow.status = FAILED
  │      └── @Compensate in reverse
```

### Jitter prevents thundering herd

| `jitter-factor` | Behavior | 4s base delay becomes |
|----------------|----------|----------------------|
| `0.0` | No jitter — all aligned (thundering herd) | always 4000ms |
| **`0.5`** | **Equal jitter (recommended)** | 2000-4000ms |
| `1.0` | Full jitter — maximum spread | 0-4000ms |

Formula: `delay = base * (1 - factor) + random(0, base * factor)`

---

## Error Handling

Resolution order when a step throws:

| Priority | Source | Behavior | Example |
|----------|--------|----------|---------|
| 1 | `@RecoverOn` | Treat as success, skip API | HTTP 409 "already created" |
| 2 | `@FailOn` | FAILED + compensation | HTTP 400 bad request |
| 3 | `@RetryOn` | Kafka retry topics | HTTP 500 server error |
| 4 | Manual throw | `RetryableStepException` / `NonRetryableStepException` | "Not yet verified" |
| 5 | Default | Any unhandled → retryable | NullPointerException |

> **No try/catch needed.** Your method is pure business logic.

---

## Two-Layer Idempotency

| Layer | Where | Protects against |
|-------|-------|-----------------|
| **Layer 1** | `orchestrator_processed_events` | Duplicate Kafka messages after crash/rebalance |
| **Layer 2** | `completedWhen` SpEL on `@Step` | Crash between API success and processed_events write |

```
Message → Layer 1: isProcessed? → YES → skip
                                  NO  → execute step
                                          │
                                     API call → save result
                                          ← CRASH →
                                     processed_events NOT written
                                          │
                                     Redelivery → Layer 1: NO → proceed
                                               → Layer 2: result in DB → skip API
                                               → advance → mark processed
```

**Key**: Layer 1 marks processed **after** completion, not before.

---

## The Unavoidable Gap: API Call + Crash

No library can make an HTTP call and a database write atomic. There is always a window where the API succeeded but the result isn't saved.

```
vendorApi.charge(amount);           // money charged
     ← CRASH →
flow.setPaymentId(result.getId());  // never runs

Redelivery → completedWhen = false → API called AGAIN
```

> **This is not a library bug.** No framework can atomically span HTTP + DB.

### Three strategies

**1. Idempotency Key (best)** — vendor deduplicates by key:
```java
vendorApi.charge(amount, idempotencyKey: flow.getCorrelationId() + ":CHARGE");
```

**2. Pre-check** — query vendor before retrying:
```java
var existing = vendorApi.findByRef(flow.getCorrelationId());
if (existing != null) { flow.setPaymentId(existing.getId()); return; }
```

**3. Reserve + Confirm** — two steps, reservation is repeatable:
```java
@Step(order = 1, completedWhen = "reservationId != null")
void reserve(Flow f) { f.setReservationId(vendor.reserve(...)); }

@Step(order = 2, completedWhen = "paymentId != null")
void confirm(Flow f) { f.setPaymentId(vendor.confirm(f.getReservationId())); }
```

### checkpoint() — narrows the gap

```java
var result = paymentClient.charge(flow.getAmount());
flow.setPaymentId(result.getId());
checkpoint(flow);   // saved to MongoDB NOW — crash after here is safe
auditRepo.save(...);
```

---

## Transactional Outbox

```
WITHOUT outbox:                         WITH outbox (this library):
1. Save flow to MongoDB  ✓             1. Save flow to MongoDB      ✓
   ← CRASH →                           2. Save outbox event (same DB) ✓
2. Publish to Kafka       ✗ LOST          ← CRASH →
→ Flow stuck forever                    3. OutboxPublisher polls → Kafka
                                        → Flow continues (~500ms)
```

Enable `orchestrator.mongodb.transactions-enabled=true` on replica sets for fully atomic writes.

---

## Saga Compensation (Rollback)

```
Step 1: chargePayment     ✓
Step 2: shipOrder         ✓
Step 3: generateInvoice   ✗ FAILED
  │
  ▼ Compensation in reverse:
  ├── @Compensate("shipOrder")     → cancelShipment()
  └── @Compensate("chargePayment") → refundPayment()
  │
  ▼ flow.status = FAILED
```

Triggers automatically on `NonRetryableStepException` and DLT.

---

## Container Crash Recovery

```
Pod A: step succeeds → flow saved → outbox saved → CRASH

Path 1 — Outbox (~500ms):    OutboxPublisher polls → sends to Kafka
Path 2 — Kafka (30s):        Rebalance → redeliver → Layer 2 skips API
Path 3 — Recovery (5 min):   StaleFlowRecoveryService re-publishes
```

---

## Kafka Rebalancing

| Default (RangeAssignor) | This library (CooperativeStickyAssignor) |
|------------------------|----------------------------------------|
| ALL consumers stop | Only affected partitions pause |
| ~30s zero processing | Others keep running |

```yaml
spring.kafka.consumer.properties:
  partition.assignment.strategy: CooperativeStickyAssignor
  session.timeout.ms: 30000
  heartbeat.interval.ms: 10000
  # group.instance.id: ${HOSTNAME}   # Kubernetes StatefulSet
  # client.rack: ${KAFKA_RACK}       # multi-region
```

---

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `orchestrator.kafka.command-topic` | `orchestrator.commands` | Kafka topic |
| `orchestrator.retry.max-attempts` | `4` | 1 initial + N-1 retries |
| `orchestrator.retry.initial-interval-ms` | `2000` | First retry delay |
| `orchestrator.retry.multiplier` | `2.0` | Exponential multiplier |
| `orchestrator.retry.max-interval-ms` | `30000` | Max delay cap |
| `orchestrator.retry.jitter-factor` | `0.5` | 0.0=none, 0.5=equal, 1.0=full |
| `orchestrator.recovery.scan-interval-ms` | `30000` | Stale flow scan |
| `orchestrator.recovery.stale-threshold-minutes` | `5` | Stale after N min |
| `orchestrator.mongodb.transactions-enabled` | `false` | Atomic outbox |

---

## MongoDB Collections

| Collection | Managed by | Purpose |
|-----------|-----------|---------|
| Your collection | You | Flow state + domain fields |
| `orchestrator_outbox` | Library | Transactional outbox |
| `orchestrator_processed_events` | Library | Consumer idempotency |
| `orchestrator_step_log` | Library | Step audit trail |

---

## Dependencies

**Pulled:** `spring-boot-starter-data-mongodb`, `spring-kafka`, `spring-retry`, `spring-boot-autoconfigure`

**Not pulled (your app provides):** `spring-boot-starter-web`, `spring-boot-starter-webflux`, `jackson-databind`

---

## Startup Validation

| Rule | Error if violated |
|------|-------------------|
| `@Compensate(step="X")` must reference existing `@Step` | Lists available steps |
| `@JoinOn(group="X")` must reference existing `@Parallel` | Lists available groups |
| `@Parallel` steps must have `completedWhen` | Needed for join |
| No duplicate step orders | Lists conflict |
| No duplicate step names | Lists conflict |
| Methods must accept one `OrchestratorFlow` param | Lists invalid signature |

> Any `@SpringBootTest` catches these. No misconfiguration reaches production.

---

## License

Apache 2.0
