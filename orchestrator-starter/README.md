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

This library combines three distributed systems patterns into one:

### 1. Saga Orchestrator

A multi-step workflow where each step is an independent operation (API call, DB write). If a step fails permanently, previously completed steps are **compensated** (rolled back) in reverse order. Unlike choreography (where services react to events), the orchestrator **centrally controls** the flow — it decides what step runs next, when to retry, and when to compensate.

### 2. Transactional Outbox

The dual-write problem: after a step completes, the library needs to (a) save the result to MongoDB and (b) publish a Kafka message for the next step. These are two different systems — if the container crashes between them, one write is lost. The outbox pattern solves this by writing both the flow state and the Kafka message intent to the **same MongoDB database**. A background poller reads unpublished outbox events and sends them to Kafka. Both writes go to the same DB, so a crash either loses both (safe — Kafka redelivers) or keeps both (outbox publisher sends later).

### 3. Kafka Non-Blocking Retry

Failed steps don't block the consumer. Spring Kafka routes failed messages to **dedicated retry topics** (`-retry-0`, `-retry-1`, `-retry-2`) with configurable exponential backoff and jitter. After all retries, messages go to a **dead letter topic** (`-dlt`). This is entirely Kafka-managed — retry state survives container crashes because it lives in Kafka, not in memory.

### How they work together

```
Your Application                     Library (auto-configured)
─────────────                        ─────────────────────────

@Flow class                          FlowOrchestrator
  @Step methods      ──────────►       ├── Step Registry (discovers @Step methods)
  @Compensate                          ├── Outbox Writer (atomic with flow save)
  @RecoverOn                           ├── Outbox Publisher (polls → Kafka)
  @Parallel                            ├── Kafka Consumer (one step per message)
  @JoinOn                              ├── Retry Topic Config (jittered backoff)
                                       ├── Idempotency Service (two-layer dedup)
                                       ├── Step Audit Logger (before/after snapshots)
                                       ├── Stale Flow Recovery (crash safety net)
                                       └── Compensation Engine (reverse on failure)

Flow Entity          ──────────►     MongoDB
  extends AbstractFlow                   ├── flow collection (your domain)
                                       ├── orchestrator_outbox
                                       ├── orchestrator_processed_events
                                       └── orchestrator_step_log

                                     Kafka Topics (auto-created)
                                       ├── {topic}           (step commands)
                                       ├── {topic}-retry-0   (2s + jitter)
                                       ├── {topic}-retry-1   (4s + jitter)
                                       ├── {topic}-retry-2   (8s + jitter)
                                       └── {topic}-dlt       (dead letter)
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

### 3. Flow entity (just your domain fields)

Extend `AbstractFlow` — all library tracking fields (id, status, retryCount, currentStep, version, etc.) are inherited. You only declare your domain fields.

```java
@Data
@EqualsAndHashCode(callSuper = true)
@Document(collection = "order_flows")
public class OrderEntity extends AbstractFlow {
    // Only YOUR fields — nothing else
    private BigDecimal amount;
    private String paymentId;
    private String trackingNumber;
    private String address;
}
```

No repository interface needed — the library auto-generates one.
No `@EnableMongoRepositories` — the library auto-scans its packages.

### 4. Configure

```yaml
spring:
  data.mongodb.uri: mongodb://localhost:27017/my_db
  kafka.bootstrap-servers: localhost:9092

orchestrator:
  kafka.command-topic: orders.commands
```

That's it — **2 files** (entity + flow class) and **4 lines of config**.

The library auto-configures:
- REST endpoints: `POST /flows`, `GET /flows/{id}`, `GET /flows/correlation/{id}`
- Kafka retry topics with jittered backoff
- Transactional outbox
- Two-layer idempotency
- Stale flow recovery
- Step audit logging
- Compensation on failure

> Disable auto-endpoints with `orchestrator.endpoints.enabled: false`.
> Override base path with `orchestrator.endpoints.base-path: /my-flows`.

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

Kafka guarantees **at-least-once** delivery — messages may be redelivered after a crash or rebalance. Without idempotency, a redelivered message would call the vendor API again, causing a duplicate charge/creation. The library uses two independent layers to prevent this:

| Layer | Where | What it checks | Protects against |
|-------|-------|---------------|-----------------|
| **Layer 1: Consumer** | `orchestrator_processed_events` collection | Kafka message `eventId` | Duplicate messages from Kafka redelivery after crash, rebalance, or retry topic routing |
| **Layer 2: Handler** | `completedWhen` SpEL on `@Step` | Domain result fields (e.g., `paymentId != null`) | Crash between API call succeeding and Layer 1 write completing |

### Resolution flow

```
Kafka message arrives
  │
  ▼
Layer 1: isProcessed(eventId)?
  ├── YES → skip entirely (fast path, no DB lookup)
  │
  └── NO → load flow from MongoDB
            │
            ▼
          Layer 2: completedWhen → evaluate SpEL against flow
            ├── TRUE → result already in DB from a previous attempt
            │          skip API call, advance to next step
            │
            └── FALSE → execute step (call vendor API)
                          │
                          ▼
                     API returns → set result on flow → save to MongoDB
                          │
                          ▼
                     Layer 1: markProcessed(eventId)  ← AFTER completion
```

### Why Layer 1 marks processed AFTER, not before

```
WRONG (mark before):                    RIGHT (mark after):
1. markProcessed ✓                      1. execute step
2. execute step                         2. save flow
   ← CRASH →                              ← CRASH →
3. (never runs)                         3. markProcessed (never runs)

Redelivery:                             Redelivery:
  isProcessed → YES → SKIP               isProcessed → NO → proceed
  Step never executes. Flow stuck.        completedWhen → TRUE → skip API
                                          Advance → markProcessed → done
```

### The two layers together

Neither layer alone is sufficient:
- **Layer 1 alone**: if we mark before execution, a crash means the step never runs but is marked "done"
- **Layer 2 alone**: if the vendor API is called but the result field isn't set yet (crash between API return and `flow.setPaymentId()`), Layer 2 can't detect the previous call

Together: Layer 1 is the **fast path** (skip without even loading the flow). Layer 2 is the **safe path** (even if re-entered, the vendor API is never called twice because the result field is already set from the previous successful call).

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

Three independent recovery mechanisms, from fastest to slowest. Any one alone is sufficient — together they're redundant safety nets.

### Recovery path 1: Outbox Publisher (~500ms)

The outbox publisher runs on **every pod** in the cluster, polling every 500ms. When Pod A crashes, a surviving pod's publisher picks up the unpublished outbox event within milliseconds. **No scheduler, no waiting, no rebalance needed.**

```
Pod A: step succeeds → flow saved to MongoDB → outbox event saved to MongoDB
       ← POD A CRASHES →

Pod B (or any surviving pod):
  OutboxPublisher polls every 500ms
    → finds unpublished event
    → sends to Kafka
    → next step executes on any available pod
    = recovery in ~500ms
```

This is why the outbox pattern is faster than scheduled recovery — it doesn't wait for a timer. The poller runs continuously on all pods.

### Recovery path 2: Kafka redelivery (~30s)

If the crash happened during step execution (offset not committed), Kafka detects the dead consumer after `session.timeout.ms` (30s), rebalances the partition to another pod, and redelivers the message.

```
Pod A: executing step → API called → offset NOT committed
       ← POD A CRASHES →

Kafka:
  30s later: no heartbeat from Pod A
    → rebalance → Pod B gets partition
    → message redelivered
    → Layer 2: completedWhen → result already set → skip API
    → advance to next step
```

### Recovery path 3: Stale flow scanner (5 min)

Safety net for edge cases where both outbox and Kafka redelivery miss (extremely rare — e.g., outbox event wasn't written and offset was committed but step didn't complete).

```
StaleFlowRecoveryService (runs every 30s):
  query: flows WHERE status=IN_PROGRESS AND updatedAt < (now - 5 min)
    → re-publishes step command to Kafka
    → idempotency prevents duplicate execution
```

### Kafka long-term retry (hours or days)

Kafka retry topics can hold messages for as long as the topic's retention period (default: 7 days). If a vendor is down for hours, messages sit in the retry topics and are redelivered when the vendor comes back.

```
orchestrator:
  retry:
    max-attempts: 10                  # more retries for longer outages
    initial-interval-ms: 5000         # 5s → 10s → 20s → 40s → 80s → 160s → 320s → ...
    multiplier: 2.0
    max-interval-ms: 3600000          # cap at 1 hour between retries
    jitter-factor: 0.5

# With these settings:
#   Attempt 1:  5s
#   Attempt 2:  10s
#   Attempt 3:  20s
#   Attempt 4:  40s
#   Attempt 5:  80s
#   Attempt 6:  160s (~3 min)
#   Attempt 7:  320s (~5 min)
#   Attempt 8:  640s (~10 min)
#   Attempt 9:  1200s (~20 min) → capped at 3600s (1 hour)
#   Attempt 10: 3600s (1 hour)
#   Total: ~2 hours of retry before DLT
```

The retry state lives in **Kafka, not in memory**. If all pods restart during an outage, the retry messages are still in the topics — they resume processing when pods come back. No state lost.

After all retries exhaust, the message goes to the DLT topic, `@Compensate` runs in reverse, and the flow is marked FAILED. An operator can then fix the issue and manually re-trigger the flow.

---

## Kafka Rebalancing

When you run multiple containers (pods), Kafka distributes partitions across them. When a container joins, leaves, or crashes, Kafka **rebalances** — reassigns partitions. How this happens determines whether your flows pause or keep processing.

### Default vs CooperativeStickyAssignor

```
DEFAULT (RangeAssignor) — stop-the-world:

  Pod C joins consumer group
  ├── ALL consumers STOP processing
  ├── ALL partitions revoked
  ├── Wait ~30s for rebalance
  ├── Partitions reassigned
  └── Resume
  = 30s of ZERO processing. P0,P1,P3,P4 didn't need to move but stopped anyway.

THIS LIBRARY (CooperativeStickyAssignor) — incremental:

  Pod C joins consumer group
  ├── Only partitions that NEED to move are revoked
  ├── A: [P0,P1,P2] → revoke P2 → [P0,P1] keeps running
  ├── B: [P3,P4,P5] → revoke P5 → [P3,P4] keeps running
  └── C: gets [P2,P5]
  = Only P2,P5 paused briefly. Everything else never stopped.
```

### What happens to in-flight messages during rebalance

```
Pod A processing flow-123 step=CHARGE_PAYMENT
  ├── API call in progress
  │     ← REBALANCE TRIGGERS →
  ├── Pod A's partition revoked
  ├── Offset NOT committed (step still in progress)
  │
  ▼ Pod B gets the partition
  ├── Message redelivered from last committed offset
  ├── Layer 1: isProcessed? → NO → proceed
  ├── Layer 2: completedWhen("paymentId != null")
  │     ├── YES (Pod A finished the API call before losing partition) → skip API
  │     └── NO (Pod A crashed mid-call) → re-execute step
  └── Flow continues safely either way
```

### Guarantees during rebalancing

| Guarantee | Status | How |
|-----------|--------|-----|
| No duplicate API calls | ✅ Guaranteed | Layer 2: `completedWhen` checks result fields |
| No lost messages | ✅ Guaranteed | Kafka redelivers from last committed offset |
| No stuck flows | ✅ Guaranteed | Outbox (~500ms) + Kafka redeliver (~30s) + recovery service (5 min) |
| Flow step ordering | ✅ Guaranteed | `flowId` as Kafka key → all steps on same partition |
| Minimal processing pause | ✅ Yes | Only moving partitions pause |
| Instant crash detection | ⚠️ ~30s | `session.timeout.ms` (but outbox recovers in ~500ms) |

### Configuration (standard Spring Boot — no custom code)

```yaml
spring.kafka.consumer.properties:
  partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
  session.timeout.ms: 30000       # detect dead consumer after 30s
  heartbeat.interval.ms: 10000    # heartbeat every 10s (must be < session/3)
  max.poll.interval.ms: 300000    # allow 5 min for slow vendor calls
  # group.instance.id: ${HOSTNAME}   # Kubernetes StatefulSet — skip rebalance on restart
  # client.rack: ${KAFKA_RACK}       # multi-region — read from closest replica

spring.kafka.listener:
  concurrency: 3                  # consumer threads per pod (≤ partition count)
  ack-mode: RECORD                # commit offset per message, not per batch
```

### Static membership for Kubernetes

Without `group.instance.id`, every pod restart triggers a rebalance (new random pod name = new consumer). With it:

```
Pod B restarts (same hostname in StatefulSet):
  1. B disconnects
  2. Kafka waits session.timeout.ms (45s) before rebalancing
  3. B reconnects within 45s with same instance ID
  4. Kafka: "same consumer, no rebalance needed"
  = 0 rebalances for a simple restart
```

Only works with Kubernetes **StatefulSet** (stable hostnames). Regular Deployments get random names — use `CooperativeStickyAssignor` without static membership (brief rebalances are handled safely by idempotency).

---

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `orchestrator.kafka.command-topic` | `orchestrator.commands` | Kafka topic |
| `orchestrator.retry.max-attempts` | `4` | 1 initial + N-1 retries. **Determines how many retry topics are created.** |
| `orchestrator.retry.initial-interval-ms` | `2000` | First retry delay (ms) |
| `orchestrator.retry.multiplier` | `2.0` | Each retry multiplies the delay |
| `orchestrator.retry.max-interval-ms` | `30000` | Cap on retry delay (ms) |
| `orchestrator.retry.jitter-factor` | `0.5` | 0.0=none, 0.5=equal, 1.0=full |
| `orchestrator.recovery.scan-interval-ms` | `30000` | Stale flow scan interval |
| `orchestrator.recovery.stale-threshold-minutes` | `5` | Stale after N min |
| `orchestrator.retention.outbox-days` | `7` | TTL: auto-delete published outbox events |
| `orchestrator.retention.processed-events-days` | `30` | TTL: auto-delete idempotency records |
| `orchestrator.retention.step-log-days` | `90` | TTL: auto-delete step audit logs. 0=keep forever |
| `orchestrator.audit.include-flow-state` | `false` | Include full flow JSON in step logs (expensive at scale) |
| `orchestrator.endpoints.enabled` | `true` | Auto-expose REST endpoints |
| `orchestrator.endpoints.base-path` | `/flows` | Base path for auto REST endpoints |
| `orchestrator.mongodb.transactions-enabled` | `false` | Atomic outbox on replica set |

### Retry Ladder Examples

`max-attempts` controls how many retry topics are created. Each topic adds one exponential backoff step. The delay doubles each time until it hits `max-interval-ms`.

**Quick retry (default) — fail fast, 3 retries in ~14 seconds:**
```yaml
orchestrator.retry:
  max-attempts: 4           # retry-0, retry-1, retry-2, dlt
  initial-interval-ms: 2000 # 2s → 4s → 8s
  multiplier: 2.0
  max-interval-ms: 30000
```
```
retry-0: 2s    retry-1: 4s    retry-2: 8s    → dlt
Total: ~14s before dead letter
```

**Medium retry — handle flaky vendor, 6 retries over ~2 minutes:**
```yaml
orchestrator.retry:
  max-attempts: 7
  initial-interval-ms: 2000
  multiplier: 2.0
  max-interval-ms: 60000
```
```
retry-0: 2s  retry-1: 4s  retry-2: 8s  retry-3: 16s  retry-4: 32s  retry-5: 60s  → dlt
Total: ~2 min before dead letter
```

**Long retry — survive vendor outage, 12 retries over ~2 hours:**
```yaml
orchestrator.retry:
  max-attempts: 13
  initial-interval-ms: 5000
  multiplier: 2.0
  max-interval-ms: 3600000  # cap at 1 hour
```
```
retry-0:  5s       retry-6:  320s (~5 min)
retry-1:  10s      retry-7:  640s (~10 min)
retry-2:  20s      retry-8:  1280s (~21 min)
retry-3:  40s      retry-9:  2560s (~42 min)
retry-4:  80s      retry-10: 3600s (1 hour, capped)
retry-5:  160s     retry-11: 3600s (1 hour, capped)
→ dlt
Total: ~2.5 hours of retry before dead letter
```

**24-hour retry — never give up easily:**
```yaml
orchestrator.retry:
  max-attempts: 20
  initial-interval-ms: 10000
  multiplier: 2.0
  max-interval-ms: 7200000  # cap at 2 hours
  jitter-factor: 0.5
```
Retries for ~24 hours with increasing intervals, capping at 2-hour gaps.

> Retry state lives in **Kafka topics, not in memory**. If all pods restart during an outage, retry messages are still in Kafka — they resume when pods come back.

---

## MongoDB Collections

| Collection | Managed by | Purpose |
|-----------|-----------|---------|
| Your collection (e.g., `order_flows`) | You | Flow state + domain fields |
| `orchestrator_outbox` | Library | Transactional outbox |
| `orchestrator_processed_events` | Library | Consumer idempotency |
| `orchestrator_step_log` | Library | Step audit trail |

### Your flow collection (e.g., `order_flows`)

```json
{
  "_id": "682b3f1a2e9c",
  "correlationId": "a1b2c3d4-e5f6-7890",
  "currentStep": "FINALIZE_DOCUMENT",
  "status": "COMPLETED",
  "retryCount": 0,
  "backoffSeconds": 0,
  "nextRetryAt": null,
  "errorMessage": null,
  "updatedAt": "2026-04-25T14:00:05Z",
  "createdAt": "2026-04-25T14:00:00Z",
  "version": 5,
  "completedParallelSteps": [],

  "title": "Contract #123",
  "signerEmail": "john@example.com",
  "documentId": "doc-456",
  "signatureRequestId": "sig-789",
  "finalUrl": "https://enigio.com/docs/doc-456"
}
```

The top fields are from `AbstractFlow` (managed by library). The bottom fields are yours.

### `orchestrator_outbox` — transactional outbox

Each entry is a pending Kafka publish. The outbox publisher polls every 500ms and sends unpublished events.

```json
{
  "_id": "evt-001",
  "flowId": "682b3f1a2e9c",
  "topic": "enigio.commands",
  "key": "682b3f1a2e9c",
  "payload": "{\"eventId\":\"x1\",\"flowId\":\"682b3f1a2e9c\",\"stepName\":\"REQUEST_SIGNATURE\"}",
  "published": true,
  "publishedAt": "2026-04-25T14:00:02Z",
  "createdAt": "2026-04-25T14:00:02Z"
}
```

- `published: false` → outbox publisher picks it up next poll
- `published: true` → already sent to Kafka, kept for audit

### `orchestrator_processed_events` — consumer idempotency (Layer 1)

Each entry is a Kafka message that was fully processed. Prevents duplicate step execution on redelivery.

```json
{
  "_id": "x1",
  "processedAt": "2026-04-25T14:00:03Z"
}
```

The `_id` is the `eventId` from the Kafka message. If this document exists, the message is skipped.

### `orchestrator_step_log` — step execution audit trail

Every step attempt is logged with before/after flow state snapshots.

```json
{
  "_id": "log-001",
  "flowId": "682b3f1a2e9c",
  "stepName": "CREATE_DOCUMENT",
  "status": "COMPLETED",
  "attemptNumber": 1,
  "flowStateBefore": "{\"documentId\":null,...}",
  "flowStateAfter": "{\"documentId\":\"doc-456\",...}",
  "errorMessage": null,
  "durationMs": 342,
  "startedAt": "2026-04-25T14:00:01Z",
  "completedAt": "2026-04-25T14:00:01.342Z"
}
```

Failed/retried steps show the error and attempt number:

```json
{
  "stepName": "REQUEST_SIGNATURE",
  "status": "RETRYING",
  "attemptNumber": 2,
  "errorMessage": "HTTP 500 on REQUEST_SIGNATURE: Internal Server Error",
  "durationMs": 15
}
```

Compensation is also logged:

```json
{
  "stepName": "CREATE_DOCUMENT",
  "status": "COMPENSATED",
  "attemptNumber": 1,
  "durationMs": 120
}
```

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
