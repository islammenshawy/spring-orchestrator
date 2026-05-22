# Disaster Recovery & Replay Guide

This document covers failure scenarios, what the library handles automatically, what requires manual intervention, and how to replay stuck or failed flows.

## Architecture Context

The orchestrator uses a **saga + outbox + retry** pattern:
- **MongoDB** stores flow state, outbox events, idempotency records, step logs, and consumer offsets
- **Kafka** transports step commands via retry topics (exponential backoff + jitter)
- **Outbox publisher** polls MongoDB every 500ms and publishes to Kafka
- **Recovery scanner** runs every 30s, detects stuck IN_PROGRESS flows, re-publishes
- **Idempotency** is two-layered: `ProcessedEvent` (event-level) + `completedSteps` set (step-level)
- **Batch claiming** ensures multi-pod schedulers don't duplicate work (claimedBy/claimedAt fields)

### Key Collections

| Collection | Purpose |
|---|---|
| `dis_instrument_flows` | Flow state (your entity collection) |
| `orchestrator_outbox` | Outbox events pending Kafka publish |
| `orchestrator_processed_events` | Idempotency deduplication records |
| `orchestrator_step_log` | Step execution audit trail |
| `orchestrator_consumer_offsets` | Kafka offsets for cross-DC failover |

### Key Kafka Topics

| Topic | Purpose |
|---|---|
| `{commandTopic}` | Step commands |
| `{commandTopic}-retry-0/1/2` | Non-blocking retry topics (exponential backoff) |
| `{commandTopic}-dlt` | Dead letter topic (exhausted retries) |
| `{commandTopic}.replies` | Step results (if reply mode enabled) |

---

## 1. Kafka Outage

### What happens
- Outbox publisher fails to send to Kafka, increments `failureCount` on each event
- Kafka consumers disconnect, stop processing step commands
- Retry topics stall (messages sit in partitions until brokers return)

### Auto-recovery
- Outbox publisher retries on next poll cycle (every 500ms)
- Kafka consumers reconnect automatically when brokers return
- Messages in retry topics resume processing after reconnect
- Consumer offsets stored in MongoDB survive Kafka data loss

### When to intervene
- **Never** for transient outages (< 30 min). The outbox + retry pipeline handles this.
- If outbox events hit `maxPublishRetries` (default 5), they are dead-lettered in MongoDB

### Check
```javascript
// Pending outbox events (should drain after Kafka returns)
db.orchestrator_outbox.countDocuments({ published: false, deadLettered: { $ne: true } })

// Dead-lettered outbox events (won't be retried)
db.orchestrator_outbox.find({ deadLettered: true })
```

### Fix dead-lettered outbox events
```javascript
// After fixing the root cause, re-enable the event
db.orchestrator_outbox.updateMany(
  { deadLettered: true },
  { $set: { deadLettered: false, failureCount: 0 }, $unset: { publishedAt: "" } }
)
```

---

## 2. MongoDB Outage

### What happens
- All writes fail: flows can't advance, outbox can't write, CAS updates fail
- Flows remain in their last persisted state
- Kafka consumers receive messages but can't process them (thrown back to retry)

### Auto-recovery
- Spring Data reconnects when MongoDB returns
- Flows resume from last persisted `currentStep` and `completedSteps`
- Recovery scanner picks up IN_PROGRESS flows after stale threshold (15 min)

### When to intervene
- If MongoDB was down long enough that Kafka retries exhausted, check DLT
- Verify no flows are stuck with stale `claimedBy` (pod may have died during outage)

### Check
```javascript
// Stuck claims from outage period
db.dis_instrument_flows.find({ claimedBy: { $ne: null } })

// Force-release all claims
db.dis_instrument_flows.updateMany(
  { claimedBy: { $ne: null } },
  { $set: { claimedBy: null, claimedAt: null } }
)
```

---

## 3. Pod Crash (Container Dies Mid-Step)

### What happens
- Flow stays IN_PROGRESS with `updatedAt` frozen at crash time
- Outbox event may or may not have been written
- If pod held a batch claim, those flows have `claimedBy` set

### Auto-recovery
- **Orphan cleanup**: recovery scanner releases claims older than `claimTtlMinutes` (default 5 min)
- **Stale recovery**: after `staleThresholdMinutes` (default 15 min), re-publishes step command
- **Idempotency**: if the step already completed before crash, `completedSteps` prevents re-execution
- **Kafka offset**: MongoDB offset store preserves position for consumer group rebalance

### When to intervene
- Only if `recoveryCount` reaches `maxRecoveryAttempts` (default 10) -- see section 4

---

## 4. Stuck Flows (Exceeded Recovery Attempts)

### What happens
When a flow fails to advance after `maxRecoveryAttempts` (default 10) recovery cycles, the scanner marks it FAILED with error "Exceeded max recovery attempts."

### Diagnosis
```javascript
db.dis_instrument_flows.find({
  status: "FAILED",
  errorMessage: /max recovery attempts/
})
```

### Root cause investigation
1. Check the step log for the last execution attempt:
   ```javascript
   db.orchestrator_step_log.find({ flowId: "<flowId>" }).sort({ startedAt: -1 }).limit(5)
   ```
2. Check if the step's vendor API is returning persistent errors
3. Check if the flow has a data issue (missing required fields)

### Replay
```javascript
// Reset a single flow for re-processing
db.dis_instrument_flows.updateOne(
  { _id: "<flowId>" },
  {
    $set: {
      status: "IN_PROGRESS",
      recoveryCount: 0,
      errorMessage: null,
      claimedBy: null,
      claimedAt: null,
      updatedAt: ISODate("2020-01-01T00:00:00Z")  // old timestamp triggers recovery scanner
    }
  }
)
```

The recovery scanner picks it up within `scanIntervalMs` (default 30s) and re-publishes the step command. The `completedSteps` set ensures already-completed steps are skipped.

### Bulk replay
```javascript
// Replay all flows that exceeded recovery attempts
db.dis_instrument_flows.updateMany(
  { status: "FAILED", errorMessage: /max recovery attempts/ },
  {
    $set: {
      status: "IN_PROGRESS",
      recoveryCount: 0,
      errorMessage: null,
      claimedBy: null,
      claimedAt: null,
      updatedAt: ISODate("2020-01-01T00:00:00Z")
    }
  }
)
```

---

## 5. Dead Letter Topic (DLT) Accumulation

### What happens
When a step fails after exhausting all Kafka retry attempts (default 4, across retry-0/1/2 topics), the message lands in the DLT topic. The library marks the flow as `DEAD_LETTERED` in MongoDB.

### Diagnosis
```bash
# Check DLT topic offset (messages count)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group <appName>-dlt --describe

# Read DLT messages
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic dis.instrument.commands-dlt --from-beginning --max-messages 10
```

```javascript
// Flows marked as dead-lettered
db.dis_instrument_flows.find({ status: "DEAD_LETTERED" })
```

### Replay
The flow is already in MongoDB. Fix the root cause (vendor API, data issue), then replay:
```javascript
db.dis_instrument_flows.updateOne(
  { _id: "<flowId>", status: "DEAD_LETTERED" },
  {
    $set: {
      status: "IN_PROGRESS",
      retryCount: 0,
      backoffSeconds: 0,
      errorMessage: null,
      updatedAt: ISODate("2020-01-01T00:00:00Z")
    }
  }
)
```

DLT topic has short retention (default 24h) -- the messages auto-expire. MongoDB is the source of truth.

---

## 6. Gate Step Expiry (WAITING_RETRY Timed Out)

### What happens
Gate steps (e.g., `AWAIT_SIGNATURES`, `AWAIT_PREPARATION_APPROVAL`) park flows in WAITING_RETRY status. If `@Step(expiresAfter="48h")` is configured and the flow waits beyond that, the recovery scanner marks it FAILED.

### Diagnosis
```javascript
db.dis_instrument_flows.find({
  status: "FAILED",
  errorMessage: /expired/
})
```

### Replay (if business allows)
```javascript
db.dis_instrument_flows.updateOne(
  { _id: "<flowId>" },
  {
    $set: {
      status: "WAITING_RETRY",
      errorMessage: null,
      waitingSince: new Date()  // reset expiry clock
    }
  }
)
```

The waiting flow scheduler re-publishes the step command on its next cycle, which re-evaluates the gate condition.

---

## 7. Consumer Offset Lost (DC Failover)

### What happens
In a multi-DC setup, if the primary cluster dies and consumers fail over to a secondary cluster, Kafka `__consumer_offsets` may not be available.

### Auto-recovery (3 layers)
1. **MongoDB offset store**: `MongoOffsetRecoveryListener` seeks to the last offset stored in `orchestrator_consumer_offsets`
2. **Timestamp fallback**: if MongoDB offset not found, `TimestampOffsetRecoveryListener` seeks to `now - offsetFallbackHours` (default 24h)
3. **Idempotency guards**: any duplicate messages are caught by `ProcessedEvent` + `completedSteps`

### Configuration
```yaml
orchestrator:
  recovery:
    offset-store: MONGO          # MONGO (cross-DC) or KAFKA (single-cluster)
    offset-fallback: TIMESTAMP   # TIMESTAMP, EARLIEST, or LATEST
    offset-fallback-hours: 24    # lookback window for TIMESTAMP fallback
```

### Check stored offsets
```javascript
db.orchestrator_consumer_offsets.find().sort({ _id: 1 })
// Each document: { _id: "group|topic|partition", offset: N, timestamp: ... }
```

---

## 8. Compensation / Rollback Failure

### What happens
When a step fails permanently and the library triggers compensation (reverse steps), if the compensation handler itself fails, the flow enters `COMPENSATION_FAILED` status.

### Diagnosis
```javascript
db.dis_instrument_flows.find({ status: "COMPENSATION_FAILED" })
// Check compensationError field for details
```

### Manual resolution
Compensation failure requires manual intervention because the system is in a partially-compensated state. Steps:
1. Read the `compensationError` and `completedSteps` to understand what was rolled back
2. Manually roll back remaining steps via vendor API or database
3. Once consistent, update the flow:
```javascript
db.dis_instrument_flows.updateOne(
  { _id: "<flowId>" },
  { $set: { status: "FAILED", compensationError: null } }
)
```

---

## 9. Outbox Poison Event

### What happens
If an outbox event is permanently unpublishable (e.g., message too large, topic deleted), it hits `maxPublishRetries` (default 5) and is dead-lettered in MongoDB. The rest of the outbox pipeline continues -- one bad event doesn't freeze everything.

### Diagnosis
```javascript
db.orchestrator_outbox.find({ deadLettered: true })
```

### Fix and retry
```javascript
// After fixing the root cause (e.g., topic recreated)
db.orchestrator_outbox.updateOne(
  { _id: "<eventId>" },
  { $set: { deadLettered: false, failureCount: 0 }, $unset: { publishedAt: "" } }
)
```

---

## Full Replay Checklist

For a major outage affecting many flows:

### 1. Assess scope
```javascript
// Count affected flows by status
db.dis_instrument_flows.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```

### 2. Release orphaned claims
```javascript
db.dis_instrument_flows.updateMany(
  { claimedBy: { $ne: null } },
  { $set: { claimedBy: null, claimedAt: null } }
)
```

### 3. Replay FAILED flows
```javascript
db.dis_instrument_flows.updateMany(
  { status: "FAILED" },
  {
    $set: {
      status: "IN_PROGRESS",
      retryCount: 0,
      backoffSeconds: 0,
      recoveryCount: 0,
      errorMessage: null,
      claimedBy: null,
      claimedAt: null,
      updatedAt: ISODate("2020-01-01T00:00:00Z")
    }
  }
)
```

### 4. Replay DEAD_LETTERED flows
```javascript
db.dis_instrument_flows.updateMany(
  { status: "DEAD_LETTERED" },
  {
    $set: {
      status: "IN_PROGRESS",
      retryCount: 0,
      backoffSeconds: 0,
      recoveryCount: 0,
      errorMessage: null,
      updatedAt: ISODate("2020-01-01T00:00:00Z")
    }
  }
)
```

### 5. Clear stuck outbox
```javascript
// Re-enable dead-lettered outbox events
db.orchestrator_outbox.updateMany(
  { deadLettered: true },
  { $set: { deadLettered: false, failureCount: 0 }, $unset: { publishedAt: "" } }
)
```

### 6. Monitor recovery
```javascript
// Watch flows drain over time
db.dis_instrument_flows.countDocuments({ status: "IN_PROGRESS" })
db.dis_instrument_flows.countDocuments({ status: "COMPLETED" })
```

### Important: Idempotency Safety

Replaying is safe because:
- **completedSteps**: already-completed steps are skipped (set check, not re-executed)
- **ProcessedEvent**: duplicate Kafka events are caught and ignored
- **CAS advancement**: `updateFirst` with version check prevents concurrent step execution

However, verify that **vendor APIs are idempotent** for the affected steps. If a step calls an external API that is NOT idempotent (e.g., creating a payment), you must check the vendor state before replaying.

---

## Configuration Reference

```yaml
orchestrator:
  recovery:
    scan-interval-ms: 30000           # how often to scan for stuck flows
    stale-threshold-minutes: 15       # how long IN_PROGRESS before recovery
    max-recovery-attempts: 10         # cap before marking FAILED
    batch-size: 100                   # flows per claim batch
    claim-ttl-minutes: 5             # orphan claim release threshold
    offset-store: MONGO              # MONGO or KAFKA
    offset-fallback: TIMESTAMP       # TIMESTAMP, EARLIEST, LATEST
    offset-fallback-hours: 24        # lookback for TIMESTAMP fallback
  retry:
    max-attempts: 4                  # Kafka retry attempts
    initial-interval-ms: 2000        # first retry delay
    multiplier: 2.0                  # backoff multiplier
    max-interval-ms: 30000           # max retry delay
    jitter-factor: 0.5               # randomization
  outbox:
    poll-interval-ms: 500            # outbox publish poll
    max-publish-retries: 5           # before dead-lettering
  retention:
    outbox-days: 7                   # TTL for published outbox events
    processed-events-days: 30        # TTL for idempotency records
    step-log-days: 90                # TTL for step audit logs
    dlt-retention-hours: 24          # Kafka DLT topic retention
```
