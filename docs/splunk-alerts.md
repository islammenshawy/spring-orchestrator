# Splunk Alerts — Digital Instrument Service (DIS)

Production monitoring alerts grouped by category. All searches scope to
`index=dis source=digital-instrument-service` unless noted.

> **High-volume normalization**: Use `stats count by` with 1-5 min windows
> to avoid alert fatigue. Most alerts trigger on threshold count, not
> individual events.

---

## 1. Flow Failures & Compensation

### 1.1 Flow Dead-Lettered (DLT) — CRITICAL
Flows that exhausted all Kafka retry attempts. Requires manual investigation.
```
[DLT] Dead letter
| stats count by flowId, stepName, flowType
| where count > 0
```
**Threshold**: Any occurrence
**Action**: PagerDuty, investigate flow in MongoDB, check vendor availability
**Runbook**: Check vendor health → replay flow if transient → escalate if persistent

### 1.2 Compensation Failed — CRITICAL
Compensation handler threw during undo. Flow stuck in COMPENSATION_FAILED.
```
"Compensation failed for step" OR status=COMPENSATION_FAILED
| stats count by flowId, stepName
```
**Threshold**: Any occurrence
**Action**: PagerDuty, manual intervention required
**Runbook**: Check vendor state → retry compensation via API → manual rollback if needed

### 1.3 Flow Failed — HIGH
Flow failed permanently (NonRetryableStepException). Compensation ran (or skipped).
```
"[Saga] Flow * completed" NOT | "[DLT]" OR ("status" AND "FAILED")
| stats count as failures by flowType, span(_time, 5m)
| where failures > 5
```
**Threshold**: >5 failures per 5 min per flow type
**Action**: Slack #dis-alerts
**Note**: Normalize by total flow volume — 5 failures in 1000 flows ≠ 5 in 10

### 1.4 Recovery Exhaustion — HIGH
Stale flow recovery exceeded max attempts — flow marked for compensation.
```
"exceeded max recovery attempts" OR "marking COMPENSATING, running compensation"
| stats count by flowId
```
**Threshold**: Any occurrence
**Action**: Slack #dis-alerts, check if flow was stuck due to infra issue

---

## 2. Vendor (Enigio) Health

### 2.1 Vendor Outage — CRITICAL
Step retries accumulating = vendor returning errors consistently.
```
"[Outbox] Failed to publish" OR "RetryableStepException" OR "RETRYING"
| stats count as retries by span(_time, 1m)
| where retries > 50
```
**Threshold**: >50 retries/min (sustained)
**Action**: Check vendor health endpoint, circuit breaker state
**Note**: Brief spikes are normal (network blips). Alert on sustained >2 min.

### 2.2 Signing Expired — HIGH
Signatures not completed within configured expiry (default 48h).
```
"Signing expired after"
| stats count by flowId, traceOriginalId
```
**Threshold**: Any occurrence
**Action**: Notify operations team, check signer email delivery

### 2.3 Transfer Rejected — HIGH
Recipient rejected the document transfer.
```
"Transfer REJECTED by recipient" OR "TRANSFER_REJECTED"
| stats count by flowId, transferId
```
**Threshold**: Any occurrence
**Action**: Notify business team — may need manual follow-up with recipient

### 2.4 Transfer Cannot Cancel — CRITICAL
Recipient already opened envelope — document possession transferred, cannot void.
```
"CANNOT CANCEL — recipient already opened"
| stats count by flowId
```
**Threshold**: Any occurrence
**Action**: Escalate to legal/compliance — irrecoverable state

### 2.5 Webhook Registration Failed — MEDIUM
Webhook couldn't be registered with vendor. Polling fallback active.
```
"Failed to register webhook" AND "polling fallback"
| stats count by span(_time, 5m)
| where count > 3
```
**Threshold**: >3 per 5 min
**Action**: Check vendor webhook endpoint, verify callback URL reachable

### 2.6 Document Invalidation Failed — HIGH
Cancel handler couldn't invalidate document on vendor.
```
"Failed to invalidate document" OR "Failed to invalidate envelope"
| stats count by flowId, traceOriginalId
```
**Threshold**: Any occurrence
**Action**: Manual invalidation may be needed via vendor portal

---

## 3. Kafka & Messaging

### 3.1 Outbox Dead-Lettered — CRITICAL
Outbox event failed to publish after max retries (poison event or topic issue).
```
"[Outbox] Dead-lettering event"
| stats count by flowId, topic
```
**Threshold**: Any occurrence
**Action**: Check Kafka topic health, verify topic exists, inspect event payload

### 3.2 Outbox Pipeline Blocked — HIGH
Outbox batch wait timeout — Kafka may be unreachable.
```
"[Outbox] Batch wait interrupted or timed out"
| stats count by span(_time, 1m)
| where count > 5
```
**Threshold**: >5 per min
**Action**: Check Kafka broker health, network connectivity

### 3.3 Reply Publish Failed — HIGH
Step completed but reply couldn't be sent. Outbox fallback used.
```
"Reply publish failed" OR "Reply outbox fallback also failed"
| stats count by flowId, stepName
```
**Threshold**: >0 for "also failed" (complete delivery failure)
**Action**: Check Kafka reply topic, outbox health

### 3.4 Kafka Consumer Deserialization Error — MEDIUM
Malformed Kafka message — likely schema mismatch or corruption.
```
"Deserialization failed"
| stats count by topic, span(_time, 5m)
| where count > 3
```
**Threshold**: >3 per 5 min
**Action**: Check producer version compatibility, message format

### 3.5 Consumer Lag — HIGH
Consumer falling behind message production.
```
index=kafka topic=dis.instrument.commands
| stats max(lag) as max_lag by consumer_group
| where max_lag > 100
```
**Threshold**: Lag >100 sustained >5 min
**Action**: Check DIS pod count, increase concurrency or pods

---

## 4. DC Failover

### 4.1 Failover Triggered — CRITICAL
Active DC switched — all in-flight flows disrupted.
```
"[Failover] Successfully failed over" OR "State: FAILING_OVER"
| stats count by activeDc, standbyDc
```
**Threshold**: Any occurrence
**Action**: Verify new DC is healthy, check MirrorMaker replication status
**Note**: Expected during maintenance windows — suppress during planned failovers

### 4.2 Both DCs Unhealthy — CRITICAL
Neither DC is reachable — system cannot process flows.
```
"[Failover] Both DCs unhealthy"
| stats count by span(_time, 1m)
```
**Threshold**: Any occurrence
**Action**: PagerDuty — immediate infrastructure investigation

### 4.3 DC Degraded — MEDIUM
Probe failures accumulating, approaching failover threshold.
```
"State: HEALTHY → DEGRADED"
| stats count by span(_time, 5m)
| where count > 0
```
**Threshold**: Any occurrence
**Action**: Check failing DC's Kafka brokers, network

### 4.4 Failover Failed — CRITICAL
Failover was attempted but threw an exception.
```
"[Failover] Failover failed! Staying on"
| stats count by activeDc
```
**Threshold**: Any occurrence
**Action**: Manual intervention — check standby DC health, container startup

### 4.5 Offset Recovery — MEDIUM
Consumer offsets being recovered from MongoDB after DC switch.
```
"[OffsetRecovery]" AND ("MongoDB offset" OR "ahead of Kafka" OR "seeked to")
| stats count by topic, partition, span(_time, 5m)
```
**Threshold**: Informational during failover, alert if occurs without failover
**Action**: Verify no message loss — check processed events vs expected

---

## 5. MongoDB & Persistence

### 5.1 MongoDB Unreachable — CRITICAL
Startup or runtime MongoDB connectivity failure.
```
"MongoDB NOT reachable" OR "MongoTimeoutException" OR "MongoSocketException"
| stats count by span(_time, 1m)
| where count > 3
```
**Threshold**: >3 per min
**Action**: Check MongoDB cluster health, connection pool, network

### 5.2 Version Conflict Exhausted — HIGH
Optimistic lock retry failed after 3 attempts — using full partial update fallback.
```
"Version conflict persisted after 3 attempts"
| stats count by flowId, span(_time, 5m)
| where count > 0
```
**Threshold**: Any occurrence (rare in normal operation)
**Action**: Check for concurrent signal storms or excessive consumer rebalancing

### 5.3 Full Partial Update Failed — CRITICAL
Even the fallback $set update failed — flow state may be inconsistent.
```
"Full partial update also failed"
| stats count by flowId
```
**Threshold**: Any occurrence
**Action**: Manual flow state inspection in MongoDB

### 5.4 Outbox Write Failed — CRITICAL
Failed to write outbox event — flow started but first step may not execute.
```
"Failed to write outbox event"
| stats count by flowId
```
**Threshold**: Any occurrence
**Action**: Check MongoDB write concern, disk space, connection pool

### 5.5 Orphaned Claims — MEDIUM
Recovery scanner found claimed flows with expired TTL — pod crashed during recovery.
```
"Released * orphaned claims"
| rex field=_raw "Released (?<count>\d+) orphaned"
| where count > 0
```
**Threshold**: >0
**Action**: Informational — verify pods are healthy, check for recurring crashes

---

## 6. Step Execution

### 6.1 Step Claim Blocked — MEDIUM
Execution claim failed — another consumer is processing the same step (rebalance).
```
"already claimed on flow" AND "skipping (rebalance duplicate)"
| stats count by stepName, span(_time, 5m)
| where count > 10
```
**Threshold**: >10 per 5 min (excessive rebalancing)
**Action**: Check consumer group stability, session.timeout.ms, pod restarts

### 6.2 Infrastructure Error in Step — MEDIUM
Non-business exception during step execution (MongoDB down, network, etc.).
```
"Infrastructure error in step"
| stats count by stepName, span(_time, 5m)
| where count > 10
```
**Threshold**: >10 per 5 min
**Action**: Check MongoDB and Kafka health

### 6.3 Step Timeout — MEDIUM
Step execution exceeded configured timeout.
```
"timed out after" AND "RetryableStepException"
| stats count by stepName, span(_time, 5m)
| where count > 5
```
**Threshold**: >5 per 5 min
**Action**: Check vendor response times, increase step timeout if vendor is slow

### 6.4 Cancel Handler Failed — MEDIUM
Cancel handler threw during flow cancellation.
```
"Cancel handler failed for step"
| stats count by flowId, stepName
```
**Threshold**: Any occurrence
**Action**: Check vendor state — document may need manual invalidation

---

## 7. Signals

### 7.1 Signal Handler Failed — MEDIUM
Pending signal execution threw an exception.
```
"[Signal] Pending * failed on flow"
| stats count by signalName, flowId
```
**Threshold**: >5 per 5 min
**Action**: Check signal handler logic, payload format

### 7.2 Signal Dropped — LOW
Signal couldn't be delivered — flow changed state between read and execute.
```
"signal * dropped"
| stats count by flowId, signalName, span(_time, 5m)
```
**Threshold**: >10 per 5 min (informational)
**Action**: Usually benign — signal arrived too late. Investigate if persistent.

---

## 8. Notifications

### 8.1 Gate Notification Failed — MEDIUM
Failed to publish phase-complete notification to downstream consumers.
```
"Failed to publish notification"
| stats count by flowId, span(_time, 5m)
| where count > 5
```
**Threshold**: >5 per 5 min
**Action**: Check Kafka notifications topic, downstream consumer health

---

## 9. Dashboard Queries (Non-Alerting)

### 9.1 Flow Throughput
```
"[Saga] Flow * completed" OR "[Saga] Executing step"
| timechart count by flowType span=1m
```

### 9.2 Step Latency p95
```
"orchestrator.step.executions"
| stats perc95(durationMs) as p95_ms by stepName, flowType
```

### 9.3 Active Flows by Status
```
index=dis sourcetype=mongodb collection=dis_instrument_flows
| stats count by status
```

### 9.4 Vendor API Response Times
```
source=mock-vendor OR source=enigio-api
| timechart avg(response_time_ms) by endpoint span=1m
```

### 9.5 Failover State Timeline
```
"[Failover] State:" OR "[Failover] Successfully"
| timechart count by supervisorState span=1m
```

---

## Alert Priority Matrix

| Priority | Response Time | Channel | Examples |
|----------|-------------|---------|----------|
| CRITICAL | 5 min | PagerDuty | DLT, both DCs down, compensation failed, MongoDB down |
| HIGH | 30 min | Slack #dis-alerts | Vendor outage, signing expired, consumer lag |
| MEDIUM | Next business day | Slack #dis-monitoring | Webhook failures, claims, infra errors |
| LOW | Weekly review | Dashboard | Signal drops, informational |

## Volume Normalization

In high-volume environments, use rate-based thresholds instead of absolute counts:
```
| stats count as events, dc(flowId) as flows by span(_time, 5m)
| eval error_rate = events / flows * 100
| where error_rate > 5
```
This alerts when >5% of flows hit errors, regardless of total volume.
