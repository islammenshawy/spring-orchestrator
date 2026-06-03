# Kafka Topics — Architecture & Sequence Diagrams

## Topic Overview

| Topic | Purpose | Producers | Consumers | Partitions |
|---|---|---|---|---|
| `dis.instrument.commands` | Step execution commands | Outbox publisher, advanceToNextStep | Command consumer (`-executor` group) | 6 |
| `dis.instrument.commands.replies` | Step completion replies | Command consumer (after step succeeds) | Reply consumer (`-orchestrator` group) | 6 |
| `dis.instrument.commands-retry-0..N` | Retry backoff | Spring Kafka RetryTopic | Command consumer (auto-routed) | 6 |
| `dis.instrument.commands-dlt` | Dead letter (exhausted retries) | Spring Kafka DLT publisher | DLT consumer (`-dlt` group) | 1 |
| `dis.instrument.notifications` | Phase-complete notifications | DIS (gate steps) | Downstream systems | 6 |

## Topic Sequence Diagrams

### 1. Happy Path — Command + Reply Pattern

```
Client                 DIS API          MongoDB        Outbox         Kafka commands    Command Consumer    Kafka replies    Reply Consumer
  │                      │                │              │                │                  │                  │                │
  │── POST /flows ──────>│                │              │                │                  │                  │                │
  │                      │── save flow ──>│              │                │                  │                  │                │
  │                      │── write ──────>│── outbox ───>│                │                  │                  │                │
  │<── 200 {id} ─────────│                │              │                │                  │                  │                │
  │                      │                │              │                │                  │                  │                │
  │                      │                │              │── publish ────>│                  │                  │                │
  │                      │                │              │                │── step command ─>│                  │                │
  │                      │                │              │                │                  │── execute step   │                │
  │                      │                │              │                │                  │── save flow ────>│                │
  │                      │                │              │                │                  │── publish reply ────────────────>│
  │                      │                │              │                │                  │                  │                │── advanceAfterReply
  │                      │                │              │                │                  │                  │                │── CAS currentStep
  │                      │                │              │                │<── next step cmd─│──────────────────│────────────────│
  │                      │                │              │                │── step command ─>│                  │                │
  │                      │                │              │                │                  │   ... repeats    │                │
```

### 2. Why Command + Reply Separation?

```
Without reply topic (inline mode):
  Command Consumer: execute step → save flow → advance to next step → publish next command
  Problem: If crash between save and advance, flow is stuck (step completed but not advanced)

With reply topic:
  Command Consumer: execute step → save flow → publish reply (includes flow snapshot)
  Reply Consumer: receive reply → CAS advance → publish next command
  Benefit: Reply carries flow snapshot, so reply consumer doesn't re-read MongoDB (no stale data)
           CAS on currentStep prevents duplicate advancement from retried replies
```

### 3. Retry Topics — Non-Blocking Retry

```
Command Consumer          Retry-0 Topic       Retry-1 Topic       DLT Topic          DLT Consumer
      │                        │                    │                  │                    │
      │── step throws ────────>│                    │                  │                    │
      │   RetryableException   │                    │                  │                    │
      │                        │── after 1s ───────>│                  │                    │
      │                        │   (backoff)        │                  │                    │
      │<── retry ──────────────│                    │                  │                    │
      │── still fails ────────>│                    │                  │                    │
      │                        │── after 2s ───────>│                  │                    │
      │                        │                    │── after 4s ─────>│                    │
      │                        │                    │                  │── dead letter ────>│
      │                        │                    │                  │                    │── markDeadLettered
      │                        │                    │                  │                    │── runCompensation
```

**Key:** Spring Kafka creates retry topics automatically from `RetryTopicConfiguration`.
Each retry topic has its own backoff delay. After `max-attempts` exhausted → DLT.

### 4. Outbox Pattern — Guaranteed Delivery

```
Step Handler         MongoDB (flow + outbox)    Outbox Publisher      Kafka
      │                      │                       │                 │
      │── checkpoint(flow) ─>│                       │                 │
      │── return (success)   │                       │                 │
      │                      │                       │                 │
      │   completeStep:      │                       │                 │
      │── save flow ────────>│                       │                 │
      │                      │                       │                 │
      │   advanceToNextStep: │                       │                 │
      │── direct publish ───────────────────────────────────────────>│  (fast path)
      │   ON FAILURE:        │                       │                 │
      │── write outbox ─────>│                       │                 │
      │                      │── poll (500ms) ──────>│                 │
      │                      │                       │── publish ─────>│  (guaranteed path)
      │                      │                       │── mark done ───>│
```

**Key:** Direct Kafka publish is tried first (low latency). On failure, outbox event is written.
The outbox publisher polls every 500ms and publishes pending events.
Poison events (invalid topic, serialization error) are dead-lettered after 5 attempts.

### 5. Signal Flow — Pending Signals

```
HTTP Signal Request     MongoDB                  Step Execution        Next Step
       │                   │                          │                    │
       │                   │                          │                    │
  If flow IN_PROGRESS:     │                          │                    │
       │── $push signal ──>│ (with version inc)       │                    │
       │                   │                          │                    │
       │                   │   drainPendingSignals:   │                    │
       │                   │<── findById (read) ──────│                    │
       │                   │                          │── process signals  │
       │                   │                          │── set null in mem  │
       │                   │                          │                    │
       │                   │   completeStep:          │                    │
       │                   │<── save (clears signals)─│                    │
       │                   │                          │── advance ────────>│
       │                   │                          │                    │
  If flow PARKED:          │                          │                    │
       │── execute handler─│                          │                    │
       │── save flow ─────>│                          │                    │
       │── re-publish step────────────────────────────────────────────────>│
```

### 6. Notification Topic

```
Gate Step (AWAIT_PREPARATION_APPROVAL)    Notification Topic    Downstream System    DIS API
       │                                       │                      │                │
       │── notifyPhaseComplete ───────────────>│                      │                │
       │── park flow (PARKED)                  │                      │                │
       │                                       │── consume ──────────>│                │
       │                                       │                      │── POST /approve─>│
       │                                       │                      │                │── re-publish step
       │<── step re-executed ──────────────────│──────────────────────│────────────────│
       │── condition met → advance             │                      │                │
```

### 7. Multi-DC Failover — Topic Mapping

#### PREFIXED Mode (recommended)
```
DC-A Kafka                    MirrorMaker 2                DC-B Kafka
  │                                │                          │
  dis.instrument.commands ────────>│── dc-a.dis.instrument.commands
  dis.instrument.commands.replies─>│── dc-a.dis.instrument.commands.replies
  │                                │                          │
  DIS subscribes to BOTH:         │                          │
  - dis.instrument.commands       │                          │
  - dc-a.dis.instrument.commands  │                          │
  - dc-b.dis.instrument.commands  │                          │
```

#### IDENTITY Mode
```
DC-A Kafka                    MirrorMaker 2                DC-B Kafka
  │                                │                          │
  dis.instrument.commands ────────>│── dis.instrument.commands (same name)
  │                                │                          │
  DIS subscribes to:              │                          │
  - dis.instrument.commands       │                          │
  │                                │                          │
  On failover: consumer switches bootstrap to DC-B
  Same topic name, different cluster
```

## Consumer Groups

| Group | Listens To | Purpose |
|---|---|---|
| `{app}-executor` | commands + retry topics | Step execution (blocking) |
| `{app}-orchestrator` | commands.replies | Step advancement (fast CAS) |
| `{app}-dlt` | commands-dlt + replies-dlt | Dead letter processing + compensation |

## Configuration

```yaml
orchestrator:
  kafka:
    command-topic: dis.instrument.commands      # Auto-creates reply + DLT topics
    partitions: 6                                # Per topic
    # reply-topic: auto → {command-topic}.replies
    # Set reply-topic: "" to disable reply mode (inline advancement)
  retry:
    max-attempts: 10                             # Before DLT
    initial-interval-ms: 3000                    # First retry delay
    multiplier: 2.0                              # Exponential backoff
    max-interval-ms: 10000                       # Cap
    jitter-factor: 0.5                           # Randomize ±50%

spring.kafka.listener.concurrency: 2             # Threads per consumer
```
