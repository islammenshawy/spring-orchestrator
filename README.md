# Spring Orchestrator

Resilient multi-step workflow orchestration for Spring Boot, backed by MongoDB and Kafka.

## Project Structure

```
spring-orchestrator/
│
├── orchestrator-starter/            ← THE LIBRARY (publish this)
│   └── README.md                      Full documentation
│
├── digital-instrument-service/      ← PRODUCTION: Enigio trace:original integration
│   └── 11-step flow, Feign clients, 4 event-driven gates
│
├── mock-vendor/                     ← Enigio API simulator for testing
│
├── examples/
│   ├── sample-app/                  ← Minimal usage example
│   └── dashboard/                   ← Monitoring web UI
│
├── alternative-frameworks/          ← Reference implementations (comparison only)
│   ├── saga-outbox/
│   ├── statemachine/
│   ├── spring-integration/
│   └── common/
│
├── infra/                           ← Docker Compose, Dockerfile, nginx, alerts
├── scripts/                         ← Soak test scripts
└── docs/                            ← API specs, integration guide
```

Each production module (`orchestrator-starter`, `digital-instrument-service`, `mock-vendor`) is **fully self-contained** — its own pom with `spring-boot-starter-parent`, all dependencies declared. Can be extracted to standalone repos.

## For Teams: Using the Library

Add the dependency and define your flow:

```xml
<dependency>
    <groupId>com.enigio.orchestrator</groupId>
    <artifactId>orchestrator-starter</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

```java
@Document(collection = "order_flows")
public class MyFlow extends AbstractFlow {
    private String orderId;
    private String paymentId;
}

@Component
@Flow
public class MyVendorFlow extends FlowDefinition<MyFlow> {

    @Step(order = 1, completedWhen = "orderId != null")
    public void createOrder(MyFlow flow) {
        flow.setOrderId(vendorClient.createOrder(...).getId());
        checkpoint(flow); // persist after vendor API call
    }

    @Step(order = 2, completedWhen = "paymentId != null")
    public void processPayment(MyFlow flow) {
        flow.setPaymentId(vendorClient.charge(...).getId());
        checkpoint(flow);
    }
}
```

See [orchestrator-starter/README.md](orchestrator-starter/README.md) for full documentation.

## Building

```bash
# Production modules (library + DIS + mock-vendor):
mvn package

# Everything including examples + alternative frameworks:
mvn package -P all

# Single module standalone:
cd orchestrator-starter && mvn package
```

## What the Library Provides

- **Transactional outbox** — atomic MongoDB write + Kafka publish
- **Kafka retry topics** — jittered exponential backoff (configurable ladder)
- **Two-layer idempotency** — consumer + handler level dedup
- **Reply mode** — decoupled command/reply topics for non-blocking orchestration
- **Gate steps** — `WaitingStepException` parks flows in MongoDB, exits Kafka entirely; re-activated by API, webhook, or scheduler
- **Annotation-driven error handling** — `@RetryOn`, `@RecoverOn`, `@FailOn`
- **Saga compensation** — `@Compensate` for reverse execution on failure
- **Flow cancellation** — `@OnCancel` with reverse handler execution
- **Parallel + join** — `@Parallel` + `@JoinOn` for concurrent step execution
- **Execution lanes** — `orchestrator.lanes.{lane}.{topics,concurrency}`: dedicated listener containers + consumer groups per lane, so batch work can never starve interactive flows
- **Step timeout overrides** — global → per-flow (`flows.{name}.step-timeout-seconds`) → per-step (`@Step(timeoutSeconds=…)`), so long sweeps aren't dead-lettered by the bounded retry ladder
- **checkpoint()** — mid-step persistence for crash safety after vendor API calls
- **Stale flow recovery** — container crash safety net
- **Auto-generated repositories** — no boilerplate Spring Data interfaces needed

## Requirements

- Java 21+
- Spring Boot 4.0+
- MongoDB 7.0+
- Kafka 3.x+
