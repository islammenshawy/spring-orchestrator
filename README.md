# Spring Orchestrator

Resilient multi-step workflow orchestration for Spring Boot, backed by MongoDB and Kafka.

## Project Structure

```
spring-orchestrator/
│
├── orchestrator-starter/        ← THE LIBRARY (publish this)
│   └── README.md                  Full documentation
│
├── sample-app/                  ← USAGE EXAMPLE
│   └── EnigioDocumentFlow.java    5-step vendor integration in one class
│
├── common/                      ┐
├── saga-outbox/                 │  REFERENCE IMPLEMENTATIONS
├── statemachine/                │  (comparison only, not for production)
├── spring-integration/          │  Build with: mvn package -P demo
├── mock-vendor/                 │
└── dashboard/                   ┘  Web UI for comparing patterns
```

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
// Flow entity — just your domain fields (extends AbstractFlow)
@Document(collection = "order_flows")
public class MyFlow extends AbstractFlow {
    private String orderId;
    private String paymentId;
}

public interface MyFlowRepo extends OrchestratorFlowRepository<MyFlow> {}

// Flow logic — one class, all steps
@Component
@Flow
public class MyVendorFlow extends FlowDefinition<MyFlow> {

    @Step(order = 1, completedWhen = "orderId != null")
    public void createOrder(MyFlow flow) {
        flow.setOrderId(vendorClient.createOrder(...).getId());
    }

    @Step(order = 2, completedWhen = "paymentId != null")
    public void processPayment(MyFlow flow) {
        flow.setPaymentId(vendorClient.charge(...).getId());
    }
}
```

See [orchestrator-starter/README.md](orchestrator-starter/README.md) for full documentation.

## Building

```bash
# Library + sample app only (what you'd publish):
mvn package

# Everything including reference implementations:
mvn package -P demo
```

## What the Library Provides

- Transactional outbox (atomic MongoDB write + Kafka publish)
- Kafka retry topics with jittered exponential backoff
- Two-layer idempotency (consumer + handler level)
- Annotation-driven error handling (@RetryOn, @RecoverOn, @FailOn)
- Single-class flow definition (@Flow + @Step on methods)
- Stale flow recovery (container crash safety net)
- CooperativeStickyAssignor rebalancing (config-only, zero custom code)

## Reference Implementations (demo profile)

Three patterns were compared to arrive at the library design:

| Pattern | Module | Verdict |
|---------|--------|---------|
| Saga + Transactional Outbox | `saga-outbox/` | Most resilient (atomic outbox), most code (~900 lines) |
| Spring Statemachine | `statemachine/` | State validation from framework, but custom MongoDB persistence |
| Spring Integration | `spring-integration/` | Least code (~470 lines), declarative flow DSL |
| **orchestrator-starter** | `orchestrator-starter/` | **Best of all three, packaged as a library** |

The `dashboard/` module provides a web UI at http://localhost:8080 for comparing all patterns side-by-side with flow visualization, Kafka ladder view, and performance metrics.

## Requirements

- Java 21+
- Spring Boot 3.2+
- MongoDB
- Kafka
