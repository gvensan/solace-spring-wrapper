# Solace Java API Spring Wrapper

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.x-green.svg)](https://spring.io/projects/spring-boot)

Spring-friendly wrapper for the Solace Java API. Provides auto-configuration, annotations, and a resilient publisher that supports direct and persistent delivery, async usage, and rich message properties.

## Features
- Auto-configured Solace `MessagingService` and JSON serializer (Jackson).
- Publisher with retry/reinit, backpressure control, persistent receipts (best-effort), and async helpers with timeouts/confirm.
- `@SolacePublish` and `@SolaceConsumer` annotations with SpEL support for conditional publishing and property enrichment.
- Direct and persistent consumption with optional queue auto-create, manual/auto ack, and local backoff.
- Optional per-consumer/per-publisher isolation via `solace.isolateConsumers` and `solace.isolatePublishers`.
- Message property support: correlation ID, reply-to, TTL, priority, app message type/id, eliding eligible, class of service, sender ID, sequence number, HTTP content type/encoding, delivery mode, message expiration, user properties, and persistent-only fields (TTL, expiration, ack-immediately, DMQ-eligible).
- Configurable publisher executor sizing and direct publisher backpressure.

## Requirements
- JDK 18+
- Maven
- Solace broker access (host/VPN/credentials)
- `com.solace:solace-messaging-client` 1.8.0+ (managed via BOM here)

## Install
Maven dependency:
```
<dependency>
  <groupId>com.solace.wrapper</groupId>
  <artifactId>solace-java-spring-wrapper</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Configure (application.yml)
```
solace:
  host: "tcp://localhost:55555"
  msgVpn: "default"
  clientUsername: "default"
  clientPassword: "secret"
  clientName: "solace-spring-wrapper"
  isolateConsumers: false
  isolatePublishers: false
  direct-publisher-backpressure: WAIT   # or REJECT
  direct-publisher-backpressure-wait-ms: 10
  publisher-executor-core-size: 2
  publisher-executor-max-size: 8
  publisher-executor-queue-capacity: 1000
  # Resource limits and timeouts
  max-consumer-connections: 50          # 0 = unlimited
  max-publisher-connections: 50         # 0 = unlimited
  termination-timeout-ms: 5000          # Graceful shutdown timeout
  pending-confirm-timeout-ms: 30000     # Cleanup stale confirmations
```
TLS/mTLS and OAuth2 settings are supported; see `SolaceProperties` for the full list.
Isolation effects:
- When `isolateConsumers=true`, each consumer gets its own `MessagingService`.
- When `isolatePublishers=true`, each publisher key gets its own `MessagingService`.
- Setting a per-annotation `clientName` forces a dedicated service even if isolation is off.

## Usage
### Direct publish
```
solacePublisher.publishToTopic("orders/new", order);
```

### Persistent publish with confirm (best-effort)
```
solacePublisher.publishPersistentToTopicAsyncConfirm("orders/new", order)
    .orTimeout(10, TimeUnit.SECONDS)
    .join();
```

### Publish with properties
```
MessageProperties props = new MessageProperties()
    .setCorrelationId("cid-123")
    .setReplyTo("reply/topic")
    .setDeliveryMode("DIRECT")   // or PERSISTENT (hint)
    .setTimeToLive(5000)
    .setApplicationMessageType("order")
    .addUserProperty("region", "us-east");

solacePublisher.publishToTopicWithProperties("orders/new", order, props);
```
Persistent-only fields (`persistentTimeToLive`, `persistentExpiration`, `persistentAckImmediately`, `persistentDmqEligible`) are applied when using `publishPersistent...WithProperties`.

**MessageProperties fields supported**
- Transport: `timeToLive`, `priority`, `deliveryMode` (DIRECT|PERSISTENT hint), `messageExpiration` (epoch ms)
- Routing/headers: `correlationId`, `replyTo`, `applicationMessageType`, `applicationMessageId`, `senderId`, `sequenceNumber`, `classOfService`, `elidingEligible`
- HTTP interop: `httpContentType`, `httpContentEncoding`
- Persistent-only: `persistentTimeToLive`, `persistentExpiration`, `persistentAckImmediately`, `persistentDmqEligible`
- Custom: `userProperties` map

### Async helpers
- `publishToTopicAsync(...)`
- `publishPersistentToTopicAsync(...)`
- `publishPersistentToTopicAsyncConfirm(...)` (completes on broker receipt when supported; completes immediately otherwise)
- `publishPersistentToTopicAwait(...)` (blocks with timeout)

### Annotations
Enable in Boot by default; in plain Spring:
```
@Configuration
@EnableSolaceAnnotations
class SolaceConfig {}
```
Publish:
```
@SolacePublish(destination = "orders/new", deliveryMode = "PERSISTENT", condition = "#result != null")
public Order createOrder(OrderRequest req) { ... }
```
Consume:
```
class OrderHandlers {
  @SolaceConsumer(queue = "q/orders", maxRetries = 3, consumerIdPrefix = "order-service")
  void onOrder(Order order) { ... }
}
```
Set `autoStart=false` to register without starting and call `SolaceConsumerManager.startConsumer("id")` later.

Client name override (per consumer/publisher):
```
@SolaceConsumer(queue = "q/orders", consumerId = "orders-consumer", clientName = "orders-consumer-client")
void onOrder(Order order) { ... }

@SolacePublish(destination = "orders/new", clientName = "orders-publisher-client")
public Order createOrder(OrderRequest req) { ... }
```

Manual ack:
```
class OrderHandlers {
  @SolaceConsumer(queue = "q/orders", ackMode = SolaceConsumer.AckMode.MANUAL, consumerIdPrefix = "order-service")
  void onOrder(Order order, SolaceAckContext ack) {
    try {
      // process
      ack.ack();
    } catch (Exception e) {
      ack.fail();
    }
  }
}
```

### SpEL Expression Variables

The `@SolacePublish` annotation supports SpEL expressions in most String attributes. The following variables are available:

| Variable | Description | Example |
|----------|-------------|---------|
| `result` | The **return value** of the method | `#{result.orderId}` |
| `#p0`, `#p1`, ... | Method **parameters** by index | `#{#p0}` (first param) |
| `#paramName` | Method parameter by **name** (requires `-parameters` compiler flag) | `#{#orderId}` |
| `T(ClassName)` | Access static methods/fields | `#{T(System).currentTimeMillis()}` |

**When to use `result` vs parameters:**

Use `result` when the return value contains the data you need (most common):
```java
@SolacePublish(
    destination = "orders/processing/#{result.orderType == 'VIP' ? 'vip' : 'standard'}",
    correlationId = "#{result.orderId}"
)
public Order sendToProcessing(Order order) {
    order.setStatus("QUEUED");
    return order;  // <-- This becomes 'result'
}
```

Use parameters (`#p0`, `#p1`) when you need input values that differ from the return type:
```java
@SolacePublish(
    destination = "orders/status/#{#p0}",     // #p0 = orderId
    condition = "#{#p2 != #p1}"               // Only publish if status changed
)
public StatusUpdate updateStatus(String orderId, String oldStatus, String newStatus) {
    return new StatusUpdate(orderId, oldStatus, newStatus);
}
```

**SpEL in userProperties:**
```java
@SolacePublish(
    destination = "orders/created",
    userProperties = {
        "orderType=#{result.orderType}",
        "timestamp=#{T(System).currentTimeMillis()}",
        "priority=#{result.amount > 1000 ? 'HIGH' : 'NORMAL'}"
    }
)
public Order createOrder(OrderRequest req) { ... }
```

### Registry & lifecycle
Use `autoStart=false` for delayed startup and control consumers via the manager or registry.
```
@SolaceConsumer(queue = "q.orders", consumerId = "orders-consumer", autoStart = false)
void onOrder(Order order) { ... }

@Component
class ConsumerLifecycle {
  private final SolaceConsumerManager manager;
  private final SolaceConsumerRegistry registry;

  ConsumerLifecycle(SolaceConsumerManager manager, SolaceConsumerRegistry registry) {
    this.manager = manager;
    this.registry = registry;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void start() {
    manager.startConsumer("orders-consumer");
  }

  public void stop() {
    manager.stopConsumer("orders-consumer");
  }

  public void status() {
    registry.getConsumerStatuses().forEach((id, status) ->
        System.out.println(id + " -> " + status));
  }
}
```
Note: `stopConsumer(...)` stops without unregistering; `removeConsumer(...)` shuts down and removes.

## Error handling and retries
- Publisher retries once after reinitializing the underlying publisher (direct and persistent) and propagates `SolacePublishException` on failure.
- Persistent publish receipts: uses `MessagePublishReceiptListener` when available; otherwise completes confirms immediately (best-effort).
- Consumer handlers support max retries/backoff (annotation processor) and manual ack/nack for persistent deliveries.

## Serialization
- Default `JsonMessageSerializer` (Jackson). Supply a custom `MessageSerializer` bean to override.
- `messageSerializer.serialize(...)` is used for payloads; property-aware builder paths use `serializeToBytes(...)` to preserve applied headers.

## Lifecycle
- Publisher resources are initialized at bean startup (`@PostConstruct`) and terminated on shutdown (`@PreDestroy`), including the executor.
- Consumer manager and connection manager also shut down resources on `@PreDestroy`.

## Annotation Reference
- `docs/ANNOTATIONS.md` lists annotation parameters, defaults, and usage patterns.

## Examples
- `example-usage/` shows annotated publishers/consumers.
- `example-usage/README.md` documents the expanded, multi-service workflow.
- `solace-samples-java/` contains upstream Solace samples (reference only).

## Testing
- Unit tests: `mvn test`
- Integration (requires broker): `mvn -Pintegration verify` with broker details in `src/test/resources/test-broker.properties`.
- Broker E2E (real broker): `mvn -Pbroker-it verify` with broker details in `src/test/resources/test-broker.properties`.
- Integration tests skip automatically if the broker is unreachable.

## Build and run locally
### Build
- Build the jar: `mvn clean package`
- Run unit tests only: `mvn test`
- Run unit + integration tests: `mvn -Pintegration verify`

### Run locally
- Configure broker settings in `src/test/resources/test-broker.properties` (or your app `application.yml`).
- Start your broker (e.g., local PubSub+ on `tcp://localhost:55554`).
- Run a demo app:
  - Annotation demo: enable the `CommandLineRunner` in `example-usage/AnnotationDemoApplication.java` and run the Spring Boot app.
  - Programmatic demo: see `example-usage/ExampleService.java` for setup/usage patterns.
- Manual ack usage: see `SolaceAckContext` and the expanded workflow in `example-usage/ExpandedOrderWorkflow.java`.

## Notes and limits
- Queue auto-create requires broker permissions (endpoint create/modify, topic subscribe).
- Queue auto-create only runs when the Solace client supports `MissingResourcesCreationStrategy` (client >= 1.8.2). If it is unavailable, queues must be pre-provisioned.
- Poison/DMQ handling is broker-side; configure DMQ/DLQ and reject-to-DMQ on queues.
- Some optional transport settings apply only when supported by the Solace client version; DEBUG logs show what was applied.
- When `solace.isolateConsumers` or `solace.isolatePublishers` is enabled, reconnect retries are forced on (for stability).

## Roadmap
- Metrics (Micrometer) for publish/consume/ack/nack.
- Additional examples and cookbook content.
- Expanded configuration docs and TLS walkthroughs.
- Additional configuration coverage and TLS examples.
