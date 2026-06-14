# Example Usage — Order Fulfillment Pipeline

This module is a single, cohesive Spring Boot application that demonstrates **every supported feature**
of the Solace Spring Wrapper in the context of one realistic use case: an **e-commerce order
fulfillment pipeline**.

```
OrderService ──orders/created──▶ InventoryService ──inventory/reserve──▶ (reserve, MANUAL ack + backoff)
     │                                   │
     │                                   └──billing/charge──▶ BillingService ──billing/charged──▶ NotificationService
     └──orders/status/*───────────────────────────────────────────────────────────────────────▶ (deferred consumer)

QuoteClient ──request/reply──▶ PricingService     (native @SolaceReplier + SolaceRequestor)
ProgrammaticOrderGateway                          (programmatic SolacePublisher / SolaceConsumerManager)
```

## Project Structure

```
example-usage/src/main/java/com/example/myapp/
├── ExampleApplication.java            # @EnableSolaceAnnotations + CommandLineRunner driver
├── model/                             # Shared Jackson POJO events/DTOs
├── order/OrderService.java           # @SolacePublish — create / cancel / status (full attribute surface)
├── inventory/InventoryService.java   # @SolaceConsumer DIRECT + persistent MANUAL-ack, chained publish
├── billing/BillingService.java       # @SolaceConsumer persistent queue + broker retries, chained publish
├── notifications/NotificationService.java  # @SolaceConsumer condition filter + wildcard + autoStart=false
├── pricing/PricingService.java       # @SolaceReplier  (native request-reply server)
├── pricing/QuoteClient.java          # SolaceRequestor (native request-reply client, sync + async)
├── observability/OrderMetrics.java   # SolaceMetrics + custom business meters
└── programmatic/ProgrammaticOrderGateway.java  # Programmatic API alternative (namespaced)
```

## Running the Example

### Prerequisites
1. **Java 17+**
2. A **Solace broker** running locally:
   ```bash
   docker run -d -p 55555:55555 -p 8080:8080 --name solace solace/solace-pubsub-standard
   ```

### Steps
```bash
# 1. Build & install the parent wrapper (from the project root)
cd /path/to/solace-spring-wrapper
mvn clean install -DskipTests

# 2. Run the example app
cd example-usage
mvn spring-boot:run
```

The driver walks through six stages (create orders → status ticks → request-reply pricing →
conditional cancellation → programmatic gateway → metrics snapshot). Watch the logs for
`@SolacePublish` / `@SolaceConsumer` / `@SolaceReplier` activity, then scrape metrics at
`http://localhost:8081/actuator/prometheus`.

## Feature Coverage Matrix

Every annotation feature is exercised by runnable code. The table maps each feature to where it lives.

### `@SolacePublish`
| Attribute | Demonstrated in |
|-----------|-----------------|
| `destination` (static + SpEL) | all publishers; `orders/status/#{#orderId}` in `OrderService.changeStatus` |
| `async` | `OrderService.createOrder` |
| `correlationId` | every publisher |
| `replyTo` | (native request-reply is preferred — see `@SolaceReplier`/`SolaceRequestor`) |
| `condition` | `OrderService.cancelOrder` (named-param `#reason`) |
| `userProperties` (SpEL) | `OrderService.createOrder`, `BillingService.charge` |
| `deliveryMode` (static + SpEL) | `InventoryService` (PERSISTENT); `OrderService.createOrder` (dynamic by tier) |
| `clientName` | `OrderService.createOrder` |
| `applicationMessageId` | `OrderService.createOrder` |
| `applicationMessageType` | `OrderService.createOrder` |
| `classOfService` | `OrderService.createOrder` (`VIP ? 2 : 1`) |
| `priority` (static) | `OrderService.changeStatus` |
| `timeToLive` (static) | `OrderService.changeStatus` |
| `elidingEligible` | `OrderService.changeStatus` |
| `messageExpiration` | `OrderService.changeStatus` |
| `sequenceNumber` | `OrderService.changeStatus` |

### `@SolaceConsumer`
| Attribute | Demonstrated in |
|-----------|-----------------|
| `queue` + `topics` | `InventoryService.reserve`, `BillingService.charge`, `NotificationService.onCharged` |
| `mode = DIRECT` | `InventoryService.onOrderCreated`, `NotificationService.onStatusChange` |
| `mode = PERSISTENT` (explicit) | `InventoryService.reserve`, `BillingService.charge` |
| `ackMode = MANUAL` (`SolaceAckContext`) | `InventoryService.reserve` |
| `ackMode = AUTO` (explicit) | `BillingService.charge` |
| `autoCreateQueue` | `InventoryService.reserve` |
| `condition` (SpEL on `message`) | `NotificationService.onCharged` |
| topic **wildcards** | `NotificationService.onStatusChange` (`orders/status/*`) |
| `consumerId` / `consumerIdPrefix` | all consumers |
| `clientName` | `BillingService.charge` |
| `messageType` (explicit FQN) | `BillingService.charge` |
| `autoStart = false` (+ manual start) | `NotificationService.onStatusChange` (started via `SolaceConsumerManager`) |
| `maxRetries` / `retryDelay` (broker redelivery) | `BillingService.charge` |
| `localMaxAttempts` / `localBackoff*` | `InventoryService.reserve` |
| chained `@SolaceConsumer` + `@SolacePublish` | `InventoryService`, `BillingService` |
| `InboundMessage` parameter | `InventoryService.reserve` |

### `@SolaceReplier` + `SolaceRequestor` (native request-reply)
| Feature | Demonstrated in |
|---------|-----------------|
| `@SolaceReplier` `topic`, `shareName`, `backpressure`, `backpressureCapacity` | `PricingService.quote` |
| `SolaceRequestor.request(...)` (sync, with timeout) | `QuoteClient.getQuote` |
| `SolaceRequestor.requestAsync(...)` (CompletableFuture) | `QuoteClient.getQuoteAsync` |

### Observability
| Feature | Demonstrated in |
|---------|-----------------|
| Auto-configured `SolaceMetrics` (publish/consume/request/reply meters) | enabled via `spring-boot-starter-actuator` |
| Custom business meters + live gauge | `OrderMetrics` (`example.orders.*`, `example.notifications.*`) |
| Prometheus scrape endpoint | `/actuator/prometheus` |

### Programmatic API
| Feature | Demonstrated in |
|---------|-----------------|
| `SolacePublisher.publishToTopic` / persistent + `MessageProperties` / async | `ProgrammaticOrderGateway` |
| `SolaceConsumerManager.createConsumer` + lifecycle cleanup | `ProgrammaticOrderGateway` |

## SpEL Reference

**Publish attributes** use Spring's **template** syntax — wrap expressions in `#{ ... }`:

| Variable | Description | Example |
|----------|-------------|---------|
| `result` | Method return value | `#{result.orderId}` |
| `#p0`, `#p1`, … | Parameters by index | `#{#p0.orderId}` |
| `#paramName` | Parameter by name (needs `-parameters`, enabled in this pom) | `#{#orderId}` |
| `T(Class)` | Static method/field access | `#{T(System).currentTimeMillis()}` |

**Consumer `condition`** is evaluated as a **raw** SpEL expression — **no** `#{ ... }` wrapper. The
deserialized payload is the root object (and the `#message` variable); the raw message is
`#originalMessage`:

| Form | Example |
|------|---------|
| Root-object property | `status == 'CHARGED'` |
| `#message` variable | `#message.status == 'CHARGED' or #message.status == 'DECLINED'` |
| Raw message access | `#originalMessage.getProperty('orderType') == 'VIP'` |

> ⚠️ A common gotcha: using publish-style `#{...}` in a consumer `condition` throws a SpEL parse
> error (`EL1043E`). Keep consumer conditions unwrapped.

> ℹ️ `classOfService` must evaluate to **0–2** (the Solace client rejects higher values; the publisher
> logs a warning and drops the property rather than failing the publish).

## Configuration
Edit `src/main/resources/application.yml`:
- `solace.host` — broker address (default `tcp://localhost:55555`)
- `solace.msg-vpn` / `solace.client-username` / `solace.client-password` — connection + credentials
- `solace.metrics.enabled` — toggle wrapper metrics (default `true`)
- `management.endpoints.web.exposure.include` — actuator endpoints exposed
