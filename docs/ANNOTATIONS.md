# Annotations Reference

This document summarizes the Solace wrapper annotations, their parameters, defaults, and typical usage patterns.

## General notes

- SpEL context variables:
  - `#param0`, `#param1`, ... for method arguments (also `#p0`/`#p1`, and named parameters such as
    `#orderId` when compiled with `-parameters`).
  - `#result` for the method return value (only for `@SolacePublish`).
  - `#message` for the message object in `@SolaceConsumer` conditions (also the root object), and
    `#originalMessage` for the raw `InboundMessage`.
- SpEL syntax differs by annotation:
  - `@SolacePublish` attributes use **template** syntax — wrap expressions in `#{ ... }`
    (e.g. `correlationId = "#{result.orderId}"`).
  - `@SolaceConsumer` `condition` is a **raw** SpEL expression — **no** `#{ ... }` wrapper
    (e.g. `condition = "#message.status == 'CHARGED'"`). Using `#{...}` there throws a SpEL parse
    error (`EL1043E`).
- `@SolacePublish` publishes to topics. For queues, use `SolacePublisher` directly.
- Queue auto-create requires broker permissions and a client version that supports `MissingResourcesCreationStrategy`.
- If `solace.isolateConsumers` or `solace.isolatePublishers` is enabled, reconnect retries are forced on for stability.

## @EnableSolaceAnnotations

Enables annotation processing for `@SolacePublish` and `@SolaceConsumer`.

Usage:
```
@Configuration
@EnableSolaceAnnotations
class SolaceConfig {}
```

## @SolaceConsumer (method-level)

Defines a message consumer. Works with direct (topic) or persistent (queue) messaging.

Core routing:
- `queue` (String, default `""`): Queue name for persistent mode.
- `topics` (String[], default `{}`): Topics to subscribe to.
- `mode` (AUTO|PERSISTENT|DIRECT, default AUTO): AUTO selects PERSISTENT if queue set, DIRECT if only topics.
- `autoCreateQueue` (boolean, default `true`): Auto-create queue when supported by client.
- `consumerId` (String, default `""`): Optional explicit consumer ID.
- `consumerIdPrefix` (String, default `""`): Optional prefix for generated consumer IDs.
- `clientName` (String, default `""`): Optional per-consumer client name override.

Ack and retry:
- `ackMode` (AUTO|MANUAL, default AUTO): MANUAL requires `SolaceAckContext` in handler.
- `maxRetries` (int, default 3): Handler retry count (annotation layer).
- `retryDelay` (long, default 1000): Delay between retries.
- `localMaxAttempts` (int, default 1): Local backoff attempts before nAck or fail.
- `localBackoffInitialMs` (long, default 200)
- `localBackoffMultiplier` (double, default 2.0)
- `localBackoffMaxMs` (long, default 2000)

Other:
- `messageType` (String, default `""`): Fully-qualified class name override.
- `condition` (String, default `""`): SpEL expression to filter messages.
- `autoStart` (boolean, default `true`): When false, the consumer is registered but not started.

Handler signatures:
- Standard: `(T payload, InboundMessage inbound)`
- Manual ack: `(T payload, InboundMessage inbound, SolaceAckContext ack)`

Manual ack example:
```
@SolaceConsumer(queue = "q.orders", ackMode = SolaceConsumer.AckMode.MANUAL)
void onOrder(Order order, InboundMessage inbound, SolaceAckContext ack) {
  if (valid(order)) {
    ack.ack();
  } else {
    ack.fail();
  }
}
```

Explicit start example:
```
@SolaceConsumer(queue = "q.orders", consumerId = "orders-consumer", autoStart = false)
void onOrder(Order order) { ... }

@Component
class ConsumerStartup {
  private final SolaceConsumerManager consumerManager;
  ConsumerStartup(SolaceConsumerManager consumerManager) { this.consumerManager = consumerManager; }

  @EventListener(ApplicationReadyEvent.class)
  public void start() {
    consumerManager.startConsumer("orders-consumer");
  }
}
```

Client name override:
```
@SolaceConsumer(queue = "q.orders", consumerId = "orders-consumer", clientName = "orders-consumer-client")
void onOrder(Order order) { ... }
```
When `clientName` is set, the consumer uses a dedicated `MessagingService` even if isolation is disabled.

Direct mode example (topic subscription):
```
@SolaceConsumer(topics = {"orders/>"}, mode = SolaceConsumer.MessagingMode.DIRECT)
void onOrderEvent(OrderEvent evt) { ... }
```

Persistent mode with topic subscriptions (queue + topics):
```
@SolaceConsumer(queue = "orders-queue", topics = {"orders/created"}, mode = SolaceConsumer.MessagingMode.PERSISTENT)
void onOrder(OrderCreated evt) { ... }
```

## @SolacePublish (method-level)

Publishes the method return value to a topic. Supports SpEL in most fields.

Destination and behavior:
- `destination` (String, required): Topic destination (SpEL supported).
- `clientName` (String, default `""`): Optional per-publisher client name override (SpEL supported).
- `async` (boolean, default `false`): Publish asynchronously.
- `condition` (String, default `""`): SpEL condition to publish.

Message properties:
- `correlationId` (String)
- `replyTo` (String)
- `timeToLive` (long, default -1)
- `priority` (int, default -1)
- `applicationMessageType` (String)
- `applicationMessageId` (String)
- `userProperties` (String[] of `key=value` pairs)
- `elidingEligible` (String, SpEL -> boolean)
- `classOfService` (String, SpEL -> integer 0-2)
- `deliveryMode` (String, SpEL -> `DIRECT` or `PERSISTENT`)
- `messageExpiration` (String, SpEL -> epoch ms)
- `sequenceNumber` (String, SpEL -> long)

Example:
```
@SolacePublish(
  destination = "orders/created",
  correlationId = "#{result.orderId}",
  clientName = "orders-publisher-client",
  deliveryMode = "PERSISTENT",
  userProperties = {"priority=#{result.priority}"},
  async = true
)
public OrderCreated create(OrderRequest req) { ... }
```

Notes:
- `condition` is evaluated after the method returns.
- If `clientName` is empty and no default is configured, the broker assigns an anonymous client name.
- When `clientName` is set, the publisher uses a dedicated `MessagingService` even if isolation is disabled.
