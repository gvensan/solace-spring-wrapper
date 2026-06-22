# Samples

Small, **single-concept** sample apps for the Solace Spring Wrapper. Each folder is a complete,
runnable Spring Boot application focused on **one** annotation feature — read one file, get the idea,
copy it.

> Looking for a full, realistic app that combines everything into one pipeline? See
> [`../example-usage`](../example-usage). These samples are the bite-sized counterpart.

## The samples

| # | Folder | Concept | Key annotation(s) |
|---|--------|---------|-------------------|
| 01 | [`01-publish-basic`](01-publish-basic) | Publish a method's return value | `@SolacePublish` |
| 02 | [`02-publish-spel`](02-publish-spel) | Dynamic destination, conditional publish, named-param SpEL | `@SolacePublish` (SpEL) |
| 03 | [`03-consume-direct`](03-consume-direct) | Subscribe to a topic, best-effort delivery | `@SolaceConsumer` (DIRECT) |
| 04 | [`04-consume-queue`](04-consume-queue) | Guaranteed delivery, queue auto-create, broker retries | `@SolaceConsumer` (PERSISTENT) |
| 05 | [`05-manual-ack`](05-manual-ack) | Explicit ack/fail + local backoff retry | `@SolaceConsumer` + `SolaceAckContext` |
| 06 | [`06-request-reply`](06-request-reply) | Native request-reply (server + client) | `@SolaceReplier` + `SolaceRequestor` |

Samples 03–06 publish their own test messages and then **stay running** so the consumer/replier keeps
serving — press `Ctrl+C` to stop. Samples 01–02 publish and exit.

## Topics each sample uses

Every sample is **self-contained**: it publishes its own test traffic and consumes/serves it within
the same app, on its own topic namespace, so samples never interfere when run together.

| # | Publishes to | Consumes / serves |
|---|--------------|-------------------|
| 01 | `samples/orders/created` (persistent) | — (publish only) |
| 02 | `samples/orders/created/{standard,vip}`, `samples/orders/cancelled` (persistent) | — (publish only) |
| 03 | `samples/ordersd/created` (self-test, direct) | DIRECT sub: `samples/ordersd/created`, `samples/ordersd/created/>` |
| 04 | `samples/ordersq/created` (self-test, persistent) | queue `samples.orders.queue` ← `samples/ordersq/created`, `samples/ordersq/created/>` |
| 05 | `samples/payments/incoming` (self-test, persistent) | queue `samples.payments.queue` ← `samples/payments/incoming` |
| 06 | `samples/pricing/quote` (request) | `@SolaceReplier` on `samples/pricing/quote` |

Two things the order samples (01–04) illustrate:

- **One payload shape.** They all use `Order {orderId, customer, tier, amount}`, so the JSON
  round-trips cleanly (the wrapper's serializer rejects unknown fields, so consumer and publisher
  types must match).
- **Concrete publish / wildcard subscribe.** Consumers 03 and 04 subscribe to `.../created` **and**
  `.../created/>`; the `>` wildcard belongs only in a subscription — never publish to it. Publishers
  always send to a concrete topic.

> **Note:** the publish-only samples (01, 02) emit to `samples/orders/...`, which no consumer in this
> set subscribes to — they demonstrate publishing, not delivery. The consumer samples (03, 04) feed
> themselves on their own `ordersd` / `ordersq` namespaces.

Each sample sets a unique `client-name`, so you can run several at once without the broker rejecting
duplicate connections.

## Prerequisites

1. **Java 17+**
2. A **Solace broker**. With Docker:
   ```bash
   docker run -d -p 55555:55555 -p 8080:8080 --name solace solace/solace-pubsub-standard
   ```
3. **Install the wrapper** into your local Maven repo (from the repo root):
   ```bash
   mvn -q clean install -DskipTests
   ```

## Running a sample

Each sample is its own module, so just `cd` into one and run it:

```bash
cd samples/01-publish-basic
mvn spring-boot:run
```

The broker host defaults to `tcp://localhost:55554` (this repo's local broker). Point a sample at any
broker without editing files via the `SOLACE_HOST` env var:

```bash
SOLACE_HOST=tcp://localhost:55555 mvn spring-boot:run
```

You can also build all samples at once from this folder: `mvn -q clean package`.

## How these stay minimal

Shared build config (Java level, the wrapper + Spring Boot dependencies, the `-parameters` compiler
flag, and the Spring Boot plugin) lives in [`samples/pom.xml`](pom.xml). Each child module is therefore
just a tiny pom, one `@SpringBootApplication` file, and an `application.yml`.
