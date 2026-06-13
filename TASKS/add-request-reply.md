# Add Request-Reply Support (Requestor + Replier)

## Goal
Implement first-class **request-reply** messaging in the wrapper, exposed through annotations and a
programmatic API that are consistent with the existing `@SolacePublish` / `@SolaceConsumer` /
`SolacePublisher` / `SolaceConsumerManager` design. The feature must be additive and backward
compatible — no changes to the semantics of existing publish/consume paths.

## Background (verified against `com.solace:solace-messaging-client` 1.8.2)
The Solace Java API has **native** request-reply support — correlation and the reply-to inbox are
handled by the broker/SDK automatically; do **not** hand-roll a `correlationId → CompletableFuture`
registry.

Entry point: `MessagingService.requestReply()` → `RequestReplyMessagingService`, which exposes:
- `createRequestReplyMessagePublisherBuilder()` → `RequestReplyMessagePublisher`
- `createRequestReplyMessageReceiverBuilder()` → `RequestReplyMessageReceiver`

Requestor (client) — `RequestReplyMessagePublisher`:
- `InboundMessage publishAwaitResponse(OutboundMessage, Topic, long timeoutMs)` — blocking.
- `void publish(OutboundMessage, ReplyMessageHandler, [Object userContext,] Topic, long timeoutMs)` —
  async; `ReplyMessageHandler.onMessage(InboundMessage, Object userContext, PubSubPlusClientException)`.

Replier (service) — `RequestReplyMessageReceiver`:
- `receiveAsync(RequestMessageHandler)` where `RequestMessageHandler.onMessage(InboundMessage request, Replier replier)`.
- `Replier.reply(OutboundMessage)` / `reply(OutboundMessage, Properties)` — correlation is automatic.
- Builder: `build(TopicSubscription)`, plus `onReplierBackPressureElastic/Wait(int)/Reject(int)`.
- `setReplyFailureListener(ReplyFailureListener)`.

Caveat: this is **direct (topic-based)** messaging (`Topic` for requests, `TopicSubscription` for the
replier). There is no queue/guaranteed request-reply in this API surface — scope the feature to
direct mode and document it.

Important API constraint: `Replier.reply(...)` must be called **before** the `RequestMessageHandler`
returns (it throws `IllegalStateException` otherwise). A synchronous replier method that returns the
reply value satisfies this naturally.

## Scope / Deliverables

Implement in phases; each phase must keep `mvn clean verify` green before moving on.

### Phase 1 — Replier side (`@SolaceReplier`)
- New annotation `com.solace.wrapper.annotation.SolaceReplier` with attributes consistent with
  `@SolaceConsumer` where sensible:
  - `topic` (String, required, SpEL-enabled) — request topic subscription.
  - `timeoutMs` (long, default from config) — replier receive timeout where applicable.
  - `clientName` (String, default "", SpEL-enabled) — optional dedicated connection.
  - `messageType` (String, default "") — explicit request type override (else inferred from the
    first non-`InboundMessage` parameter).
  - `consumerId` / `consumerIdPrefix` (String) — id control, mirroring `@SolaceConsumer`.
  - `autoStart` (boolean, default true).
  - `shareName` (String, default "") — optional shared-subscription name.
  - backpressure controls consistent with the direct-publisher backpressure config.
- The **method return value is serialized and sent as the reply**. A `void` return (or `null`) means
  "no reply" (and should be logged/metered). Support an optional raw `InboundMessage` parameter for
  header access, mirroring `@SolaceConsumer`.
- New `SolaceReplierProcessor` (a `BeanPostProcessor`, modeled on `SolaceConsumerProcessor`) that
  discovers `@SolaceReplier` methods, builds a `RequestReplyMessageReceiver` via the connection
  manager's pooled `MessagingService`, deserializes the request, invokes the method, serializes the
  return value, and calls `Replier.reply(...)`. Reuse `SpelExpressionResolver` for `topic`/`clientName`.
- Lifecycle: start on context refresh (respect `autoStart`), terminate cleanly on `@PreDestroy`.
- Error handling: if the replier method throws, v1 sends no reply (requestor will time out), logs at
  error, and increments a failure metric. Wire `setReplyFailureListener` to log/metric reply failures.

### Phase 2 — Requestor side (`SolaceRequestor`)
- New `SolaceRequestor` service (modeled on `SolacePublisher`) that caches a
  `RequestReplyMessagePublisher` per publisher key via `SolaceConnectionManager.createPublisherService`.
- Public API:
  - `<R> R request(String topic, Object payload, Class<R> replyType, Duration timeout)` — blocking;
    deserializes the reply to `replyType`.
  - `<R> CompletableFuture<R> requestAsync(String topic, Object payload, Class<R> replyType, Duration timeout)`.
  - Overloads accepting `MessageProperties` for the request.
- Translate SDK timeouts/errors into a dedicated `SolaceRequestException` /
  `SolaceRequestTimeoutException` (new, under `com.solace.wrapper.exception`).
- Auto-configure a `SolaceRequestor` bean in `SolaceAutoConfiguration`.

### Phase 3 — Observability + docs
- Emit metrics consistent with the existing `SolaceMetrics` facade (if present on the branch):
  request latency timer, request success/timeout/failure counters, replier success/failure counters.
  Keep it null-safe/optional exactly like the publish/consume metrics.
- README "Request-Reply" section with annotation + programmatic examples and a sequence overview;
  add `docs/REQUEST-REPLY.md`; reference it from `docs/ARCHITECTURE.md`.

### Phase 4 (optional) — Declarative client (`@SolaceRequestClient`)
- A Feign/`@HttpExchange`-style interface: `@SolaceRequestClient` on an interface, `@SolaceRequest`
  on its methods (return type = reply, single arg = request, `CompletableFuture<R>` supported).
- A `FactoryBean`/registrar that creates dynamic proxies delegating to `SolaceRequestor`.
- Treat as optional; only do it once Phases 1–3 are solid.

## Configuration
Add properties under `solace.request-reply` (new fields on `SolaceProperties` or a dedicated
`@ConfigurationProperties`), e.g.:
- `default-timeout-ms` (default 5000)
- replier backpressure strategy + capacity
- requestor executor sizing (mirror publisher executor properties)

## Testing Requirements
- **Unit tests** (no broker) using the existing dynamic-proxy mocking patterns
  (`SolacePublisherTest`, `SolaceConsumerTest`, `ConsumerAnnotationProcessorTest`,
  `SolaceConsumerProcessorManualAckTest` are good templates) covering:
  - Replier processor: discovery, request deserialization, return-value reply, `void`/`null` =
    no-reply, raw `InboundMessage` injection, exception → no reply + failure path, SpEL topic/clientName.
  - Requestor: blocking + async happy path, timeout → exception/exceptional future, error mapping,
    publisher-key/clientName routing, `MessageProperties` overloads.
  - Config defaults and auto-configuration wiring.
- **Integration tests** against a real broker (a `*BrokerIT` or `*BrokerOptionalIT`, gated by
  `TestBroker.assumeAvailable()` and reading `src/test/resources/test-broker.properties`): an
  end-to-end round trip — an annotated `@SolaceReplier` answering a `SolaceRequestor.request(...)`,
  asserting the reply payload; plus a timeout case against an unanswered topic.

## Success Criteria
- `mvn clean verify` passes cleanly with **zero new warnings**.
- `mvn clean verify -Pintegration` and `mvn clean verify -Pbroker-it` pass against a local broker
  (note: this environment's broker runs on `tcp://localhost:55554`, already set in
  `src/test/resources/test-broker.properties`).
- JaCoCo coverage for the new request-reply classes is in line with the rest of the project
  (aim ≥ 85% on the new code; overall main-source coverage must not regress).
- No breaking changes to existing public APIs; existing tests remain green.
- Good Javadoc/comments on all new public types; README + docs updated.
- New code follows Spring Boot conventions and the project's resilience/observability focus.

## Project Rules (reference CLAUDE.md)
- Spring Boot 3+, annotation-driven first; prefer declarative over programmatic where it reads well.
- Use standard Spring patterns (`@ConditionalOnMissingBean`, auto-config imports, `BeanPostProcessor`).
- SLF4J logging; DEBUG for detailed Solace interactions.
- Do NOT break the existing public API. Keep publisher/consumer/replier layers resilient and observable.

## Execution Mode (autonomous)
- Work fully autonomously. Do NOT ask questions. If a decision is ambiguous, choose the most
  sensible default consistent with this file and the existing code, note the assumption in the
  commit message, and keep going.
- Only stop when all Success Criteria above are met (or a hard external blocker — e.g. broker
  unreachable for the IT — which should be reported, not paused on).
- Default decisions already settled (do not re-litigate): timeout default 5000ms; direct-mode only;
  replier error handling = send no reply (requestor times out) + log + metric; phasing 1→2→3, with
  Phase 4 optional; branch `feature/request-reply`; commit per phase.

## Working Notes
- Branch off `main` (e.g. `feature/request-reply`); commit per phase with clear `feat:` / `test:` /
  `docs:` messages; open a PR when Phases 1–3 are complete and green.
- Reuse, don't duplicate: `SolaceConnectionManager` pooling, `MessageSerializer`,
  `SpelExpressionResolver`, the metrics facade, and the existing test mocking harnesses.
