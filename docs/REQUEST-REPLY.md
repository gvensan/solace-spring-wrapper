# Request-Reply

The wrapper implements the request-reply pattern on top of the Solace Java API's **native**
request-reply support. Correlation and the reply-to inbox are handled by the broker/SDK — the wrapper
does not maintain a correlation registry. Request-reply uses **direct (topic-based)** messaging.

There are two sides:
- **Replier** (service): the `@SolaceReplier` annotation — the method's return value is the reply.
- **Requestor** (client): the injectable `SolaceRequestor` service — blocking and async.

## Replier — `@SolaceReplier`

```java
@Component
class PricingService {

    @SolaceReplier(topic = "pricing/quote/v1")
    public Quote handle(QuoteRequest req) {
        return priceEngine.quote(req);     // serialized and sent back as the reply
    }
}
```

Behavior:
- The request payload is deserialized to the method's (first non-`InboundMessage`) parameter type.
- The **return value is serialized and sent as the reply**. A `void` method, or one returning
  `null`, sends **no reply** — the requestor observes a timeout.
- If the method throws, **no reply** is sent (requestor times out); the error is logged and metered.
- An optional `com.solace.messaging.receiver.InboundMessage` parameter exposes the raw request
  (headers/properties), mirroring `@SolaceConsumer`.
- **Fail-fast startup:** if an `autoStart` replier cannot start (e.g. the broker is unreachable at
  boot), context creation fails rather than booting silently with no live listener. Use
  `autoStart = false` to register without starting and start later via the processor.

Attributes:

| Attribute | Default | Meaning |
|-----------|---------|---------|
| `topic` | (required) | Request topic subscription (SpEL, wildcards allowed) |
| `shareName` | `""` | Shared-subscription name to load-balance across replier instances (SpEL) |
| `messageType` | `""` | Explicit request type; otherwise inferred from the first non-`InboundMessage` parameter |
| `replierId` / `replierIdPrefix` | `""` | Replier id control |
| `clientName` | `""` | Dedicated connection (SpEL) |
| `autoStart` | `true` | Start on context refresh |
| `backpressure` | `ELASTIC` | `ELASTIC` / `WAIT` / `REJECT` for the reply buffer |
| `backpressureCapacity` | `1024` | Capacity for `WAIT` / `REJECT` |

> Note: the SDK `Replier` must reply **before** the request handler returns; a synchronous
> `@SolaceReplier` method that returns the reply satisfies this automatically.

## Requestor — `SolaceRequestor`

The `SolaceRequestor` bean is auto-configured. Inject it and call:

```java
@Autowired SolaceRequestor requestor;

// Blocking
Quote q = requestor.request("pricing/quote/v1", req, Quote.class, Duration.ofSeconds(5));

// Blocking, default timeout (solace.request-reply.default-timeout-ms)
Quote q2 = requestor.request("pricing/quote/v1", req, Quote.class);

// Async
CompletableFuture<Quote> f = requestor.requestAsync("pricing/quote/v1", req, Quote.class, Duration.ofSeconds(5));
```

Semantics:
- The request payload is serialized with the configured `MessageSerializer`; the reply is
  deserialized to `replyType`.
- A missing reply within the timeout throws `SolaceRequestTimeoutException` (blocking) or completes
  the future exceptionally with it (async). `SolaceRequestTimeoutException extends SolaceRequestException`.
- Any other failure maps to `SolaceRequestException`.
- Requests are routed through a single pooled request-reply publisher (reusing the wrapper's
  connection pooling).
- **Self-healing publisher:** on a recoverable (non-timeout) send failure the cached request-reply
  publisher is terminated and rebuilt, and the request is retried once — so a single poisoned
  publisher cannot permanently strand subsequent calls. Timeouts do **not** trigger a rebuild (they
  mean no replier answered, which a rebuild would not fix).

## Configuration

```yaml
solace:
  request-reply:
    default-timeout-ms: 5000   # used by the no-timeout request overloads
```

## Metrics

When metrics are enabled (a `MeterRegistry` is present — see `docs/METRICS.md`), request-reply emits:

| Meter | Type | Tags |
|-------|------|------|
| `solace.request.total` | counter | `outcome`, `destination` |
| `solace.request.latency` | timer | `outcome`, `destination` |
| `solace.request.timeouts.total` | counter | `destination` |
| `solace.reply.total` | counter | `outcome`, `destination`, `replierId` |

`solace.reply.total`'s `outcome` distinguishes `success` (reply delivered), `no_reply` (handled but
void/null result — the requestor will time out), and `failure` (the handler threw, or the broker
reported a reply-delivery failure). This lets dashboards separate intentional no-reply from real
failures rather than counting every handler invocation as a success.

Everything degrades to a no-op when metrics are disabled.

## How it works

- Requestor: `MessagingService.requestReply().createRequestReplyMessagePublisherBuilder().build()`;
  `publishAwaitResponse(...)` (blocking) or `publish(..., ReplyMessageHandler, ...)` (async).
- Replier: `MessagingService.requestReply().createRequestReplyMessageReceiverBuilder().build(TopicSubscription)`;
  `receiveAsync(RequestMessageHandler)` and `Replier.reply(OutboundMessage)`.

## Limitations

- Direct-mode only (no queue/guaranteed request-reply in this API surface) — appropriate since
  replies are inherently transient.
- Async replier methods (returning `CompletableFuture`) are not first-class: the reply must be sent
  within the handler invocation, so a synchronous return value is the supported model.
- A single shared requestor publisher is used (no per-`clientName` requestor isolation yet).
