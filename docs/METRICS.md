# Metrics & Observability

The Solace Spring Wrapper is instrumented with [Micrometer](https://micrometer.io). Metrics are
emitted through a central, null-safe facade (`SolaceMetrics`) and registered with whatever
`MeterRegistry` your application provides. Because the wrapper depends on
`spring-boot-starter-actuator`, in a Spring Boot application the meters are automatically wired to
the Boot-managed registry and exposed through Actuator endpoints.

## How it is wired

- `SolaceMetricsAutoConfiguration` is registered in
  `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- It binds `solace.metrics.*` properties (`SolaceMetricsProperties`).
- It creates a fallback `SimpleMeterRegistry` **only if** no `MeterRegistry` bean already exists
  (`@ConditionalOnMissingBean`). Under Spring Boot Actuator the Boot registry is reused instead.
- It creates a `SolaceMetrics` bean which is injected into `SolacePublisher` and
  `SolaceConsumerManager` (which propagates it to every `SolaceConsumer` it creates) via optional
  setter injection — so the existing public constructors are unchanged and metrics are fully
  optional.
- A `SolaceMetricsGaugeBinder` registers the connection/active-endpoint gauges after the context's
  singletons are instantiated, using lazy providers evaluated at scrape time.

Everything degrades to a no-op when metrics are disabled or when a component is constructed directly
without a registry (e.g. in unit tests).

### Annotation-based publishing is instrumented automatically

`@SolacePublish`-annotated methods publish through `SolacePublisher`, so they flow through the same
instrumented path and emit `solace.publish.*` metrics with no extra annotations required. Likewise,
`@SolaceConsumer` handlers run inside `SolaceConsumer`, which emits `solace.consume.*` metrics. This
gives `@Observed`-style coverage of the annotation programming model without a separate aspect.

## Meters

| Meter | Type | Tags | Meaning |
|-------|------|------|---------|
| `solace.publish.total` | counter | `outcome`, `deliveryMode`, `destination`, `clientName` | Publish attempts by outcome (`success`/`failure`) |
| `solace.publish.latency` | timer | `outcome`, `deliveryMode`, `destination`, `clientName` | Publish latency (percentile histogram enabled) |
| `solace.publish.backpressure.rejected` | counter | `deliveryMode`, `destination`, `clientName` | Publishes rejected due to backpressure / buffer overflow |
| `solace.consume.total` | counter | `outcome`, `destination`, `consumerId` | Messages consumed by outcome (`success`/`failure`) |
| `solace.consume.latency` | timer | `outcome`, `destination`, `consumerId` | Message processing latency (percentile histogram enabled) |
| `solace.consume.retries.total` | counter | `destination`, `consumerId` | Local backoff re-attempts |
| `solace.connection.up` | gauge | – | `1` when the primary connection is up, else `0` |
| `solace.publishers.active` | gauge | – | Active underlying publisher instances (direct + persistent) |
| `solace.consumers.active` | gauge | – | Running consumers |

Notes:
- `outcome` is `success` or `failure`.
- `deliveryMode` is `DIRECT` or `PERSISTENT`.
- `destination` is the topic (publish) or queue / comma-joined topics (consume). Null/empty values
  are reported as `unknown`.
- Timers register a percentile histogram, so Prometheus exposes `_bucket` series suitable for
  `histogram_quantile(...)`.

## Configuration

```yaml
solace:
  metrics:
    enabled: true                 # master switch (default true)
    include-destination-tag: true # include the destination tag (default true)
```

Disabling `include-destination-tag` is recommended when you publish/consume across a very large or
unbounded set of destinations, to keep meter cardinality bounded.

### Exposing through Actuator

Spring Boot only exposes the `health` endpoint over HTTP by default. To scrape metrics, expose the
`metrics` and/or `prometheus` endpoints:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus
```

The `metrics` endpoint (`/actuator/metrics/solace.publish.total`, etc.) is available with just
`micrometer-core` (a transitive dependency of actuator). For `/actuator/prometheus`, add the
Prometheus registry to your application — it is declared here as an **optional** dependency, so it is
not forced onto consumers:

```xml
<dependency>
  <groupId>io.micrometer</groupId>
  <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

## Programmatic access

The `SolaceMetrics` bean can be injected if you want to add your own meters with consistent naming,
or you can use the Micrometer `MeterRegistry` directly:

```java
@Autowired
SolaceMetrics solaceMetrics;       // null-safe facade

@Autowired
MeterRegistry meterRegistry;       // standard Micrometer registry
```

## Example queries (PromQL)

```promql
# Publish success ratio over 5m
sum(rate(solace_publish_total{outcome="success"}[5m]))
  / sum(rate(solace_publish_total[5m]))

# p99 publish latency (seconds), by delivery mode
histogram_quantile(0.99, sum(rate(solace_publish_latency_seconds_bucket[5m])) by (le, deliveryMode))

# Consume failures per second, by queue
sum(rate(solace_consume_total{outcome="failure"}[5m])) by (destination)

# Local retries per second
sum(rate(solace_consume_retries_total[5m])) by (consumerId)

# Backpressure rejections per second
sum(rate(solace_publish_backpressure_rejected_total[5m]))

# Active endpoints
solace_publishers_active
solace_consumers_active

# Alert: connection down
solace_connection_up == 0
```

> Micrometer/Prometheus rewrites meter names: dots become underscores, counters gain a `_total`
> suffix, and timers expose `_seconds`, `_seconds_count`, `_seconds_sum`, and `_seconds_bucket`
> series. The PromQL above reflects those transformed names.

## Grafana

Useful starter panels:
- **Connection status** — stat panel on `solace_connection_up` (map `1`→UP, `0`→DOWN).
- **Throughput** — time series of `sum(rate(solace_publish_total[1m]))` and
  `sum(rate(solace_consume_total[1m]))`.
- **Latency heatmap** — from `solace_publish_latency_seconds_bucket`.
- **Active endpoints** — `solace_publishers_active` and `solace_consumers_active`.
- **Errors** — `sum(rate(solace_consume_total{outcome="failure"}[5m])) by (destination)` and
  `solace_publish_backpressure_rejected_total`.
