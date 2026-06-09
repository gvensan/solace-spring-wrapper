# Add Micrometer Metrics Support

## Goal
Implement comprehensive observability using Micrometer (standard in Spring Boot).

## Requirements
- Add dependency management for `micrometer-core` and integration with Spring Boot Actuator.
- Key metrics to expose:
  - Publish success/failure counters (with tags: deliveryMode, destination, clientName)
  - Consume success/failure/retry counters (tags: queue/topic, consumerId)
  - Publish/Consume latency timers (distribution summaries)
  - Connection status / active publishers-consumers gauges
  - Backpressure / rejection counters
- Auto-configure a `MeterRegistry` bean if not present.
- Add `@Observed` style support or simple annotation-based metric emission where sensible.
- Expose via Actuator endpoints by default.
- Update README with configuration examples and common queries (Prometheus/Grafana).

## Success Criteria
- `mvn clean compile test` passes cleanly.
- Integration tests still work and new metrics appear.
- README + docs updated.
- No breaking changes to existing APIs.