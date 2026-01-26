# Test Suite Overview

This document summarizes the test coverage for the Solace Spring Wrapper. Each entry lists what is being tested, how it is exercised, the expected outcome, and whether any real broker connectivity is involved.

## Per-class briefs

- `connection/SolaceConnectionManagerAuthTlsTest`: Validates connection manager initialization paths for TLS/auth configuration using dummy properties.
- `connection/SolaceConnectionManagerTest`: Verifies primary and isolated service creation and connection info accessors.
- `publisher/SolacePublisherTest`: Exercises direct/persistent publishing flows, backpressure configuration, and retry/reinit logic on simulated failures.
- `consumer/SolaceConsumerTest`: Checks consumer local backoff/retry handling and ack behavior with simulated handler failures.
- `consumer/SolaceConsumerManagerTest`: Ensures manager create/restart/stop/shutdown flows track consumers correctly.
- `annotation/ConsumerAnnotationProcessorTest`: Directly drives the annotation processor to confirm registration decisions (mode/ack/id/type), condition evaluation, retry counts, and inbound injection using a recording manager.
- `annotation/SolacePublishAspectTest`: Validates @SolacePublish clientName override is passed through to the publisher.
- `annotation/SolaceConsumerAnnotationTest`: Focused @SolaceConsumer behavior for mappings, message type override, id prefix, and condition evaluation via recording manager.
- `annotation/EnableSolaceAnnotationsTest`: Minimal Spring context to assert @EnableSolaceAnnotations registers the publish aspect and processor with mocked dependencies.
- `health/SolaceHealthIndicatorTest`: Health indicator UP/DOWN paths with mocked connection manager and null-safe interruption cause.
- `serialization/JsonMessageSerializerTest`: Payload and metadata round-trip serialization/deserialization.
- `spel/ComprehensiveSpelExpressionTest`: Expression parsing/evaluation utility cases.
- `spel/SpelExpressionValidationTest`: Security validation tests for dangerous SpEL patterns (Runtime, ProcessBuilder, System.exit, etc.).
- `connection/ConnectionPoolLimitsTest`: Tests for maxConsumerConnections and maxPublisherConnections enforcement.
- `publisher/PendingConfirmsCleanupTest`: Tests for pendingConfirms cleanup mechanism to prevent memory leaks.
- `integration/AnnotatedPublishIntegrationTest`: End-to-end annotated publish with a direct consumer (skips if broker unreachable).
- `integration/DirectReapplyIntegrationTest`: Direct subscription reapply after disconnect/reconnect (skips if broker unreachable).
- `integration/LocalBrokerIntegrationTest`: Direct + persistent flow and health indicator wiring against a local broker (skips if broker unreachable).
- `integration/PersistentAckModeIntegrationTest`: Manual vs auto ack behavior against a broker (skips if broker unreachable).
- `integration/AnnotatedConsumerBrokerOptionalIT`: Lightweight Spring context load to ensure annotated consumers register (skips if broker unreachable).
- `broker/DirectMessagingBrokerIT` (profile `broker-it`): Direct publish/consume against a real broker using topic-based consumers.
- `broker/HealthIndicatorBrokerIT` (profile `broker-it`): Health indicator live check against a real broker connection.
- `broker/PersistentMessagingBrokerIT` (profile `broker-it`): Persistent queue scenarios (manual ack, broker redelivery) against a real broker.

## Test matrix

| Test class | Focus / feature area | What is exercised | Expected result | Broker connection? | Notes |
| --- | --- | --- | --- | --- | --- |
| `connection/SolaceConnectionManagerAuthTlsTest` | Connection manager TLS/auth setup | Primary service init with TLS/auth properties | Builds/connects primary service without errors | No | Uses dummy properties; config-only |
| `connection/SolaceConnectionManagerTest` | Connection manager lifecycle | Primary init; isolated consumer/publisher services; info/stat accessors | Services created; stats/info returned; no exceptions | No | Simulates multiple isolated services |
| `publisher/SolacePublisherTest` | Direct/persistent publishing + retries | Backpressure config; publish with props; persistent retry/reinit on simulated failure | Direct publish succeeds; persistent retries then succeeds after reinit | No | Dummy messaging service with injected failures |
| `consumer/SolaceConsumerTest` | Consumer backoff/retry + ack handling | Local backoff (attempts/delay); handler invocation sequencing | Retries expected count; ack paths honored | No | Simulated handler failures |
| `consumer/SolaceConsumerManagerTest` | Manager lifecycle | Enhanced consumer creation, restart, start/stop all | Consumers registered/tracked; lifecycle ops succeed | No | Stub connection manager/serializer |
| `annotation/ConsumerAnnotationProcessorTest` | Annotation processor registration | Queue/topic mapping; AUTO mode; ack; consumerId prefix; clientName/autoStart flags; type inference/override; condition; retry; inbound injection | Registrations match expectations; condition filters; retry counts match; inbound injected | No | Recording manager stub; handlers invoked manually |
| `annotation/SolacePublishAspectTest` | @SolacePublish clientName override | Aspect calls publisher with resolved clientName | Publisher invoked with clientName override | No | Mockito-based unit test |
| `annotation/SolaceConsumerAnnotationTest` | @SolaceConsumer behavior | Persistent/direct mappings; ack; type override; consumerId prefix; condition evaluation | Registrations expected; condition filters messages | No | Recording manager stub |
| `annotation/EnableSolaceAnnotationsTest` | Enablement wiring | Spring context with @EnableSolaceAnnotations bean registration | Publish aspect and consumer processor beans present | No | Mockito mocks for dependencies |
| `health/SolaceHealthIndicatorTest` | Health reporting | UP/DOWN paths; connection info; service stats | Health status set with expected details | No | Mocked connection manager; null-safe cause |
| `serialization/JsonMessageSerializerTest` | Serialization | Payload/metadata round-trip | Round-trips without loss/errors | No | Pure unit |
| `spel/ComprehensiveSpelExpressionTest` | SpEL utilities | Expression parsing/evaluation cases | Expressions evaluate as expected | No | Pure unit |
| `spel/SpelExpressionValidationTest` | SpEL security | Dangerous pattern detection (Runtime, ProcessBuilder, System.exit, etc.) | Dangerous patterns rejected; safe expressions allowed | No | Pure unit |
| `connection/ConnectionPoolLimitsTest` | Connection pool limits | maxConsumerConnections and maxPublisherConnections enforcement | Limits enforced; SolaceConnectionException when exceeded | No | Uses testable subclass |
| `publisher/PendingConfirmsCleanupTest` | Memory leak prevention | pendingConfirms cleanup, stale entry removal, shutdown cleanup | Stale entries removed; futures completed exceptionally | No | Reflection-based |
| `integration/AnnotatedPublishIntegrationTest` | Annotated publish + direct consume | Direct consumer receives annotated publish | Message delivered and asserted | Yes (skips if broker unreachable) | Runs in `integration` profile |
| `integration/DirectReapplyIntegrationTest` | Direct subscription reapply | Disconnect/reconnect + consumer restart | Second message delivered | Yes (skips if broker unreachable) | Runs in `integration` profile |
| `integration/LocalBrokerIntegrationTest` | Local broker integration | Direct + persistent publish/consume + health details | Messages delivered; health details present | Yes (skips if broker unreachable) | Runs in `integration` profile |
| `integration/PersistentAckModeIntegrationTest` | Ack behavior | Manual vs auto ack redelivery | Manual redelivers, auto does not | Yes (skips if broker unreachable) | Runs in `integration` profile |
| `integration/AnnotatedConsumerBrokerOptionalIT` | Annotation integration | Spring context startup with annotated consumers | Context loads; consumers registered | Yes (skips if broker unreachable) | Not included by default profiles; run directly |
| `broker/DirectMessagingBrokerIT` | Direct messaging E2E (real broker) | Direct consumer on unique topic + publish via `SolacePublisher` | Message delivered and asserted | Yes (opt-in via `broker-it` profile) | Reads `test-broker.properties`/env for creds |
| `broker/HealthIndicatorBrokerIT` | Health indicator (real broker) | Health against live connection manager | Status UP with details | Yes (opt-in via `broker-it` profile) | Reads `test-broker.properties`/env for creds |
| `broker/PersistentMessagingBrokerIT` | Persistent messaging (real broker) | Manual ack delivery and broker redelivery on failure | Message delivered; redelivery occurs after first failure | Yes (opt-in via `broker-it` profile) | Uses unique queue+topic; auto-create configurable |
