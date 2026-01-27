# Solace Spring Wrapper - Test Documentation

## Overview

This project includes comprehensive unit tests and integration tests for the Solace Spring Wrapper library.

| Test Type | Count | Broker Required | Command |
|-----------|-------|-----------------|---------|
| Unit Tests | 158 | No (mocked) | `mvn test` |
| Integration Tests | 4 classes | Yes | `mvn verify -P integration` |

## Test Coverage

**Overall: 70% instruction coverage, 59% branch coverage**

| Package | Coverage | Description |
|---------|----------|-------------|
| `annotation` | 100% | `@SolaceConsumer`, `@SolacePublish` annotations |
| `annotation.processor` | 80% | Annotation processing, SpEL resolution |
| `consumer` | 75% | Consumer lifecycle, ACK modes, backoff |
| `publisher` | 75% | Direct/persistent publishing, receipts |
| `connection` | 40% | Connection pooling, isolation |
| `config` | 59% | Properties, auto-configuration |
| `serialization` | 52% | JSON message serialization |
| `health` | 100% | Health indicator for actuator |
| `exception` | 100% | Custom exception classes |

### Generate Coverage Report

```bash
mvn test jacoco:report
open target/site/jacoco/index.html
```

---

## Unit Tests

Unit tests use **mocked Solace SDK** via JDK Proxy - no broker connection required.

### Running Unit Tests

```bash
# Run all unit tests
mvn test

# Run specific test class
mvn test -Dtest=SolacePublisherTest

# Run specific test method
mvn test -Dtest=SolacePublisherTest#direct_publish_sends_message
```

### Test Structure

```
src/test/java/com/solace/wrapper/
├── annotation/
│   ├── ConsumerAnnotationProcessorTest.java    # @SolaceConsumer processing
│   ├── EnableSolaceAnnotationsTest.java        # @EnableSolace configuration
│   ├── SolaceConsumerAnnotationTest.java       # Consumer annotation validation
│   └── SolacePublishAspectTest.java            # @SolacePublish AOP aspect
├── connection/
│   ├── ConnectionPoolLimitsTest.java           # Max connection limits
│   ├── SolaceConnectionManagerTest.java        # Basic connection tests
│   ├── SolaceConnectionManagerAuthTlsTest.java # TLS/OAuth2 auth strategies
│   └── SolaceConnectionManagerComprehensiveTest.java  # 17 tests: pooling, isolation, threading
├── consumer/
│   ├── SolaceConsumerTest.java                 # Basic consumer tests
│   ├── SolaceConsumerComprehensiveTest.java    # 24 tests: modes, lifecycle, backoff
│   ├── SolaceConsumerManagerTest.java          # Manager basic tests
│   └── SolaceConsumerManagerComprehensiveTest.java  # 24 tests: registry, status
├── publisher/
│   ├── SolacePublisherTest.java                # 13 tests: direct/persistent, receipts
│   └── PendingConfirmsCleanupTest.java         # Receipt cleanup tests
├── serialization/
│   └── JsonMessageSerializerTest.java          # JSON serialization tests
├── health/
│   └── SolaceHealthIndicatorTest.java          # Actuator health tests
└── spel/
    └── SpelExpressionResolverTest.java         # SpEL expression tests
```

### Key Test Classes

#### SolacePublisherTest (13 tests)
Tests publishing functionality with mocked SDK:
- `direct_publish_sends_message` - Basic direct publish
- `persistent_publish_sends_message` - Persistent publish with receipts
- `publish_with_properties_sets_headers` - Custom message properties
- `publish_retries_on_failure` - Retry logic
- `receipt_listener_exception_completes_future_exceptionally` - Error handling

#### SolaceConsumerComprehensiveTest (24 tests)
Tests consumer behavior:
- Mode detection (DIRECT vs PERSISTENT)
- Validation (queue required for persistent)
- Lifecycle (start/stop/restart)
- Manual ACK support
- Local backoff retry mechanism

#### SolaceConnectionManagerComprehensiveTest (17 tests)
Tests connection management:
- Primary service creation and reuse
- Consumer/publisher isolation when configured
- Thread-safe concurrent access
- Shutdown behavior
- Client name overrides

---

## Integration Tests

Integration tests connect to a **real Solace broker** and test end-to-end functionality.

### Prerequisites

1. A running Solace broker (local or remote)
2. Configure connection in `src/test/resources/test-broker.properties`:

```properties
solace.host=tcp://localhost:55555
solace.msgVpn=default
solace.clientUsername=default
solace.clientPassword=default
```

### Running Integration Tests

```bash
# Run all integration tests
mvn verify -P integration

# Run specific integration test
mvn verify -P integration -Dtest=LocalBrokerIntegrationTest
```

### Quick Start with Docker

```bash
# Start Solace Standard broker (free)
docker run -d -p 55555:55555 -p 8080:8080 \
  --name solace \
  solace/solace-pubsub-standard:latest

# Wait for broker to start (~30 seconds), then run tests
mvn verify -P integration
```

### Integration Test Classes

```
src/test/java/com/solace/wrapper/integration/
├── LocalBrokerIntegrationTest.java        # Direct/persistent messaging, health
├── PersistentAckModeIntegrationTest.java  # Manual ACK/NACK with queues
├── DirectReapplyIntegrationTest.java      # Topic subscription reapply
└── AnnotatedPublishIntegrationTest.java   # @SolacePublish with real broker
```

#### LocalBrokerIntegrationTest
- `direct_publish_and_receive` - Publish to topic, receive via subscription
- `persistent_publish_and_receive` - Queue-based messaging
- `health_indicator_reports_up` - Actuator health check

#### PersistentAckModeIntegrationTest
- Manual ACK mode with persistent queues
- NACK and redelivery behavior
- Message settlement patterns

#### DirectReapplyIntegrationTest
- Topic subscription management
- Subscription reapply after reconnect

#### AnnotatedPublishIntegrationTest
- `@SolacePublish` annotation with real broker
- SpEL expression evaluation in topics

### Broker Availability Check

Tests use `TestBroker.assumeAvailable()` which:
1. Attempts TCP socket connection to configured host:port
2. If successful → tests run
3. If failed → tests are **skipped** (not failed)

This allows CI/CD pipelines to skip integration tests when no broker is available.

---

## Test Utilities

### TestBroker

Located at `src/test/java/com/solace/wrapper/testutil/TestBroker.java`

```java
@BeforeAll
static void brokerCheck() {
    TestBroker.assumeAvailable();  // Skip if broker unreachable
}
```

### Test Configuration

`src/test/resources/test-broker.properties`:
```properties
solace.host=tcp://localhost:55555
solace.msgVpn=default
solace.clientUsername=default
solace.clientPassword=default
testbroker.timeoutMs=500
```

---

## Maven Profiles

| Profile | Purpose | Command |
|---------|---------|---------|
| (default) | Unit tests only | `mvn test` |
| `integration` | Unit + Integration tests | `mvn verify -P integration` |
| `broker-it` | Broker-specific IT tests | `mvn verify -P broker-it` |

### Surefire Configuration (Unit Tests)

Excludes integration tests from default test phase:
```xml
<excludes>
    <exclude>**/*IntegrationTest.java</exclude>
    <exclude>**/*BrokerIT.java</exclude>
    <exclude>**/integration/**</exclude>
</excludes>
```

### Failsafe Configuration (Integration Tests)

Runs integration tests during `verify` phase:
```xml
<includes>
    <include>**/*IntegrationTest.java</include>
</includes>
```

---

## Writing New Tests

### Unit Test Template

```java
@Test
void my_feature_works_correctly() {
    log.info("\n───────────────────────────────────────────────────────────────\n" +
            "TEST: my_feature_works_correctly\n" +
            "PURPOSE: Verify that [feature] behaves as expected\n" +
            "───────────────────────────────────────────────────────────────");

    // Arrange
    log.info("STEP 1: Setting up test data");

    // Act
    log.info("STEP 2: Executing feature");

    // Assert
    log.info("STEP 3: Verifying results");
    assertThat(result).isEqualTo(expected);

    log.info("Feature works correctly");
}
```

### Integration Test Template

```java
@SpringBootTest(classes = SolaceAutoConfiguration.class)
@TestPropertySource(locations = "classpath:test-broker.properties")
public class MyIntegrationTest {

    @BeforeAll
    static void brokerCheck() {
        TestBroker.assumeAvailable();
    }

    @Autowired
    SolacePublisher publisher;

    @Test
    void my_integration_test() {
        // Test with real broker
    }
}
```

---

## Troubleshooting

### Tests Hang

If tests hang, check:
1. No infinite loops in message handlers
2. Proper timeout configuration
3. Receipt listeners completing futures

### Integration Tests Skipped

If integration tests show `Tests run: 0`:
1. Verify broker is running
2. Check `test-broker.properties` configuration
3. Test connectivity: `nc -zv localhost 55555`

### Coverage Not Updating

```bash
mvn clean test jacoco:report
```
