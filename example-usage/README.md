# Example Usage

This folder demonstrates usage of the Solace Spring Wrapper with both annotation-based and programmatic approaches.

## Project Structure

```
example-usage/
├── pom.xml                          # Maven build file
├── README.md                        # This file
└── src/main/
    ├── java/com/example/myapp/
    │   ├── ExampleApplication.java           # Spring Boot entry point
    │   ├── service/
    │   │   ├── AnnotationBasedOrderService.java  # Annotation-based examples (active)
    │   │   └── ProgrammaticExampleService.java   # Programmatic API examples (active)
    │   └── workflow/
    │       └── ExpandedOrderWorkflow.java        # Advanced multi-service workflow (reference)
    └── resources/
        └── application.yml           # Solace connection settings
```

## Running the Example

### Prerequisites

1. **Java 18+** installed
2. **Solace Broker** running locally. Start one with Docker:
   ```bash
   docker run -d -p 55555:55555 -p 8080:8080 --name solace solace/solace-pubsub-standard
   ```

### Steps to Run

```bash
# 1. Build and install the parent wrapper (from project root)
cd /path/to/solace-spring-wrapper
mvn clean install -DskipTests

# 2. Run the example app
cd example-usage
mvn spring-boot:run
```

### What You'll See

The demo will:
1. Create orders (published to `orders/created`)
2. Route orders to processing (VIP vs standard queues)
3. Update order status (conditional publishing)
4. Process bulk updates

Watch the logs for `@SolacePublish` and `@SolaceConsumer` activity.

## Example Files

### AnnotationBasedOrderService.java (Active)
Demonstrates annotation-based Solace integration:
- `@SolacePublish` for declarative publishing with SpEL expressions
- `@SolaceConsumer` for declarative queue/topic consumption
- Dynamic routing with SpEL (`#{result.orderType == 'VIP' ? 'vip' : 'standard'}`)
- Conditional publishing (`condition = "#{#p2 != #p1}"`)
- User properties from SpEL (`userProperties = {"orderType=#{result.orderType}"}`)

### ProgrammaticExampleService.java (Active)
Demonstrates programmatic API usage:
- `SolacePublisher` for direct publishing control
- `SolaceConsumerManager.createConsumer()` for runtime consumer creation
- `MessageProperties` for custom message headers
- Consumer lifecycle management (`start/stop/remove`)
- Async publishing with `CompletableFuture`

### ExpandedOrderWorkflow.java (Reference Only)
Advanced multi-service workflow showing:
- Chained `@SolaceConsumer` + `@SolacePublish` for message transformation
- Manual ack with `SolaceAckContext`
- DIRECT mode consumers (topic subscription without queue)
- Request-reply pattern with dynamic `replyTo`
- Local retry with exponential backoff

**Note:** The services in this file have `@Service` commented out to avoid conflicts. Uncomment to activate.

## Configuration

Edit `src/main/resources/application.yml` to change:
- `solace.host` - Broker address (default: `tcp://localhost:55555`)
- `solace.msg-vpn` - Message VPN (default: `default`)
- `solace.client-username` / `solace.client-password` - Credentials

## Annotation Quick Reference

### @SolacePublish Attributes
| Attribute | Type | SpEL | Notes |
|-----------|------|------|-------|
| destination | String | Yes | Dynamic topic routing (required) |
| correlationId | String | Yes | Message correlation |
| replyTo | String | Yes | Request-reply pattern |
| condition | String | Yes | Conditional publishing |
| userProperties | String[] | Yes | Custom headers as `key=value` pairs |
| deliveryMode | String | Yes | `DIRECT` or `PERSISTENT` |
| clientName | String | Yes | Override publisher connection name |
| applicationMessageType | String | Yes | Application-defined message type |
| applicationMessageId | String | Yes | Application-defined message ID |
| elidingEligible | String | Yes | Boolean - message eligible for eliding |
| classOfService | String | Yes | Integer 0-3 for priority classes |
| messageExpiration | String | Yes | Long - absolute expiration timestamp |
| sequenceNumber | String | Yes | Long - message sequence number |
| priority | int | No | Static 0-255 message priority |
| timeToLive | long | No | Static TTL in ms (-1 for none) |
| async | boolean | No | Non-blocking publish |

### @SolaceConsumer Attributes
| Attribute | Type | Notes |
|-----------|------|-------|
| queue | String | Queue name for persistent messaging |
| topics | String[] | Topic subscriptions (supports wildcards) |
| mode | MessagingMode | AUTO, PERSISTENT, or DIRECT |
| ackMode | AckMode | AUTO or MANUAL |
| condition | String | SpEL filter expression |
| autoCreateQueue | boolean | Auto-create queue if missing (default: true) |
| consumerId | String | Custom consumer ID |
| consumerIdPrefix | String | Prefix for generated consumer ID |
| clientName | String | Override consumer connection name |
| messageType | String | Expected message type class name |
| autoStart | boolean | Start on application startup (default: true) |
| maxRetries | int | Retry attempts for failed messages (default: 3) |
| retryDelay | long | Delay between retries in ms (default: 1000) |
| localMaxAttempts | int | Local retry attempts (default: 1) |
| localBackoffInitialMs | long | Initial backoff delay (default: 200) |
| localBackoffMultiplier | double | Exponential backoff multiplier (default: 2.0) |
| localBackoffMaxMs | long | Maximum backoff cap in ms (default: 2000) |

## SpEL Expression Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `result` | Method return value | `#{result.orderId}` |
| `#p0`, `#p1`, ... | Parameters by index | `#{#p0}` (first param) |
| `#paramName` | Parameter by name | `#{#orderId}` (requires `-parameters`) |
| `T(ClassName)` | Static methods/fields | `#{T(System).currentTimeMillis()}` |

Use `result` when accessing return value fields (most common). Use `#p0`/`#p1` when you need input parameter values.
