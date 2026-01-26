# Troubleshooting Guide

This guide helps diagnose and resolve common issues with the Solace Spring Wrapper.

## Quick Diagnostic Checklist

Before diving into specific issues, verify these basics:

- [ ] Broker is running and accessible
- [ ] Connection credentials are correct
- [ ] VPN name is correct
- [ ] Network allows connection to broker port
- [ ] Queue/topic permissions are configured
- [ ] Required dependencies are in classpath

## Connection Issues

### Unable to Connect to Broker

**Symptoms:**
- Application fails to start
- `SolaceConnectionException` or connection timeout
- Log shows "Unable to connect to host"

**Diagnostic Steps:**

1. **Verify broker is reachable:**
   ```bash
   # Test TCP connectivity
   nc -zv your-broker.example.com 55555

   # Or with telnet
   telnet your-broker.example.com 55555
   ```

2. **Check configuration:**
   ```yaml
   solace:
     host: tcp://your-broker:55555  # Verify host and port
     msg-vpn: default                # Verify VPN exists
     client-username: admin          # Verify credentials
     client-password: admin
   ```

3. **Enable debug logging:**
   ```yaml
   logging:
     level:
       com.solace: DEBUG
       com.solace.wrapper: DEBUG
   ```

**Common Fixes:**
- Correct the host URL format (`tcp://` for plain, `tcps://` for TLS)
- Verify firewall rules allow the connection
- Check VPN is enabled on the broker
- Verify client profile allows connections

### Connection Drops / Reconnection Issues

**Symptoms:**
- Intermittent disconnections
- Log shows "Session down" or "Connection interrupted"
- Messages stop flowing

**Configuration for better resilience:**

```yaml
solace:
  # Enable reconnection
  reconnect-retries: 5
  reconnect-retry-wait-ms: 3000

  # For isolated connections (forces reconnect behavior)
  isolate-consumers: true
  isolate-publishers: true
```

**If using connection isolation:**
- Isolation mode automatically enables reconnect retries
- Check `max-consumer-connections` and `max-publisher-connections` limits

### Too Many Connections

**Symptoms:**
- `SolaceConnectionException: Consumer connection pool limit exceeded`
- Broker rejects new connections
- Resource exhaustion

**Diagnosis:**
```yaml
# Check your pool limits
solace:
  max-consumer-connections: 50   # Increase if needed
  max-publisher-connections: 50  # Increase if needed
```

**Fixes:**
- Increase pool limits (0 = unlimited)
- Disable isolation if not needed
- Review number of `@SolaceConsumer` annotations
- Check for consumer/publisher leaks

## Publishing Issues

### Message Not Delivered

**Symptoms:**
- No errors but consumer doesn't receive message
- `publishToTopic()` returns without error

**Diagnostic Steps:**

1. **Verify topic name:**
   ```java
   // Check exact topic being used
   @SolacePublish(destination = "orders/created")  // No leading /
   ```

2. **Verify consumer subscription matches:**
   ```java
   @SolaceConsumer(topics = {"orders/>"})  // Wildcard subscription
   ```

3. **Check delivery mode:**
   ```java
   // For guaranteed delivery, use persistent
   @SolacePublish(destination = "...", deliveryMode = "PERSISTENT")
   ```

4. **Enable debug logging:**
   ```yaml
   logging:
     level:
       com.solace.wrapper.publisher: DEBUG
   ```

### Publish Timeout / Slow Publishing

**Symptoms:**
- Publishing takes long time
- `TimeoutException` on persistent publish

**Fixes:**

1. **Adjust pending confirm timeout:**
   ```yaml
   solace:
     pending-confirm-timeout-ms: 60000  # Increase from default 30s
   ```

2. **Check backpressure settings:**
   ```yaml
   solace:
     direct-publisher-backpressure: WAIT  # or REJECT
     direct-publisher-backpressure-wait-ms: 5000
   ```

3. **Use async publishing:**
   ```java
   @SolacePublish(destination = "...", async = true)
   ```

### Serialization Errors

**Symptoms:**
- `RuntimeException: Failed to serialize object`
- Jackson serialization errors

**Fixes:**

1. **Ensure object is serializable:**
   ```java
   // Add Jackson annotations if needed
   @JsonIgnoreProperties(ignoreUnknown = true)
   public class MyMessage {
       // Ensure getters/setters or public fields
   }
   ```

2. **Configure custom ObjectMapper:**
   ```java
   @Bean
   public JsonMessageSerializer messageSerializer(ObjectMapper mapper) {
       return new JsonMessageSerializer(mapper);
   }
   ```

## Consumer Issues

### Consumer Not Receiving Messages

**Symptoms:**
- Consumer method never called
- No errors in logs
- Messages visible in Solace broker queue

**Diagnostic Steps:**

1. **Verify consumer is started:**
   ```java
   @SolaceConsumer(queue = "my-queue", autoStart = true)  // Default
   ```

2. **Check queue name matches:**
   ```java
   @SolaceConsumer(queue = "my-queue")  // Must match broker queue
   ```

3. **For topics, check subscription:**
   ```java
   @SolaceConsumer(topics = {"orders/>"})  // Wildcard syntax
   ```

4. **Verify annotation scanning:**
   ```java
   @Configuration
   @EnableSolaceAnnotations  // Must be present
   public class AppConfig {}
   ```

5. **Check consumer status programmatically:**
   ```java
   @Autowired
   SolaceConsumerManager consumerManager;

   // List all consumers
   consumerManager.getAllConsumers().forEach((id, consumer) -> {
       System.out.println(id + ": running=" + consumer.isRunning());
   });
   ```

### Deserialization Errors

**Symptoms:**
- `RuntimeException: Failed to deserialize message`
- Consumer receives message but handler fails

**Fixes:**

1. **Ensure message type matches:**
   ```java
   @SolaceConsumer(queue = "orders")
   public void handleOrder(Order order, InboundMessage msg) {
       // Order class must match published structure
   }
   ```

2. **Handle raw messages:**
   ```java
   @SolaceConsumer(queue = "orders")
   public void handleOrder(String rawJson, InboundMessage msg) {
       // Process raw JSON manually
   }
   ```

3. **Specify message type explicitly:**
   ```java
   @SolaceConsumer(queue = "orders", messageType = "com.example.Order")
   ```

### Message Redelivery Loop

**Symptoms:**
- Same message processed repeatedly
- Consumer throws exception and message returns

**Fixes:**

1. **Handle exceptions properly:**
   ```java
   @SolaceConsumer(queue = "orders", ackMode = SolaceConsumer.AckMode.MANUAL)
   public void handle(Order order, InboundMessage msg, SolaceAckContext ack) {
       try {
           process(order);
           ack.ack();  // Acknowledge success
       } catch (Exception e) {
           ack.fail();  // Mark as failed, don't redeliver
       }
   }
   ```

2. **Configure retry limits:**
   ```java
   @SolaceConsumer(
       queue = "orders",
       localMaxAttempts = 3,
       localBackoffInitialMs = 200,
       localBackoffMultiplier = 2.0
   )
   ```

### Queue Not Found / Auto-Create Fails

**Symptoms:**
- `Unknown Queue` error
- Consumer fails to start

**Fixes:**

1. **Pre-create queue on broker** (recommended for production)

2. **Enable auto-create (requires broker support):**
   ```java
   @SolaceConsumer(queue = "my-queue", autoCreateQueue = true)
   ```

3. **Check client profile permissions:**
   - Ensure client profile allows endpoint creation

## SpEL Expression Issues

### Invalid SpEL Expression

**Symptoms:**
- `SpelEvaluationException`
- `IllegalStateException: Invalid SpEL expression`

**Common Mistakes:**

```java
// WRONG - missing # prefix
@SolacePublish(destination = "orders/{result.id}")

// CORRECT
@SolacePublish(destination = "orders/#{result.id}")

// WRONG - wrong variable name
@SolacePublish(destination = "orders/#{order.id}")  // 'order' not defined

// CORRECT - use result or parameter refs
@SolacePublish(destination = "orders/#{result.id}")
@SolacePublish(destination = "orders/#{#p0.id}")  // First parameter
```

### SpEL Security Validation Error

**Symptoms:**
- `IllegalStateException: Invalid SpEL expression - contains dangerous pattern`

**Cause:** Expression contains blocked patterns for security.

**Blocked patterns include:**
- `Runtime`, `ProcessBuilder`
- `Class.forName`, `System.exit`
- `exec(`, `ClassLoader`
- File/network operations
- Reflection methods

**Fix:** Use only safe SpEL expressions:
```java
// Safe examples
@SolacePublish(destination = "orders/#{result.type}")
@SolacePublish(condition = "#{result != null}")
@SolacePublish(userProperties = {"key=#{result.value}"})
```

## Performance Issues

### High Memory Usage

**Symptoms:**
- OutOfMemoryError
- Heap usage growing continuously

**Potential Causes:**

1. **Unbounded pending confirms (fixed in v1.0):**
   - Ensure `pending-confirm-timeout-ms` is configured
   - Check cleanup task is running

2. **Too many connections:**
   ```yaml
   solace:
     max-consumer-connections: 50
     max-publisher-connections: 50
   ```

3. **Large message payloads:**
   - Consider message compression
   - Use streaming for large data

### Slow Message Processing

**Symptoms:**
- High latency
- Message backlog growing

**Optimizations:**

1. **Tune thread pools:**
   ```yaml
   solace:
     publisher-executor-core-size: 4
     publisher-executor-max-size: 16
     publisher-executor-queue-capacity: 2000
   ```

2. **Use async publishing:**
   ```java
   @SolacePublish(destination = "...", async = true)
   ```

3. **Optimize serialization:**
   - Use efficient ObjectMapper settings
   - Consider alternative serializers for high throughput

## Debugging Tips

### Enable Comprehensive Logging

```yaml
logging:
  level:
    com.solace: DEBUG
    com.solace.wrapper: DEBUG
    com.solace.wrapper.publisher: TRACE
    com.solace.wrapper.consumer: TRACE
    com.solace.wrapper.connection: DEBUG
```

### Inspect Consumer Registry

```java
@Autowired
SolaceConsumerRegistry registry;

// In a diagnostic endpoint
registry.getAllConsumerIds().forEach(id -> {
    var status = registry.getConsumerStatus(id);
    System.out.println(id + ": " + status);
});
```

### Monitor Health Indicators

If health indicators are enabled:

```bash
curl http://localhost:8080/actuator/health/solace
```

```json
{
  "status": "UP",
  "details": {
    "connection": "CONNECTED",
    "consumers": 3,
    "publishers": 1
  }
}
```

### Solace Broker Tools

Use Solace CLI or PubSub+ Manager to:
- Check queue depth and bindings
- View client connections
- Monitor message rates
- Review client profile settings

```bash
# Example Solace CLI commands
solace> show queue * detail
solace> show client * stats
solace> show message-vpn * stats
```

## Getting Help

If you're still stuck:

1. **Check the logs** - most issues have descriptive error messages
2. **Review configuration** - many issues are config-related
3. **Test connectivity** - ensure broker is reachable
4. **Open an issue** - provide logs, config (sanitized), and steps to reproduce

See also:
- [Architecture Overview](ARCHITECTURE.md)
- [TLS Setup Guide](TLS_SETUP.md)
- [Annotations Reference](ANNOTATIONS.md)
