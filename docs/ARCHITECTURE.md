# Architecture Overview

This document describes the high-level architecture of the Solace Java API Spring Wrapper.

## Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Spring Application                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐    ┌─────────────────────┐                        │
│  │  @SolacePublish     │    │  @SolaceConsumer    │   Annotations          │
│  │  (method-level)     │    │  (method-level)     │                        │
│  └──────────┬──────────┘    └──────────┬──────────┘                        │
│             │                          │                                    │
│             ▼                          ▼                                    │
│  ┌─────────────────────┐    ┌─────────────────────┐                        │
│  │ SolacePublishAspect │    │SolaceConsumerProc.  │   Annotation Processors│
│  │ (AOP @Around)       │    │(BeanPostProcessor)  │                        │
│  └──────────┬──────────┘    └──────────┬──────────┘                        │
│             │                          │                                    │
│             ▼                          ▼                                    │
│  ┌─────────────────────┐    ┌─────────────────────┐                        │
│  │  SolacePublisher    │    │SolaceConsumerManager│   Core Services        │
│  │  (@Service)         │    │  (@Service)         │                        │
│  └──────────┬──────────┘    └──────────┬──────────┘                        │
│             │                          │                                    │
│             └──────────┬───────────────┘                                    │
│                        ▼                                                    │
│             ┌─────────────────────────────┐                                │
│             │  SolaceConnectionManager    │   Connection Management         │
│             │  (Connection pooling,       │                                │
│             │   isolation, reconnect)     │                                │
│             └──────────────┬──────────────┘                                │
│                            │                                                │
│                            ▼                                                │
│             ┌─────────────────────────────┐                                │
│             │    MessageSerializer        │   Serialization                │
│             │  (JsonMessageSerializer)    │                                │
│             └─────────────────────────────┘                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
                    ┌─────────────────────────────┐
                    │      Solace Broker          │
                    │   (PubSub+ Event Broker)    │
                    └─────────────────────────────┘
```

## Core Components

### 1. Configuration Layer

#### SolaceProperties
- Holds all Solace connection configuration
- Auto-configured via `application.yml` under `solace.*` prefix
- Supports TLS, OAuth2, and connection pooling settings

#### SolaceAnnotationConfiguration
- Enables annotation processing via `@EnableSolaceAnnotations`
- Auto-enabled in Spring Boot via `spring.factories`

### 2. Connection Layer

#### SolaceConnectionManager
- Manages `MessagingService` connections to Solace broker
- Supports connection isolation per consumer/publisher
- Handles connection pooling with configurable limits
- Provides reconnection support when isolation is enabled

**Connection Modes:**
- **Shared Mode** (default): All publishers/consumers share a single connection
- **Isolated Mode**: Each publisher/consumer gets its own connection
  - `solace.isolateConsumers=true` for consumer isolation
  - `solace.isolatePublishers=true` for publisher isolation

### 3. Publishing Layer

#### SolacePublisher
- Core service for publishing messages
- Supports direct (fire-and-forget) and persistent (guaranteed) delivery
- Features:
  - Automatic retry with publisher reinitialization
  - Async publishing with `CompletableFuture`
  - Persistent publish confirmations (best-effort)
  - Configurable backpressure handling
  - Rich message property support

#### SolacePublishAspect
- AOP aspect intercepting `@SolacePublish` annotated methods
- Evaluates SpEL expressions for dynamic routing
- Validates SpEL expressions for security (injection prevention)

### 4. Consumer Layer

#### SolaceConsumer
- Individual consumer instance
- Supports both PERSISTENT (queue) and DIRECT (topic) modes
- Features:
  - Auto/Manual acknowledgment modes
  - Local retry with exponential backoff
  - Configurable termination timeouts

#### SolaceConsumerManager
- Manages lifecycle of all consumers
- Provides start/stop/remove operations
- Registry for consumer status tracking

#### SolaceConsumerProcessor
- BeanPostProcessor scanning for `@SolaceConsumer` methods
- Registers consumers with the manager
- Handles SpEL expression resolution for queue/topic names

### 5. Serialization Layer

#### MessageSerializer Interface
- Abstraction for message serialization/deserialization
- Default implementation: `JsonMessageSerializer` (Jackson)
- Custom implementations can be provided as Spring beans

## Data Flow

### Publishing Flow

```
1. Application calls method with @SolacePublish
           │
           ▼
2. SolacePublishAspect intercepts (AOP @Around)
           │
           ▼
3. Method executes, return value captured
           │
           ▼
4. SpEL expressions evaluated (destination, condition, properties)
           │
           ▼
5. Condition check - skip if false
           │
           ▼
6. SolacePublisher.publishInternal() called
           │
           ▼
7. Message serialized via MessageSerializer
           │
           ▼
8. Properties applied to OutboundMessageBuilder
           │
           ▼
9. Published via DirectMessagePublisher or PersistentMessagePublisher
           │
           ▼
10. Retry on failure with publisher reinitialization
```

### Consumer Flow

```
1. Application startup - SolaceConsumerProcessor scans beans
           │
           ▼
2. @SolaceConsumer methods detected and registered
           │
           ▼
3. SolaceConsumer instances created per annotation
           │
           ▼
4. Consumer started (if autoStart=true)
           │
           ▼
5. Message received from broker
           │
           ▼
6. onMessage() callback invoked
           │
           ▼
7. Message deserialized to target type
           │
           ▼
8. Local retry with backoff on handler failure
           │
           ▼
9. Handler method invoked with payload
           │
           ▼
10. Acknowledgment (AUTO) or manual ack/fail via SolaceAckContext
```

## Thread Model

### Publisher Threads
- `ThreadPoolTaskExecutor` for async publishing
- Configurable pool size via:
  - `solace.publisher-executor-core-size`
  - `solace.publisher-executor-max-size`
  - `solace.publisher-executor-queue-capacity`
- Daemon threads for graceful JVM shutdown

### Consumer Threads
- Solace API manages consumer threads internally
- `receiveAsync()` uses Solace's internal thread pool
- Handler methods execute on Solace receiver threads

### Cleanup Threads
- `solace-confirm-cleanup` - daemon thread for stale confirmation cleanup
- Runs every 30 seconds to prevent memory leaks

## Connection Pooling

```
┌────────────────────────────────────────────────────────────┐
│                 SolaceConnectionManager                    │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  Primary Service (shared when isolation disabled)          │
│  ┌──────────────────────────────────────────────────────┐ │
│  │         MessagingService (primaryService)            │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                            │
│  Consumer Pool (when isolateConsumers=true)               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│  │ consumer-1  │  │ consumer-2  │  │ consumer-N  │       │
│  │ Service     │  │ Service     │  │ Service     │       │
│  └─────────────┘  └─────────────┘  └─────────────┘       │
│                                                            │
│  Publisher Pool (when isolatePublishers=true)             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│  │ publisher-1 │  │ publisher-2 │  │ publisher-N │       │
│  │ Service     │  │ Service     │  │ Service     │       │
│  └─────────────┘  └─────────────┘  └─────────────┘       │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

**Pool Limits:**
- `solace.max-consumer-connections` (default: 50, 0=unlimited)
- `solace.max-publisher-connections` (default: 50, 0=unlimited)

## Error Handling Strategy

### Publisher Errors
1. First attempt fails → log warning
2. Reinitialize publisher (terminate + recreate)
3. Retry publish once
4. If retry fails → throw `SolacePublishException`

### Consumer Errors
1. Local retry with exponential backoff
2. Configurable via annotation:
   - `localMaxAttempts`
   - `localBackoffInitialMs`
   - `localBackoffMultiplier`
   - `localBackoffMaxMs`
3. After retries exhausted:
   - MANUAL ack: settle with FAILED outcome
   - AUTO ack: message may be lost (broker handles)

### Connection Errors
- Solace API handles reconnection internally
- When isolation enabled, reconnect retries forced on
- Consumers log termination but don't auto-restart (to prevent connection proliferation)

## Security Considerations

### SpEL Expression Validation
- All SpEL expressions validated before evaluation
- Blocked patterns include:
  - `Runtime`, `ProcessBuilder`, `Class.forName`
  - `System.exit`, `exec(`, `ClassLoader`
  - File/network operations
  - Reflection methods (`invoke`, `getDeclaredMethod`, `setAccessible`)

### TLS/mTLS Support
- Configure via `solace.tls*` properties
- Certificate validation configurable
- See [TLS Setup Guide](TLS_SETUP.md)

## Lifecycle Management

### Startup Sequence
1. `SolaceProperties` loaded from configuration
2. `SolaceConnectionManager` initializes primary service
3. `SolacePublisher.init()` creates default publisher, starts cleanup task
4. `SolaceConsumerProcessor` scans beans for `@SolaceConsumer`
5. Consumers registered and started (if `autoStart=true`)

### Shutdown Sequence
1. `SolaceConsumerManager.shutdown()` stops all consumers
2. `SolacePublisher.shutdown()`:
   - Stops cleanup executor
   - Completes pending confirms exceptionally
   - Terminates all publishers
   - Shuts down task executor
3. `SolaceConnectionManager.shutdown()`:
   - Disconnects all pooled services
   - Disconnects primary service
