package com.solace.wrapper.publisher;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.*;
import com.solace.messaging.DirectMessagePublisherBuilder;
import com.solace.messaging.resources.Topic;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.exception.SolacePublishException;
import com.solace.wrapper.metrics.SolaceMetrics;
import com.solace.wrapper.serialization.MessageSerializer;
import com.solace.wrapper.config.SolaceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Service for publishing messages to Solace topics and queues using the new Solace Java API.
 */
@Service
public class SolacePublisher {

    private static final Logger logger = LoggerFactory.getLogger(SolacePublisher.class);

    private final SolaceConnectionManager connectionManager;
    private final MessageSerializer messageSerializer;
    private final TaskExecutor taskExecutor;
    /** Optional metrics facade; never null (defaults to a disabled no-op). */
    private volatile SolaceMetrics metrics = new SolaceMetrics(null);
    private final Map<String, DirectMessagePublisher> directPublishers = new ConcurrentHashMap<>();
    private final Map<String, PersistentMessagePublisher> persistentPublishers = new ConcurrentHashMap<>();
    private final Map<String, Boolean> persistentReceiptsSupported = new ConcurrentHashMap<>();
    private final Map<String, PendingConfirm> pendingConfirms = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "solace-confirm-cleanup");
        t.setDaemon(true);
        return t;
    });
    private long pendingConfirmTimeoutMs = 30000; // default 30 seconds
    private long terminationTimeoutMs = 5000; // default 5 seconds

    public SolacePublisher(SolaceConnectionManager connectionManager,
                          MessageSerializer messageSerializer) {
        this.connectionManager = connectionManager;
        this.messageSerializer = messageSerializer;
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setThreadNamePrefix("solace-pub-");
        exec.setDaemon(true); // Use daemon threads so JVM can exit gracefully
        // Allow configuration via SolaceProperties if bean available
        SolaceProperties props = connectionManager.getProperties();
        int core = props.getPublisherExecutorCoreSize() != null ? props.getPublisherExecutorCoreSize() : 2;
        int max = props.getPublisherExecutorMaxSize() != null ? props.getPublisherExecutorMaxSize() : 8;
        int cap = props.getPublisherExecutorQueueCapacity() != null ? props.getPublisherExecutorQueueCapacity() : 1000;
        exec.setCorePoolSize(core);
        exec.setMaxPoolSize(max);
        exec.setQueueCapacity(cap);
        exec.initialize();
        this.taskExecutor = exec;
        // Configure pending confirm timeout from properties
        this.pendingConfirmTimeoutMs = props.getPendingConfirmTimeoutMs();
        // Configure termination timeout from properties
        this.terminationTimeoutMs = props.getTerminationTimeoutMs();
    }

    /**
     * Injects the metrics facade. Optional: when not set, publishing metrics are silently
     * skipped. Wired by {@code SolaceAutoConfiguration} when a {@link SolaceMetrics} bean exists.
     *
     * @param metrics the metrics facade; ignored if {@code null}
     */
    public void setMetrics(SolaceMetrics metrics) {
        if (metrics != null) {
            this.metrics = metrics;
        }
    }

    @PostConstruct
    public void init() {
        ensurePublisher();
        // Start cleanup task for stale pending confirms (runs every 30 seconds)
        cleanupExecutor.scheduleAtFixedRate(this::cleanupStalePendingConfirms, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * Removes stale pending confirms that have exceeded the timeout.
     * Completes them exceptionally with a TimeoutException.
     */
    private void cleanupStalePendingConfirms() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, PendingConfirm>> it = pendingConfirms.entrySet().iterator();
        int cleaned = 0;
        while (it.hasNext()) {
            Map.Entry<String, PendingConfirm> entry = it.next();
            PendingConfirm pc = entry.getValue();
            if (now - pc.createdAt > pendingConfirmTimeoutMs) {
                it.remove();
                if (!pc.future.isDone()) {
                    pc.future.completeExceptionally(new TimeoutException(
                            "Pending publish confirmation timed out after " + pendingConfirmTimeoutMs + "ms"));
                }
                cleaned++;
            }
        }
        if (cleaned > 0) {
            logger.debug("Cleaned up {} stale pending publish confirmations", cleaned);
        }
    }

    @PreDestroy
    public void shutdown() {
        // Shutdown cleanup executor first
        cleanupExecutor.shutdownNow();

        // Complete any remaining pending confirms exceptionally
        pendingConfirms.forEach((cid, pc) -> {
            if (!pc.future.isDone()) {
                pc.future.completeExceptionally(new SolacePublishException("Publisher shutting down"));
            }
        });
        pendingConfirms.clear();

        directPublishers.values().forEach(publisher -> {
            try { publisher.terminate(terminationTimeoutMs); } catch (Exception e) { logger.warn("Error terminating publisher", e); }
        });
        directPublishers.clear();
        persistentPublishers.values().forEach(publisher -> {
            try { publisher.terminate(terminationTimeoutMs); } catch (Exception e) { logger.warn("Error terminating persistent publisher", e); }
        });
        persistentPublishers.clear();
        if (taskExecutor instanceof ThreadPoolTaskExecutor) {
            ThreadPoolTaskExecutor tpe = (ThreadPoolTaskExecutor) taskExecutor;
            tpe.shutdown();
            try {
                // Wait for executor to terminate
                if (!tpe.getThreadPoolExecutor().awaitTermination(terminationTimeoutMs, TimeUnit.MILLISECONDS)) {
                    tpe.getThreadPoolExecutor().shutdownNow();
                }
            } catch (InterruptedException e) {
                tpe.getThreadPoolExecutor().shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final String PRIMARY_PUBLISHER_KEY = "primary";

    private void ensurePublisher() {
        PublisherContext context = resolvePublisherContext(null, null);
        getOrCreateDirectPublisher(context);
    }

    private PublisherContext resolvePublisherContext(String publisherKey, String clientNameOverride) {
        String key = (publisherKey == null || publisherKey.isEmpty()) ? PRIMARY_PUBLISHER_KEY : publisherKey;
        MessagingService messagingService = connectionManager.createPublisherService(key, clientNameOverride);
        return new PublisherContext(key, messagingService);
    }

    private DirectMessagePublisher getOrCreateDirectPublisher(PublisherContext context) {
        return directPublishers.computeIfAbsent(context.publisherKey, key -> {
            DirectMessagePublisherBuilder builder = context.messagingService.createDirectMessagePublisherBuilder();
            SolaceProperties props = connectionManager.getProperties();
            try {
                int capacity = Math.max(1, props.getDirectPublisherBackpressureWaitMs());
                if (props.getDirectPublisherBackpressure() == SolaceProperties.BackpressureStrategy.REJECT) {
                    builder.onBackPressureReject(capacity);
                } else {
                    builder.onBackPressureWait(capacity);
                }
            } catch (Throwable t) {
                builder.onBackPressureWait(1);
            }
            DirectMessagePublisher publisher = builder.build();
            publisher.start();
            logger.info("DirectMessagePublisher initialized for key={}", key);
            return publisher;
        });
    }

    private PersistentMessagePublisher getOrCreatePersistentPublisher(PublisherContext context) {
        return persistentPublishers.computeIfAbsent(context.publisherKey, key -> {
            PersistentMessagePublisher pub = context.messagingService.createPersistentMessagePublisherBuilder()
                    .build();
            try {
                pub.setMessagePublishReceiptListener(receipt -> {
                    String cid = null;
                    try {
                        if (receipt != null && receipt.getUserContext() != null) {
                            cid = String.valueOf(receipt.getUserContext());
                        } else if (receipt != null && receipt.getMessage() != null && receipt.getMessage().getProperties() != null) {
                            Object v = receipt.getMessage().getProperties().get(
                                    com.solace.messaging.config.SolaceProperties.MessageProperties.CORRELATION_ID);
                            if (v != null) cid = String.valueOf(v);
                        }
                    } catch (Throwable t) {
                        logger.debug("Error extracting correlation ID from receipt: {}", t.getMessage());
                    }
                    PendingConfirm pc = (cid != null) ? pendingConfirms.remove(cid) : null;
                    CompletableFuture<Void> fut = (pc != null) ? pc.future : null;
                    if (receipt != null && receipt.getException() != null) {
                        if (fut != null) fut.completeExceptionally(receipt.getException());
                        logger.error("Persistent publish failed: {}", receipt.getException().toString(), receipt.getException());
                        return;
                    }
                    if (receipt != null && receipt.isPersisted()) {
                        if (fut != null && !fut.isDone()) fut.complete(null);
                        return;
                    }
                    if (fut != null && !fut.isDone()) fut.completeExceptionally(new com.solace.wrapper.exception.SolacePublishException("Publish receipt not persisted"));
                });
                persistentReceiptsSupported.put(key, Boolean.TRUE);
            } catch (Throwable t) {
                persistentReceiptsSupported.put(key, Boolean.FALSE);
                logger.debug("MessagePublishReceiptListener not available; using best-effort confirmations: {}", t.toString());
            }
            pub.start();
            logger.info("PersistentMessagePublisher initialized for key={}", key);
            return pub;
        });
    }

    private void reinitDirectPublisher(PublisherContext context) {
        DirectMessagePublisher existing = directPublishers.remove(context.publisherKey);
        if (existing != null) {
            try {
                existing.terminate(terminationTimeoutMs);
            } catch (Exception e) {
                logger.debug("Error terminating direct publisher during reinit: {}", e.getMessage());
            }
        }
        getOrCreateDirectPublisher(context);
    }

    private void reinitPersistentPublisher(PublisherContext context) {
        PersistentMessagePublisher existing = persistentPublishers.remove(context.publisherKey);
        if (existing != null) {
            try {
                existing.terminate(terminationTimeoutMs);
            } catch (Exception e) {
                logger.debug("Error terminating persistent publisher during reinit: {}", e.getMessage());
            }
        }
        getOrCreatePersistentPublisher(context);
    }

    private void publishWithRetry(PublisherContext context, OutboundMessage message, Topic topic) {
        try {
            DirectMessagePublisher publisher = getOrCreateDirectPublisher(context);
            publisher.publish(message, topic);
        } catch (Exception e) {
            logger.warn("Publish failed, reinitializing direct publisher and retrying", e);
            reinitDirectPublisher(context);
            try {
                DirectMessagePublisher publisher = getOrCreateDirectPublisher(context);
                publisher.publish(message, topic);
            } catch (Exception finalEx) {
                logger.error("Publish failed after retry", finalEx);
                throw new SolacePublishException("Failed to publish after retry", finalEx);
            }
        }
    }

    private void publishPersistentWithRetry(PublisherContext context, OutboundMessage message, Topic topic,
                                            java.util.Properties extendedProperties) {
        try {
            PersistentMessagePublisher publisher = getOrCreatePersistentPublisher(context);
            if (extendedProperties != null) {
                publisher.publish(message, topic, null, extendedProperties);
            } else {
                publisher.publish(message, topic);
            }
        } catch (Exception e) {
            logger.warn("Persistent publish failed, reinitializing persistent publisher and retrying", e);
            reinitPersistentPublisher(context);
            try {
                PersistentMessagePublisher publisher = getOrCreatePersistentPublisher(context);
                if (extendedProperties != null) {
                    publisher.publish(message, topic, null, extendedProperties);
                } else {
                    publisher.publish(message, topic);
                }
            } catch (Exception finalEx) {
                logger.error("Persistent publish failed after retry", finalEx);
                throw new SolacePublishException("Failed to persistent publish after retry", finalEx);
            }
        }
    }

    /**
     * Publishes a message to a topic synchronously.
     *
     * @param topicName The topic name to publish to
     * @param message The message object to publish
     */
    public void publishToTopic(String topicName, Object message) {
        publishInternal(topicName, message, null, false, null, null);
    }

    /**
     * Publishes a message to a topic using persistent delivery.
     */
    public void publishPersistentToTopic(String topicName, Object message) {
        publishInternal(topicName, message, null, true, null, null);
    }

    public void publishWithContext(String topicName, Object message, MessageProperties properties,
                                   boolean persistent, String clientName, String publisherKey) {
        publishInternal(topicName, message, properties, persistent, clientName, publisherKey);
    }

    public CompletableFuture<Void> publishWithContextAsync(String topicName, Object message, MessageProperties properties,
                                                           boolean persistent, String clientName, String publisherKey) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        taskExecutor.execute(() -> {
            try {
                publishInternal(topicName, message, properties, persistent, clientName, publisherKey);
                f.complete(null);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        return f;
    }

    /**
     * Publish persistent and return a future that completes on broker confirm (best-effort).
     */
    public CompletableFuture<Void> publishPersistentToTopicAsyncConfirm(String topicName, Object message) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        taskExecutor.execute(() -> {
            String cid = null;
            try {
                PublisherContext context = resolvePublisherContext(null, null);
                Topic topic = Topic.of(topicName);
                OutboundMessage outbound = messageSerializer.serialize(context.messagingService, message, topic);
                cid = java.util.UUID.randomUUID().toString();
                pendingConfirms.put(cid, new PendingConfirm(result));
                // Publish with userContext so receipt listener can correlate
                try {
                    PersistentMessagePublisher publisher = getOrCreatePersistentPublisher(context);
                    publisher.publish(outbound, topic, cid);
                } catch (Exception e2) {
                    // retry path
                    logger.warn("Persistent publish (with userContext) failed, reinitializing and retrying", e2);
                    reinitPersistentPublisher(context);
                    PersistentMessagePublisher publisher = getOrCreatePersistentPublisher(context);
                    publisher.publish(outbound, topic, cid);
                }
                if (!Boolean.TRUE.equals(persistentReceiptsSupported.get(context.publisherKey))) {
                    pendingConfirms.remove(cid);
                    result.complete(null);
                }
            } catch (Exception e) {
                if (cid != null) {
                    pendingConfirms.remove(cid);
                }
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    /**
     * Publish persistent and block until confirm or timeout (best-effort).
     */
    public void publishPersistentToTopicAwait(String topicName, Object message, Duration timeout) {
        CompletableFuture<Void> fut = publishPersistentToTopicAsyncConfirm(topicName, message);
        try {
            if (timeout == null) timeout = Duration.ofSeconds(10);
            fut.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new SolacePublishException("Persistent publish confirm failed or timed out", e);
        }
    }

    /**
     * Publishes a message to a topic asynchronously.
     *
     * @param topicName The topic name to publish to
     * @param message The message object to publish
     * @return CompletableFuture that completes when message is published
     */
    public CompletableFuture<Void> publishToTopicAsync(String topicName, Object message) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        taskExecutor.execute(() -> {
            try {
                publishToTopic(topicName, message);
                f.complete(null);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        return f;
    }

    public CompletableFuture<Void> publishPersistentToTopicAsync(String topicName, Object message) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        taskExecutor.execute(() -> {
            try {
                publishPersistentToTopic(topicName, message);
                f.complete(null);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        return f;
    }

    /**
     * Publishes a message with custom properties to a topic.
     *
     * @param topicName The topic name to publish to
     * @param message The message object to publish
     * @param properties Custom message properties
     */
    public void publishToTopicWithProperties(String topicName, Object message, 
                                           MessageProperties properties) {
        publishInternal(topicName, message, properties, false, null, null);
    }

    public void publishPersistentToTopicWithProperties(String topicName, Object message,
                                                       MessageProperties properties) {
        publishInternal(topicName, message, properties, true, null, null);
    }

    private void publishInternal(String topicName, Object message, MessageProperties properties,
                                 boolean persistent, String clientName, String publisherKey) {
        final String deliveryMode = persistent ? "PERSISTENT" : "DIRECT";
        final long start = System.nanoTime();
        boolean success = false;
        try {
            PublisherContext context = resolvePublisherContext(publisherKey, clientName);
            Topic topic = Topic.of(topicName);
            OutboundMessage outboundMessage = createMessageWithProperties(context.messagingService, message, topic, properties);
            java.util.Properties extended = (properties != null) ? toPersistentExtendedProperties(properties) : null;
            if (persistent) {
                publishPersistentWithRetry(context, outboundMessage, topic, extended);
                logger.info("Persistent message published to topic: {}", topicName);
            } else {
                publishWithRetry(context, outboundMessage, topic);
                logger.info("Message published to topic: {}", topicName);
            }
            success = true;
        } catch (Exception e) {
            logger.error("Failed to publish message to topic: {}", topicName, e);
            throw new SolacePublishException("Failed to publish message to topic: " + topicName, e);
        } finally {
            metrics.recordPublishLatency(success, deliveryMode, topicName, clientName, System.nanoTime() - start);
        }
    }

    /**
     * Publishes a message with custom properties asynchronously using the internal executor.
     */
    public CompletableFuture<Void> publishToTopicWithPropertiesAsync(String topicName, Object message,
                                                                     MessageProperties properties) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        taskExecutor.execute(() -> {
            try {
                publishToTopicWithProperties(topicName, message, properties);
                f.complete(null);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        return f;
    }

    public CompletableFuture<Void> publishPersistentToTopicWithPropertiesAsync(String topicName, Object message,
                                                                               MessageProperties properties) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        taskExecutor.execute(() -> {
            try {
                publishPersistentToTopicWithProperties(topicName, message, properties);
                f.complete(null);
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        return f;
    }

    /**
     * Creates an OutboundMessage with custom properties applied.
     */
    private OutboundMessage createMessageWithProperties(MessagingService messagingService, Object messageObj,
                                                       Object destination, MessageProperties properties) {
        if (properties == null) {
            return messageSerializer.serialize(messagingService, messageObj, destination);
        }
        byte[] payload = messageSerializer.serializeToBytes(messagingService, messageObj);
        OutboundMessageBuilder messageBuilder = messagingService != null ? messagingService.messageBuilder() : null;
        if (messageBuilder == null) {
            logger.warn("MessagingService.messageBuilder() returned null; falling back to default serializer (properties ignored)");
            return messageSerializer.serialize(messagingService, messageObj, destination);
        }
        applyMessageProperties(messageBuilder, properties);
        return messageBuilder.build(payload);
    }

    /**
     * Applies only Solace Java API supported properties:
     *  - TTL: withTimeToLive
     *  - Priority: withPriority
     *  - Correlation ID: SolaceProperties.MessageProperties.CORRELATION_ID
     */
    private void applyMessageProperties(OutboundMessageBuilder messageBuilder, MessageProperties properties) {
        try {
            // Correlation ID (official constant)
            if (properties.getCorrelationId() != null) {
                messageBuilder.withProperty( com.solace.messaging.config.SolaceProperties.MessageProperties.CORRELATION_ID, properties.getCorrelationId());
            }

            // Transport-affecting via builder methods
            if (properties.getTimeToLive() > 0) {
                messageBuilder.withTimeToLive(properties.getTimeToLive());
            }

            if (properties.getPriority() >= 0 && properties.getPriority() <= 255) {
                messageBuilder.withPriority(properties.getPriority());
            } else if (properties.getPriority() != -1) {
                logger.warn("Invalid priority value: {} (must be 1-255, default is -1)", properties.getPriority());
            }

            // Optional reply-to for request/response style flows
            if (properties.getReplyTo() != null) {
                messageBuilder.withProperty("reply-to", properties.getReplyTo());
            }
            
            // Application-visible, documented properties
            if (properties.getApplicationMessageType() != null) {
                messageBuilder.withProperty(com.solace.messaging.config.SolaceProperties.MessageProperties.APPLICATION_MESSAGE_TYPE,
                        properties.getApplicationMessageType());
            }
            if (properties.getApplicationMessageId() != null) {
                messageBuilder.withProperty(com.solace.messaging.config.SolaceProperties.MessageProperties.APPLICATION_MESSAGE_ID,
                        properties.getApplicationMessageId());
            }
            if (properties.getElidingEligible() != null) {
                messageBuilder.withProperty(com.solace.messaging.config.SolaceProperties.MessageProperties.ELIDING_ELIGIBLE,
                        properties.getElidingEligible().toString());
            }
            if (properties.getClassOfService() != null) {
                Integer cos = properties.getClassOfService();
                if (cos < 0 || cos > 2) {
                    logger.warn("Invalid class of service value: {} (must be 0..2)", cos);
                }
                messageBuilder.withProperty(com.solace.messaging.config.SolaceProperties.MessageProperties.CLASS_OF_SERVICE,
                        String.valueOf(cos));
            }
            if (properties.getSenderId() != null) {
                messageBuilder.withProperty(com.solace.messaging.config.SolaceProperties.MessageProperties.SENDER_ID,
                        properties.getSenderId());
            }
            if (properties.getSequenceNumber() != null) {
                messageBuilder.withProperty(com.solace.messaging.config.SolaceProperties.MessageProperties.SEQUENCE_NUMBER,
                        String.valueOf(properties.getSequenceNumber()));
            }

            // Optional HTTP interop properties if present
            if (properties.getHttpContentType() != null) {
                messageBuilder.withProperty(com.solace.messaging.config.SolaceProperties.MessageProperties.HTTP_CONTENT_TYPE,
                        properties.getHttpContentType());
            }
            if (properties.getHttpContentEncoding() != null) {
                messageBuilder.withProperty(com.solace.messaging.config.SolaceProperties.MessageProperties.HTTP_CONTENT_ENCODING,
                        properties.getHttpContentEncoding());
            }

            // Optional delivery mode hint
            if (properties.getDeliveryMode() != null) {
                String mode = properties.getDeliveryMode().trim().toUpperCase();
                if ("PERSISTENT".equals(mode) || "DIRECT".equals(mode)) {
                    messageBuilder.withProperty("delivery-mode", mode);
                } else {
                    logger.warn("Invalid delivery mode: {} (expected DIRECT or PERSISTENT)", properties.getDeliveryMode());
                }
            }

            // Optional absolute expiration timestamp
            if (properties.getMessageExpiration() != null) {
                messageBuilder.withProperty("message-expiration",
                        String.valueOf(properties.getMessageExpiration()));
            }

            Map<String, Object> userProps = properties.getUserProperties();
            if (userProps != null && !userProps.isEmpty()) {
                for (Map.Entry<String, Object> entry : userProps.entrySet()) {
                    if (entry.getKey() != null) {
                        Object valObj = entry.getValue();
                        if (valObj != null) {
                            messageBuilder.withProperty(entry.getKey(), valObj.toString());
                        }
                    }
                }
            }

        } catch (Exception e) {
            logger.warn("Failed to apply some message properties", e);
        }
    }

    private java.util.Properties toPersistentExtendedProperties(MessageProperties properties) {
        java.util.Properties ext = new java.util.Properties();
        try {
            if (properties.getPersistentTimeToLive() != null) {
                ext.setProperty(
                        com.solace.messaging.config.SolaceProperties.MessageProperties.PERSISTENT_TIME_TO_LIVE,
                        String.valueOf(properties.getPersistentTimeToLive()));
            }
            if (properties.getPersistentExpiration() != null) {
                ext.setProperty(
                        com.solace.messaging.config.SolaceProperties.MessageProperties.PERSISTENT_EXPIRATION,
                        String.valueOf(properties.getPersistentExpiration()));
            }
            if (properties.getPersistentAckImmediately() != null) {
                ext.setProperty(
                        com.solace.messaging.config.SolaceProperties.MessageProperties.PERSISTENT_ACK_IMMEDIATELY,
                        properties.getPersistentAckImmediately().toString());
            }
            if (properties.getPersistentDmqEligible() != null) {
                ext.setProperty(
                        com.solace.messaging.config.SolaceProperties.MessageProperties.PERSISTENT_DMQ_ELIGIBLE,
                        properties.getPersistentDmqEligible().toString());
            }
        } catch (Exception e) {
            logger.warn("Failed to map some persistent-only properties", e);
        }
        return ext;
    }

    private static class PublisherContext {
        private final String publisherKey;
        private final MessagingService messagingService;

        private PublisherContext(String publisherKey, MessagingService messagingService) {
            this.publisherKey = publisherKey;
            this.messagingService = messagingService;
        }
    }

    /**
     * Wrapper class for pending confirmations with timestamp for cleanup.
     */
    private static class PendingConfirm {
        final CompletableFuture<Void> future;
        final long createdAt;

        PendingConfirm(CompletableFuture<Void> future) {
            this.future = future;
            this.createdAt = System.currentTimeMillis();
        }
    }
}
