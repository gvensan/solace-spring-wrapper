package com.solace.wrapper.consumer;

import com.solace.messaging.MessagingService;
import com.solace.messaging.PersistentMessageReceiverBuilder;
import com.solace.messaging.receiver.*;
import com.solace.messaging.config.MessageAcknowledgementConfiguration.Outcome;
import com.solace.messaging.config.MissingResourcesCreationConfiguration.MissingResourcesCreationStrategy;
import com.solace.messaging.resources.Queue;
import com.solace.messaging.resources.TopicSubscription;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.exception.SolaceConsumerException;
import com.solace.wrapper.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enhanced consumer supporting both persistent (queue-based) and direct (topic-based) messaging.
 * Automatically detects messaging mode and handles queue creation intelligently.
 */
public class SolaceConsumer<T> implements MessageReceiver.MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(SolaceConsumer.class);

    private final String consumerId;
    private final String clientName;
    private final String queueName;
    private final String[] topics;
    private final MessagingMode messagingMode;
    private final boolean autoCreateQueue;
    private final AckMode ackMode; // only applicable for persistent
    private final Class<T> messageType;
    private final SolaceMessageHandler<T> messageHandler;
    private final SolaceManualAckMessageHandler<T> manualAckHandler;
    private final SolaceConnectionManager connectionManager;
    private final MessageSerializer messageSerializer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    // Local backoff retry settings (opt-in via annotation/manager)
    private int localMaxAttempts = 1;
    private long localBackoffInitialMs = 200;
    private double localBackoffMultiplier = 2.0;
    private long localBackoffMaxMs = 2000;
    // Configurable termination timeout (default 5 seconds)
    private long terminationTimeoutMs = 5000;

    private volatile MessagingService messagingService;
    private volatile PersistentMessageReceiver persistentReceiver;
    private volatile DirectMessageReceiver directReceiver;

    /**
     * Messaging mode enumeration.
     */
    public enum MessagingMode {
        PERSISTENT,  // Queue-based messaging with guaranteed delivery
        DIRECT       // Topic-based messaging with best-effort delivery
    }

    /**
     * Enhanced constructor supporting both persistent and direct messaging.
     * Automatically determines messaging mode based on parameters.
     */
    public SolaceConsumer(String consumerId, String queueName, String[] topics,
                         MessagingMode messagingMode, boolean autoCreateQueue,
                         AckMode ackMode,
                         Class<T> messageType, SolaceMessageHandler<T> messageHandler,
                         SolaceConnectionManager connectionManager, MessageSerializer messageSerializer,
                         String clientName) {
        this.consumerId = consumerId;
        this.clientName = clientName;
        this.queueName = queueName;
        this.topics = topics != null ? topics : new String[0];
        this.messagingMode = determineMessagingMode(messagingMode, queueName, this.topics);
        this.autoCreateQueue = autoCreateQueue;
        this.messageType = messageType;
        this.ackMode = ackMode == null ? AckMode.AUTO : ackMode;
        this.messageHandler = messageHandler;
        this.manualAckHandler = null;
        this.connectionManager = connectionManager;
        this.messageSerializer = messageSerializer;
        
        // Validate configuration
        validateConfiguration();
        if (this.ackMode == AckMode.MANUAL && this.manualAckHandler == null) {
            logger.info("Manual ack requested for consumer {} without a manual-ack handler; falling back to auto-ack behavior.", consumerId);
        }
    }

    public SolaceConsumer(String consumerId, String queueName, String[] topics,
                         MessagingMode messagingMode, boolean autoCreateQueue,
                         AckMode ackMode,
                         Class<T> messageType, SolaceMessageHandler<T> messageHandler,
                         SolaceConnectionManager connectionManager, MessageSerializer messageSerializer) {
        this(consumerId, queueName, topics, messagingMode, autoCreateQueue, ackMode, messageType,
                messageHandler, connectionManager, messageSerializer, null);
    }

    /**
     * Constructor for manual-ack handlers.
     */
    public SolaceConsumer(String consumerId, String queueName, String[] topics,
                         MessagingMode messagingMode, boolean autoCreateQueue,
                         AckMode ackMode,
                         Class<T> messageType, SolaceManualAckMessageHandler<T> manualAckHandler,
                         SolaceConnectionManager connectionManager, MessageSerializer messageSerializer,
                         String clientName) {
        this.consumerId = consumerId;
        this.clientName = clientName;
        this.queueName = queueName;
        this.topics = topics != null ? topics : new String[0];
        this.messagingMode = determineMessagingMode(messagingMode, queueName, this.topics);
        this.autoCreateQueue = autoCreateQueue;
        this.messageType = messageType;
        this.ackMode = AckMode.MANUAL;
        this.messageHandler = null;
        this.manualAckHandler = manualAckHandler;
        this.connectionManager = connectionManager;
        this.messageSerializer = messageSerializer;

        // Validate configuration
        validateConfiguration();
        if (ackMode != null && ackMode != AckMode.MANUAL) {
            logger.warn("Manual-ack handler provided for consumer {} with ackMode {}. Forcing MANUAL mode.",
                    consumerId, ackMode);
        }
    }

    public SolaceConsumer(String consumerId, String queueName, String[] topics,
                         MessagingMode messagingMode, boolean autoCreateQueue,
                         AckMode ackMode,
                         Class<T> messageType, SolaceManualAckMessageHandler<T> manualAckHandler,
                         SolaceConnectionManager connectionManager, MessageSerializer messageSerializer) {
        this(consumerId, queueName, topics, messagingMode, autoCreateQueue, ackMode, messageType,
                manualAckHandler, connectionManager, messageSerializer, null);
    }

    /**
     * Allows configuring local backoff retry behavior per consumer.
     */
    public SolaceConsumer<T> withLocalBackoff(int maxAttempts, long initialMs, double multiplier, long maxMs) {
        this.localMaxAttempts = Math.max(1, maxAttempts);
        this.localBackoffInitialMs = Math.max(0, initialMs);
        this.localBackoffMultiplier = multiplier <= 0 ? 2.0 : multiplier;
        this.localBackoffMaxMs = Math.max(initialMs, maxMs);
        return this;
    }

    /**
     * Sets the termination timeout for stopping receivers.
     * @param timeoutMs timeout in milliseconds (default 5000)
     */
    public SolaceConsumer<T> withTerminationTimeout(long timeoutMs) {
        this.terminationTimeoutMs = Math.max(100, timeoutMs);
        return this;
    }

    /**
     * Backward compatible constructor for existing code.
     */
    public SolaceConsumer(String consumerId, String queueName, Class<T> messageType,
                         SolaceMessageHandler<T> messageHandler,
                         SolaceConnectionManager connectionManager,
                         MessageSerializer messageSerializer) {
        this(consumerId, queueName, new String[0], MessagingMode.PERSISTENT, true,
             AckMode.AUTO, messageType, messageHandler, connectionManager, messageSerializer, null);
    }

    /**
     * Smart mode detection based on annotation parameters.
     */
    private MessagingMode determineMessagingMode(MessagingMode requestedMode, String queueName, String[] topics) {
        if (requestedMode != MessagingMode.PERSISTENT && requestedMode != MessagingMode.DIRECT) {
            // AUTO mode - determine based on parameters
            if (!queueName.isEmpty() && topics.length == 0) {
                return MessagingMode.PERSISTENT;
            } else if (queueName.isEmpty() && topics.length > 0) {
                return MessagingMode.DIRECT;
            } else if (!queueName.isEmpty() && topics.length > 0) {
                // Both specified - default to persistent with topic subscriptions
                return MessagingMode.PERSISTENT;
            } else {
                throw new IllegalArgumentException("Either queue or topics must be specified");
            }
        }
        return requestedMode;
    }

    /**
     * Validates the consumer configuration.
     */
    private void validateConfiguration() {
        if (messagingMode == MessagingMode.PERSISTENT && queueName.isEmpty()) {
            throw new IllegalArgumentException("Queue name is required for persistent messaging mode");
        }
        if (messagingMode == MessagingMode.DIRECT && topics.length == 0) {
            throw new IllegalArgumentException("Topics are required for direct messaging mode");
        }
        if (consumerId == null || consumerId.trim().isEmpty()) {
            throw new IllegalArgumentException("Consumer ID cannot be null or empty");
        }
        if (messageHandler == null && manualAckHandler == null) {
            throw new IllegalArgumentException("Message handler cannot be null");
        }
        if (messageHandler != null && manualAckHandler != null) {
            throw new IllegalArgumentException("Only one message handler type can be set");
        }
    }

    /**
     * Starts the consumer with smart mode detection and queue creation.
     */
    public synchronized void start() {
        if (isRunning.get() || isShutdown.get()) {
            return;
        }

        try {
            messagingService = connectionManager.createConsumerService(consumerId, clientName);
            
            // Verify connection is healthy before proceeding
            if (messagingService == null || !messagingService.isConnected()) {
                throw new SolaceConsumerException("Messaging service is not connected");
            }
            
            if (messagingMode == MessagingMode.PERSISTENT) {
                startPersistentConsumer();
            } else {
                startDirectConsumer();
            }
            
            isRunning.set(true);
            logger.info("Started Solace consumer {} in {} mode for: {}", 
                       consumerId, messagingMode, getDestinationDescription());
            
        } catch (Exception e) {
            logger.error("Failed to start consumer {} in {} mode: {}", 
                        consumerId, messagingMode, e.getMessage(), e);
            throw new SolaceConsumerException("Failed to start consumer", e);
        }
    }

    /**
     * Starts persistent consumer with smart queue creation and topic subscriptions.
     */
    private void startPersistentConsumer() throws Exception {
        Queue queue = Queue.durableExclusiveQueue(queueName);
        
        // Attempt queue creation if autoCreateQueue is enabled using documented API (requires supported library version)
        final var builderBase = messagingService.createPersistentMessageReceiverBuilder();
        if (autoCreateQueue) {
            logger.debug("Auto-create queue enabled for {} - enabling MissingResourcesCreationStrategy.CREATE_ON_START", queueName);
            try {
                builderBase.withMissingResourcesCreationStrategy(
                        MissingResourcesCreationStrategy.CREATE_ON_START);
            } catch (Throwable t) {
                logger.warn("MissingResourcesCreationStrategy not available: {}. Ensure solace-messaging-client >= 1.8.2 or provision queue via SEMP.", t.toString());
            }
        } else {
            logger.debug("Auto-create queue disabled for {} - queue must exist on broker", queueName);
        }
        
        // Configure acknowledgment behavior based on configured ack mode
        applyAckMode(builderBase);

        // Create persistent message receiver with topic subscriptions if specified
        if (topics.length > 0) {
            TopicSubscription[] topicSubscriptions = Arrays.stream(topics)
                .map(TopicSubscription::of)
                .toArray(TopicSubscription[]::new);
            persistentReceiver = builderBase
                .withSubscriptions(topicSubscriptions)
                .build(queue);
            logger.debug("Added topic subscriptions to queue {}: {}", queueName, Arrays.toString(topics));
        } else {
            persistentReceiver = builderBase.build(queue);
        }
        persistentReceiver.setReceiveFailureListener(failedReceiveEvent -> {
            logger.error("Failed to receive message for persistent consumer {}: {}", 
                       consumerId, failedReceiveEvent.getException().getMessage());
        });

        // Set message handler and start
        persistentReceiver.receiveAsync(this);
        persistentReceiver.start();
        
        logger.info("Started persistent consumer {} for queue: {} with {} topic subscriptions", 
                   consumerId, queueName, topics.length);
    }

    /**
     * Starts direct consumer with topic subscriptions.
     */
    private void startDirectConsumer() throws Exception {
        if (topics.length == 0) {
            throw new IllegalArgumentException("Topics are required for direct messaging mode");
        }
        
        // Create topic subscriptions
        TopicSubscription[] topicSubscriptions = Arrays.stream(topics)
            .map(TopicSubscription::of)
            .toArray(TopicSubscription[]::new);
        
        // Create direct message receiver
        directReceiver = messagingService.createDirectMessageReceiverBuilder()
            .withSubscriptions(topicSubscriptions)
            .build();
        
        // Set message handler and start
        directReceiver.receiveAsync(this);
        directReceiver.start();
        
        logger.info("Started direct consumer {} for topics: {}", consumerId, Arrays.toString(topics));
    }

    /**
     * Gets a description of the consumer destination for logging.
     */
    private String getDestinationDescription() {
        if (messagingMode == MessagingMode.PERSISTENT) {
            if (topics.length > 0) {
                return String.format("queue=%s, topics=%s", queueName, Arrays.toString(topics));
            } else {
                return String.format("queue=%s", queueName);
            }
        } else {
            return String.format("topics=%s", Arrays.toString(topics));
        }
    }

    /**
     * Stops the consumer.
     */
    public synchronized void stop() {
        if (!isRunning.get()) {
            return;
        }

        isRunning.set(false);

        try {
            if (persistentReceiver != null) {
                persistentReceiver.terminate(terminationTimeoutMs);
                persistentReceiver = null;
            }

            if (directReceiver != null) {
                directReceiver.terminate(terminationTimeoutMs);
                directReceiver = null;
            }

            if (messagingService != null) {
                connectionManager.removeConsumerService(consumerId);
                messagingService = null;
            }

            logger.info("Stopped Solace consumer {} in {} mode", consumerId, messagingMode);

        } catch (Exception e) {
            logger.error("Error stopping consumer {} in {} mode: {}",
                        consumerId, messagingMode, e.getMessage(), e);
        }
    }

    /**
     * Shuts down the consumer permanently.
     */
    public synchronized void shutdown() {
        isShutdown.set(true);
        stop();
        logger.info("Shutdown Solace consumer {} for queue: {}", consumerId, queueName);
    }

    @Override
    public void onMessage(InboundMessage inboundMessage) {
        if (!isRunning.get() || isShutdown.get()) {
            return;
        }

        // Capture local references to avoid race conditions with stop()
        final PersistentMessageReceiver localPersistentReceiver = this.persistentReceiver;

        SolaceAckContext ackContext = null;
        boolean useManualAckHandler = ackMode == AckMode.MANUAL && manualAckHandler != null;
        try {
            // Deserialize once per attempt if immutable; otherwise re-deserialize per attempt
            // Here we deserialize once for efficiency.
            T messageObject = messageSerializer.deserialize(inboundMessage, messageType);

            if (useManualAckHandler) {
                ackContext = createAckContext(inboundMessage);
                boolean processed = processWithLocalBackoffManual(messageObject, inboundMessage, ackContext);
                handleManualAckOutcome(processed, ackContext, inboundMessage);
            } else {
                boolean processed = processWithLocalBackoff(messageObject, inboundMessage);
                handleAutoAckOutcome(processed, inboundMessage);
            }

        } catch (Exception e) {
            logger.error("Error processing message from: {}", getDestinationDescription(), e);

            // Handle acknowledgment based on messaging mode using local reference
            if (messagingMode == MessagingMode.PERSISTENT && localPersistentReceiver != null) {
                if (useManualAckHandler) {
                    if (ackContext == null || ackContext.getStatus() == SolaceAckContext.Status.NONE) {
                        try {
                            localPersistentReceiver.settle(inboundMessage, Outcome.FAILED);
                        } catch (Exception ackException) {
                            logger.error("Failed to negative acknowledge persistent message", ackException);
                        }
                    }
                } else if (ackMode == AckMode.MANUAL) {
                    // Negative acknowledge the message so it can be redelivered
                    try {
                        localPersistentReceiver.settle(inboundMessage, Outcome.FAILED);
                    } catch (Exception ackException) {
                        logger.error("Failed to negative acknowledge persistent message", ackException);
                    }
                } else {
                    logger.warn("Error processing message under AUTO ack; no manual nAck possible");
                }
            } else {
                // Direct messages are lost on error (no acknowledgment mechanism)
                logger.warn("Direct message lost due to processing error: {}", e.getMessage());
            }
        }
    }

    private SolaceAckContext createAckContext(InboundMessage inboundMessage) {
        return new SolaceAckContext(
            () -> {
                if (messagingMode != MessagingMode.PERSISTENT || persistentReceiver == null) {
                    logger.warn("Ack requested for non-persistent consumer {}", consumerId);
                    return;
                }
                persistentReceiver.ack(inboundMessage);
            },
            () -> {
                if (messagingMode != MessagingMode.PERSISTENT || persistentReceiver == null) {
                    logger.warn("Fail requested for non-persistent consumer {}", consumerId);
                    return;
                }
                persistentReceiver.settle(inboundMessage, Outcome.FAILED);
            }
        );
    }

    private void handleAutoAckOutcome(boolean processed, InboundMessage inboundMessage) {
        // Acknowledge based on messaging mode if processed
        if (processed) {
            if (messagingMode == MessagingMode.PERSISTENT && persistentReceiver != null) {
                if (ackMode == AckMode.MANUAL) {
                    persistentReceiver.ack(inboundMessage);
                    logger.debug("Processed and acknowledged persistent message from: {}", getDestinationDescription());
                } else {
                    logger.debug("Processed persistent message (AUTO ack) from: {}", getDestinationDescription());
                }
            } else {
                logger.debug("Processed direct message from: {}", getDestinationDescription());
            }
        } else {
            // Local attempts exhausted; for persistent, nack to trigger redelivery
            if (messagingMode == MessagingMode.PERSISTENT && persistentReceiver != null) {
                if (ackMode == AckMode.MANUAL) {
                    try {
                        persistentReceiver.settle(inboundMessage, Outcome.FAILED);
                    } catch (Exception ackException) {
                        logger.error("Failed to negative acknowledge persistent message", ackException);
                    }
                } else {
                    logger.warn("Persistent message failed under AUTO ack; cannot nAck explicitly");
                }
            } else {
                logger.warn("Direct message failed after local attempts; dropping (no ack model)");
            }
        }
    }

    private void handleManualAckOutcome(boolean processed, SolaceAckContext ackContext, InboundMessage inboundMessage) {
        if (!processed) {
            if (ackContext.getStatus() == SolaceAckContext.Status.NONE
                    && messagingMode == MessagingMode.PERSISTENT
                    && persistentReceiver != null) {
                try {
                    persistentReceiver.settle(inboundMessage, Outcome.FAILED);
                } catch (Exception ackException) {
                    logger.error("Failed to negative acknowledge persistent message", ackException);
                }
            }
            return;
        }

        if (ackContext.getStatus() == SolaceAckContext.Status.NONE) {
            logger.warn("Manual-ack handler completed without ack/fail for {}", getDestinationDescription());
        }
    }

    /**
     * Apply ack mode to persistent receiver builder in a version-tolerant way.
     */
    private void applyAckMode(PersistentMessageReceiverBuilder builderBase) {
        try {
            if (ackMode == AckMode.MANUAL) {
                // Client-controlled ack
                builderBase.withMessageClientAcknowledgement();
                try {
                    builderBase.withRequiredMessageClientOutcomeOperationSupport(Outcome.FAILED);
                } catch (Throwable t) {
                    logger.debug("Outcome support configuration not available, relying on defaults: {}", t.toString());
                }
            } else {
                // Library-managed ack
                builderBase.withMessageAutoAcknowledgement();
            }
        } catch (Throwable t) {
            logger.debug("Ack configuration methods not available, relying on defaults: {}", t.toString());
        }
    }

    private boolean processWithLocalBackoff(T messageObject, InboundMessage inboundMessage) {
        int attempts = Math.max(1, localMaxAttempts);
        Exception last = null;
        for (int i = 0; i < attempts; i++) {
            try {
                messageHandler.handleMessage(messageObject, inboundMessage);
                return true;
            } catch (Exception ex) {
                last = ex;
                if (i < attempts - 1) {
                    long sleepMs = computeBackoffDelay(i);
                    logger.warn("Handler failed (attempt {}/{}). Backing off {} ms", i + 1, attempts, sleepMs, ex);
                    try { Thread.sleep(sleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }
        }
        logger.error("Handler failed after {} attempts", attempts, last);
        return false;
    }

    private boolean processWithLocalBackoffManual(T messageObject, InboundMessage inboundMessage,
                                                  SolaceAckContext ackContext) {
        int attempts = Math.max(1, localMaxAttempts);
        Exception last = null;
        for (int i = 0; i < attempts; i++) {
            try {
                manualAckHandler.handleMessage(messageObject, inboundMessage, ackContext);
                return true;
            } catch (Exception ex) {
                last = ex;
                if (ackContext.isCompleted()) {
                    logger.warn("Manual-ack handler threw after completing ack/fail for {}", getDestinationDescription(), ex);
                    return true;
                }
                if (i < attempts - 1) {
                    long sleepMs = computeBackoffDelay(i);
                    logger.warn("Handler failed (attempt {}/{}). Backing off {} ms", i + 1, attempts, sleepMs, ex);
                    try { Thread.sleep(sleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }
        }
        logger.error("Handler failed after {} attempts", attempts, last);
        return false;
    }

    private long computeBackoffDelay(int attemptIndex) {
        // attemptIndex starts at 0
        double delay = localBackoffInitialMs * Math.pow(localBackoffMultiplier, attemptIndex);
        delay = Math.min(delay, localBackoffMaxMs);
        return (long) delay;
    }

    /**
     * Handle persistent receiver termination.
     */
    public void onTermination(PersistentMessageReceiver receiver) {
        logger.warn("Persistent consumer {} terminated", consumerId);
        handleReceiverTermination();
    }

    /**
     * Handle direct receiver termination.
     */
    public void onTermination(DirectMessageReceiver receiver) {
        logger.warn("Direct consumer {} terminated", consumerId);
        handleReceiverTermination();
    }

    /**
     * Common termination handling logic.
     * Disabled automatic restart to prevent connection proliferation.
     */
    private void handleReceiverTermination() {
        // Log termination but don't automatically restart to prevent connection issues
        if (isRunning.get() && !isShutdown.get()) {
            logger.warn("Consumer {} terminated - automatic restart disabled to prevent connection proliferation", consumerId);
            isRunning.set(false);
        }
    }

    // Getters
    public String getConsumerId() {
        return consumerId;
    }

    public String getQueueName() {
        return queueName;
    }

    public String[] getTopics() {
        return topics.clone(); // Return defensive copy
    }

    public MessagingMode getMessagingMode() {
        return messagingMode;
    }

    public boolean isAutoCreateQueue() {
        return autoCreateQueue;
    }

    public Class<T> getMessageType() {
        return messageType;
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    /**
     * Gets a human-readable description of the consumer configuration.
     */
    public String getDescription() {
        return String.format("SolaceConsumer{id='%s', mode=%s, %s}", 
                           consumerId, messagingMode, getDestinationDescription());
    }

    /**
     * Ack mode used by this consumer (persistent only).
     */
    public enum AckMode { AUTO, MANUAL }
}
