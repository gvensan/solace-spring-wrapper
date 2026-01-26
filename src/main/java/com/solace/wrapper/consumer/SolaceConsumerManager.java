package com.solace.wrapper.consumer;

import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.exception.SolaceConsumerException;
import com.solace.wrapper.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manager for handling multiple Solace consumers.
 */
@Service
public class SolaceConsumerManager {

    private static final Logger logger = LoggerFactory.getLogger(SolaceConsumerManager.class);

    private final SolaceConnectionManager connectionManager;
    private final MessageSerializer messageSerializer;
    private final Map<String, SolaceConsumer<?>> consumers = new ConcurrentHashMap<>();
    private final AtomicInteger consumerCounter = new AtomicInteger(0);

    public SolaceConsumerManager(SolaceConnectionManager connectionManager,
                                MessageSerializer messageSerializer) {
        this.connectionManager = connectionManager;
        this.messageSerializer = messageSerializer;
    }

    /**
     * Creates and starts a consumer for a specific queue.
     *
     * @param queueName The name of the queue to consume from
     * @param messageType The class type of the expected message
     * @param messageHandler The handler for processing received messages
     * @param <T> The type of the message
     * @return The consumer ID for managing the consumer later
     */
    public <T> String createConsumer(String queueName, Class<T> messageType,
                                   SolaceMessageHandler<T> messageHandler) {
        String consumerId = generateConsumerId(queueName);
        return createConsumer(consumerId, queueName, messageType, messageHandler);
    }

    /**
     * Creates and starts a consumer with a specific consumer ID.
     *
     * @param consumerId The unique identifier for this consumer
     * @param queueName The name of the queue to consume from
     * @param messageType The class type of the expected message
     * @param messageHandler The handler for processing received messages
     * @param <T> The type of the message
     * @return The consumer ID
     */
    public <T> String createConsumer(String consumerId, String queueName, Class<T> messageType,
                                   SolaceMessageHandler<T> messageHandler) {
        if (consumers.containsKey(consumerId)) {
            throw new SolaceConsumerException("Consumer with ID already exists: " + consumerId);
        }

        try {
            // Create the consumer with proper type casting
            SolaceConsumer<T> consumer = new SolaceConsumer<>(
                consumerId, queueName, messageType, messageHandler,
                connectionManager, messageSerializer
            );
            
            consumers.put(consumerId, consumer);
            consumer.start();
            
            logger.info("Created and started consumer {} for queue: {}", consumerId, queueName);
            return consumerId;
            
        } catch (Exception e) {
            consumers.remove(consumerId);
            logger.error("Failed to create consumer {} for queue: {}", consumerId, queueName, e);
            throw new SolaceConsumerException("Failed to create consumer", e);
        }
    }

    /**
     * Creates and starts a consumer using raw types (for annotation processor compatibility).
     * This method bypasses generic type checking to work with annotation processors
     * where generic type information may not be available at compile time.
     *
     * @param consumerId The unique identifier for this consumer
     * @param queueName The name of the queue to consume from
     * @param messageType The class type of the expected message
     * @param messageHandler The handler for processing received messages
     * @return The consumer ID
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public String createConsumerRaw(String consumerId, String queueName, Class<?> messageType,
                                   SolaceMessageHandler<?> messageHandler) {
        if (consumers.containsKey(consumerId)) {
            throw new SolaceConsumerException("Consumer with ID already exists: " + consumerId);
        }

        try {
            // Create the consumer with raw types to handle generic type erasure issues
            SolaceConsumer consumer = new SolaceConsumer(
                consumerId, queueName, messageType, messageHandler,
                connectionManager, messageSerializer
            );
            
            consumers.put(consumerId, consumer);
            consumer.start();
            
            logger.info("Created and started raw consumer {} for queue: {} with message type: {}", 
                       consumerId, queueName, messageType.getSimpleName());
            return consumerId;
            
        } catch (Exception e) {
            consumers.remove(consumerId);
            logger.error("Failed to create raw consumer {} for queue: {}", consumerId, queueName, e);
            throw new SolaceConsumerException("Failed to create consumer", e);
        }
    }

    /**
     * Enhanced method for creating consumers with full feature support.
     * Supports both persistent and direct messaging modes with topic subscriptions.
     *
     * @param consumerId The unique identifier for this consumer
     * @param queueName The name of the queue to consume from (optional for direct mode)
     * @param topics Topics to subscribe to (optional for persistent mode)
     * @param messagingMode The messaging mode (AUTO, PERSISTENT, or DIRECT)
     * @param autoCreateQueue Whether to auto-create queue if it doesn't exist
     * @param messageType The class type of the expected message
     * @param messageHandler The handler for processing received messages
     * @return The consumer ID
     */
    @SuppressWarnings({})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                           Class<?> messageType,
                                           SolaceMessageHandler<?> messageHandler) {
        return createEnhancedConsumerRaw(consumerId, queueName, topics, messagingMode, autoCreateQueue,
                ackMode, messageType, messageHandler, null,
                1, 200, 2.0, 2000);
    }

    @SuppressWarnings({})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                           Class<?> messageType,
                                           SolaceManualAckMessageHandler<?> messageHandler) {
        return createEnhancedConsumerRaw(consumerId, queueName, topics, messagingMode, autoCreateQueue,
                ackMode, messageType, messageHandler, null,
                1, 200, 2.0, 2000);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                           Class<?> messageType,
                                           SolaceMessageHandler<?> messageHandler,
                                           int localMaxAttempts,
                                           long localBackoffInitialMs,
                                           double localBackoffMultiplier,
                                           long localBackoffMaxMs) {
        return createEnhancedConsumerRaw(consumerId, queueName, topics, messagingMode, autoCreateQueue,
                ackMode, messageType, messageHandler, null, localMaxAttempts, localBackoffInitialMs,
                localBackoffMultiplier, localBackoffMaxMs);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                           Class<?> messageType,
                                           SolaceManualAckMessageHandler<?> messageHandler,
                                           int localMaxAttempts,
                                           long localBackoffInitialMs,
                                           double localBackoffMultiplier,
                                           long localBackoffMaxMs) {
        return createEnhancedConsumerRaw(consumerId, queueName, topics, messagingMode, autoCreateQueue,
                ackMode, messageType, messageHandler, null, localMaxAttempts, localBackoffInitialMs,
                localBackoffMultiplier, localBackoffMaxMs);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                           Class<?> messageType,
                                           SolaceMessageHandler<?> messageHandler,
                                           String clientName,
                                           int localMaxAttempts,
                                           long localBackoffInitialMs,
                                           double localBackoffMultiplier,
                                           long localBackoffMaxMs) {
        return createEnhancedConsumerRaw(consumerId, queueName, topics, messagingMode, autoCreateQueue, ackMode,
                messageType, messageHandler, clientName, localMaxAttempts, localBackoffInitialMs,
                localBackoffMultiplier, localBackoffMaxMs, true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                           Class<?> messageType,
                                           SolaceMessageHandler<?> messageHandler,
                                           String clientName,
                                           int localMaxAttempts,
                                           long localBackoffInitialMs,
                                           double localBackoffMultiplier,
                                           long localBackoffMaxMs,
                                           boolean autoStart) {
        if (consumers.containsKey(consumerId)) {
            throw new SolaceConsumerException("Consumer with ID already exists: " + consumerId);
        }

        try {
            // Convert annotation MessagingMode to consumer MessagingMode
            SolaceConsumer.MessagingMode consumerMode = convertMessagingMode(messagingMode, queueName, topics);
            
            // Create the enhanced consumer with raw types
            SolaceConsumer.AckMode consumerAckMode = convertAckMode(ackMode);
            SolaceConsumer consumer = new SolaceConsumer(
                consumerId, queueName, topics, consumerMode, autoCreateQueue,
                consumerAckMode, messageType, messageHandler, connectionManager, messageSerializer, clientName
            );
            consumer.withLocalBackoff(localMaxAttempts, localBackoffInitialMs, localBackoffMultiplier, localBackoffMaxMs);
            // Apply termination timeout from properties
            consumer.withTerminationTimeout(connectionManager.getProperties().getTerminationTimeoutMs());

            consumers.put(consumerId, consumer);
            if (autoStart) {
                consumer.start();
                logger.info("Created and started enhanced consumer {} in {} mode with message type: {}",
                           consumerId, consumerMode, messageType.getSimpleName());
            } else {
                logger.info("Registered enhanced consumer {} in {} mode with message type: {} (autoStart=false)",
                           consumerId, consumerMode, messageType.getSimpleName());
            }
            return consumerId;
            
        } catch (Exception e) {
            consumers.remove(consumerId);
            logger.error("Failed to create enhanced consumer {}: {}", consumerId, e.getMessage(), e);
            throw new SolaceConsumerException("Failed to create enhanced consumer", e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                           Class<?> messageType,
                                           SolaceManualAckMessageHandler<?> messageHandler,
                                           String clientName,
                                           int localMaxAttempts,
                                           long localBackoffInitialMs,
                                           double localBackoffMultiplier,
                                           long localBackoffMaxMs) {
        return createEnhancedConsumerRaw(consumerId, queueName, topics, messagingMode, autoCreateQueue, ackMode,
                messageType, messageHandler, clientName, localMaxAttempts, localBackoffInitialMs,
                localBackoffMultiplier, localBackoffMaxMs, true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                           Class<?> messageType,
                                           SolaceManualAckMessageHandler<?> messageHandler,
                                           String clientName,
                                           int localMaxAttempts,
                                           long localBackoffInitialMs,
                                           double localBackoffMultiplier,
                                           long localBackoffMaxMs,
                                           boolean autoStart) {
        if (consumers.containsKey(consumerId)) {
            throw new SolaceConsumerException("Consumer with ID already exists: " + consumerId);
        }

        try {
            // Convert annotation MessagingMode to consumer MessagingMode
            SolaceConsumer.MessagingMode consumerMode = convertMessagingMode(messagingMode, queueName, topics);

            SolaceConsumer.AckMode consumerAckMode = convertAckMode(ackMode);
            if (consumerAckMode != SolaceConsumer.AckMode.MANUAL) {
                logger.warn("Manual-ack handler provided for consumer {} with ackMode {}. Forcing MANUAL mode.",
                        consumerId, ackMode);
                consumerAckMode = SolaceConsumer.AckMode.MANUAL;
            }
            SolaceConsumer consumer = new SolaceConsumer(
                consumerId, queueName, topics, consumerMode, autoCreateQueue,
                consumerAckMode, messageType, messageHandler, connectionManager, messageSerializer, clientName
            );
            consumer.withLocalBackoff(localMaxAttempts, localBackoffInitialMs, localBackoffMultiplier, localBackoffMaxMs);
            // Apply termination timeout from properties
            consumer.withTerminationTimeout(connectionManager.getProperties().getTerminationTimeoutMs());

            consumers.put(consumerId, consumer);
            if (autoStart) {
                consumer.start();
                logger.info("Created and started enhanced manual-ack consumer {} in {} mode with message type: {}",
                           consumerId, consumerMode, messageType.getSimpleName());
            } else {
                logger.info("Registered enhanced manual-ack consumer {} in {} mode with message type: {} (autoStart=false)",
                           consumerId, consumerMode, messageType.getSimpleName());
            }
            return consumerId;

        } catch (Exception e) {
            consumers.remove(consumerId);
            logger.error("Failed to create enhanced manual-ack consumer {}: {}", consumerId, e.getMessage(), e);
            throw new SolaceConsumerException("Failed to create enhanced consumer", e);
        }
    }

    /**
     * Backward-compatible overload without ackMode parameter; defaults to AUTO.
     */
    @SuppressWarnings({})
    public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                           com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                           boolean autoCreateQueue,
                                           Class<?> messageType,
                                           SolaceMessageHandler<?> messageHandler) {
        return createEnhancedConsumerRaw(consumerId, queueName, topics, messagingMode, autoCreateQueue,
                 com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO, messageType, messageHandler);
    }

    private SolaceConsumer.AckMode convertAckMode(
            com.solace.wrapper.annotation.SolaceConsumer.AckMode annotationAckMode) {
        if (annotationAckMode == null) return SolaceConsumer.AckMode.AUTO;
        switch (annotationAckMode) {
            case MANUAL:
                return SolaceConsumer.AckMode.MANUAL;
            case AUTO:
            default:
                return SolaceConsumer.AckMode.AUTO;
        }
    }

    /**
     * Converts annotation MessagingMode to consumer MessagingMode with smart detection.
     */
    private SolaceConsumer.MessagingMode convertMessagingMode(
            com.solace.wrapper.annotation.SolaceConsumer.MessagingMode annotationMode,
            String queueName, String[] topics) {
        
        switch (annotationMode) {
            case PERSISTENT:
                return SolaceConsumer.MessagingMode.PERSISTENT;
            case DIRECT:
                return SolaceConsumer.MessagingMode.DIRECT;
            case AUTO:
            default:
                // Smart detection based on parameters
                if (!queueName.isEmpty() && topics.length == 0) {
                    return SolaceConsumer.MessagingMode.PERSISTENT;
                } else if (queueName.isEmpty() && topics.length > 0) {
                    return SolaceConsumer.MessagingMode.DIRECT;
                } else if (!queueName.isEmpty() && topics.length > 0) {
                    // Both specified - default to persistent with topic subscriptions
                    return SolaceConsumer.MessagingMode.PERSISTENT;
                } else {
                    throw new IllegalArgumentException("Either queue or topics must be specified for AUTO mode");
                }
        }
    }

    /**
     * Stops a consumer by ID.
     *
     * @param consumerId The consumer ID to stop
     */
    public void stopConsumer(String consumerId) {
        SolaceConsumer<?> consumer = consumers.get(consumerId);
        if (consumer != null) {
            consumer.stop();
            logger.info("Stopped consumer: {}", consumerId);
        } else {
            logger.warn("Consumer not found: {}", consumerId);
        }
    }

    /**
     * Removes and shuts down a consumer by ID.
     *
     * @param consumerId The consumer ID to remove
     */
    public void removeConsumer(String consumerId) {
        SolaceConsumer<?> consumer = consumers.remove(consumerId);
        if (consumer != null) {
            consumer.shutdown();
            logger.info("Removed consumer: {}", consumerId);
        } else {
            logger.warn("Consumer not found: {}", consumerId);
        }
    }

    /**
     * Restarts a consumer by ID.
     *
     * @param consumerId The consumer ID to restart
     */
    public void restartConsumer(String consumerId) {
        SolaceConsumer<?> consumer = consumers.get(consumerId);
        if (consumer != null) {
            consumer.stop();
            consumer.start();
            logger.info("Restarted consumer: {}", consumerId);
        } else {
            logger.warn("Consumer not found: {}", consumerId);
        }
    }

    /**
     * Gets the status of a consumer.
     *
     * @param consumerId The consumer ID
     * @return ConsumerStatus containing the consumer's current state
     */
    public ConsumerStatus getConsumerStatus(String consumerId) {
        SolaceConsumer<?> consumer = consumers.get(consumerId);
        if (consumer == null) {
            return null;
        }
        
        return new ConsumerStatus(
            consumerId,
            consumer.getQueueName(),
            consumer.getTopics(),
            consumer.getMessagingMode().toString(),
            consumer.getMessageType().getSimpleName(),
            consumer.isRunning(),
            consumer.isShutdown()
        );
    }

    /**
     * Gets the status of all consumers.
     *
     * @return Map of consumer ID to ConsumerStatus
     */
    public Map<String, ConsumerStatus> getAllConsumerStatuses() {
        Map<String, ConsumerStatus> statuses = new ConcurrentHashMap<>();
        consumers.forEach((id, consumer) -> {
            statuses.put(id, new ConsumerStatus(
                id,
                consumer.getQueueName(),
                consumer.getTopics(),
                consumer.getMessagingMode().toString(),
                consumer.getMessageType().getSimpleName(),
                consumer.isRunning(),
                consumer.isShutdown()
            ));
        });
        return statuses;
    }

    /**
     * Stops all consumers.
     */
    public void stopAllConsumers() {
        logger.info("Stopping all consumers...");
        consumers.values().forEach(SolaceConsumer::stop);
        logger.info("All consumers stopped");
    }

    /**
     * Starts all stopped consumers.
     */
    public void startAllConsumers() {
        logger.info("Starting all consumers...");
        consumers.values().forEach(consumer -> {
            if (!consumer.isRunning() && !consumer.isShutdown()) {
                consumer.start();
            }
        });
        logger.info("All consumers started");
    }

    /**
     * Cleanup method called when the bean is destroyed.
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down all consumers...");
        consumers.values().forEach(SolaceConsumer::shutdown);
        consumers.clear();
        logger.info("All consumers shutdown complete");
    }

    /**
     * Generates a unique consumer ID.
     */
    private String generateConsumerId(String queueName) {
        return String.format("%s-consumer-%d", queueName, consumerCounter.incrementAndGet());
    }

    /**
     * Gets the count of active consumers.
     */
    public int getActiveConsumerCount() {
        return (int) consumers.values().stream()
            .filter(SolaceConsumer::isRunning)
            .count();
    }

    /**
     * Gets the total count of consumers.
     */
    public int getTotalConsumerCount() {
        return consumers.size();
    }

    /**
     * Checks if a consumer is running.
     *
     * @param consumerId The consumer ID to check
     * @return True if the consumer is running, false otherwise
     */
    public boolean isConsumerRunning(String consumerId) {
        SolaceConsumer<?> consumer = consumers.get(consumerId);
        return consumer != null && consumer.isRunning();
    }

    /**
     * Checks if a consumer exists.
     *
     * @param consumerId The consumer ID to check
     * @return true if the consumer exists
     */
    public boolean hasConsumer(String consumerId) {
        return consumers.containsKey(consumerId);
    }

    /**
     * Gets a list of all consumer IDs.
     *
     * @return A set of all consumer IDs
     */
    public java.util.Set<String> getConsumerIds() {
        return new java.util.HashSet<>(consumers.keySet());
    }

    /**
     * Gets consumers by queue name.
     *
     * @param queueName The queue name to search for
     * @return A list of consumer IDs for the specified queue
     */
    public java.util.List<String> getConsumersByQueue(String queueName) {
        return consumers.entrySet().stream()
            .filter(entry -> queueName.equals(entry.getValue().getQueueName()))
            .map(Map.Entry::getKey)
            .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Gets consumers by message type.
     *
     * @param messageType The message type class to search for
     * @return A list of consumer IDs for the specified message type
     */
    public java.util.List<String> getConsumersByMessageType(Class<?> messageType) {
        return consumers.entrySet().stream()
            .filter(entry -> messageType.equals(entry.getValue().getMessageType()))
            .map(Map.Entry::getKey)
            .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Enhanced status class for consumer information.
     */
    public static class ConsumerStatus {
        private final String consumerId;
        private final String queueName;
        private final String[] topics;
        private final String messagingMode;
        private final String messageType;
        private final boolean running;
        private final boolean shutdown;

        public ConsumerStatus(String consumerId, String queueName, String[] topics, String messagingMode,
                            String messageType, boolean running, boolean shutdown) {
            this.consumerId = consumerId;
            this.queueName = queueName;
            this.topics = topics != null ? topics.clone() : new String[0];
            this.messagingMode = messagingMode;
            this.messageType = messageType;
            this.running = running;
            this.shutdown = shutdown;
        }

        // Getters
        public String getConsumerId() { return consumerId; }
        public String getQueueName() { return queueName; }
        public String[] getTopics() { return topics.clone(); } // Defensive copy
        public String getMessagingMode() { return messagingMode; }
        public String getMessageType() { return messageType; }
        public boolean isRunning() { return running; }
        public boolean isShutdown() { return shutdown; }

        /**
         * Gets a description of the consumer destination.
         */
        public String getDestinationDescription() {
            StringBuilder desc = new StringBuilder();
            if (queueName != null && !queueName.isEmpty()) {
                desc.append("queue=").append(queueName);
            }
            if (topics.length > 0) {
                if (desc.length() > 0) desc.append(", ");
                desc.append("topics=").append(java.util.Arrays.toString(topics));
            }
            return desc.toString();
        }

        @Override
        public String toString() {
            return String.format("ConsumerStatus{id='%s', mode=%s, %s, type='%s', running=%s, shutdown=%s}",
                consumerId, messagingMode, getDestinationDescription(), messageType, running, shutdown);
        }
    }
}
