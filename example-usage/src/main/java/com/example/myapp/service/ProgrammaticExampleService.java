package com.example.myapp.service;

import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.publisher.MessageProperties;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.messaging.receiver.InboundMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.UUID;

/**
 * Example service demonstrating PROGRAMMATIC usage of the Solace wrapper.
 *
 * This is an alternative to annotation-based usage shown in AnnotationBasedOrderService.
 * Use this approach when you need dynamic consumer creation, runtime control over
 * consumers, or more fine-grained control over message handling.
 *
 * Key differences from annotation-based:
 * - Consumers are created at runtime in @PostConstruct
 * - Full control over consumer lifecycle (start/stop/remove)
 * - Direct access to SolacePublisher for custom publishing logic
 * - Manual cleanup required in @PreDestroy
 */
@Service
public class ProgrammaticExampleService {

    private static final Logger logger = LoggerFactory.getLogger(ProgrammaticExampleService.class);

    @Autowired
    private SolacePublisher solacePublisher;

    @Autowired
    private SolaceConsumerManager consumerManager;

    private String orderConsumerId;
    private String notificationConsumerId;

    @PostConstruct
    public void initialize() {
        logger.info("═══════════════════════════════════════════════════════════════");
        logger.info("PROGRAMMATIC EXAMPLE SERVICE INITIALIZATION");
        logger.info("═══════════════════════════════════════════════════════════════");
        logger.info("This service demonstrates programmatic consumer/publisher setup.");
        logger.info("Unlike annotation-based approaches, consumers are created manually");
        logger.info("using SolaceConsumerManager in @PostConstruct.");
        logger.info("═══════════════════════════════════════════════════════════════");
        setupConsumers();
        logger.info("ProgrammaticExampleService initialized with Solace consumers");
    }

    @PreDestroy
    public void cleanup() {
        if (orderConsumerId != null) {
            consumerManager.removeConsumer(orderConsumerId);
        }
        if (notificationConsumerId != null) {
            consumerManager.removeConsumer(notificationConsumerId);
        }
        logger.info("ProgrammaticExampleService cleanup completed");
    }

    /**
     * Set up message consumers for different types of messages.
     */
    private void setupConsumers() {
        logger.info("───────────────────────────────────────────────────────────────");
        logger.info("EXAMPLE: setupConsumers");
        logger.info("DEMONSTRATES: Programmatic consumer creation with SolaceConsumerManager");
        logger.info("───────────────────────────────────────────────────────────────");

        // Consumer for order processing
        logger.info("Creating order consumer for queue: orders-processing-queue");
        orderConsumerId = consumerManager.createConsumer(
            "orders-processing-queue",
            OrderMessage.class,
            this::handleOrderMessage
        );

        // Consumer for notifications
        logger.info("Creating notification consumer for queue: notifications-queue");
        notificationConsumerId = consumerManager.createConsumer(
            "notifications-queue",
            NotificationMessage.class,
            this::handleNotificationMessage
        );
        logger.info("Both consumers registered - orderConsumerId={}, notificationConsumerId={}",
                orderConsumerId, notificationConsumerId);
    }

    /**
     * Example: Process a customer order.
     *
     * DEMONSTRATES: Publishing to topics with MessageProperties
     *   - Topic publish for event broadcasting (orders/created)
     *   - Persistent topic publish with custom properties for reliable processing
     *
     * NOTE: The wrapper uses topic-based messaging. For queue-based consumption,
     * configure queues to subscribe to topics on the broker side.
     */
    public void processCustomerOrder(String customerId, String productId, double amount) {
        logger.info("───────────────────────────────────────────────────────────────");
        logger.info("EXAMPLE: processCustomerOrder");
        logger.info("DEMONSTRATES: Publishing to topics with MessageProperties");
        logger.info("  - Topic publish for event broadcasting (orders/created)");
        logger.info("  - Persistent publish with custom properties for reliable delivery");
        logger.info("───────────────────────────────────────────────────────────────");
        try {
            // Create order message
            OrderMessage order = new OrderMessage();
            order.setOrderId(UUID.randomUUID().toString());
            order.setCustomerId(customerId);
            order.setProductId(productId);
            order.setAmount(amount);
            order.setTimestamp(System.currentTimeMillis());
            order.setStatus("PENDING");

            // Publish order created event to topic (for other services to listen)
            // Use direct messaging for simple event broadcasting
            solacePublisher.publishToTopic("orders/created", order);

            // Send order for processing with custom properties using persistent messaging
            // Queues subscribe to this topic on the broker side for reliable consumption
            MessageProperties properties = new MessageProperties()
                .setCorrelationId(order.getOrderId())
                .setReplyTo("orders/replies")
                .setTimeToLive(300000) // 5 minutes TTL
                .setPriority(5)
                .setDeliveryMode("PERSISTENT")
                .addUserProperty("orderType", "customer")
                .addUserProperty("priority", "normal");

            // Publish to topic with persistent delivery - queues subscribing to this topic
            // will receive the message with guaranteed delivery
            solacePublisher.publishPersistentToTopicWithProperties("orders/processing", order, properties);

            logger.info("Order {} submitted for processing", order.getOrderId());

        } catch (Exception e) {
            logger.error("Failed to process customer order", e);
            throw new RuntimeException("Order processing failed", e);
        }
    }

    /**
     * Example: Send a notification message.
     *
     * DEMONSTRATES: Async persistent topic publishing with CompletableFuture callbacks
     *   - Non-blocking publish for high-throughput scenarios
     *   - Completion callbacks for success/failure handling
     */
    public void sendNotification(String userId, String messageText, String notificationType) {
        logger.info("───────────────────────────────────────────────────────────────");
        logger.info("EXAMPLE: sendNotification");
        logger.info("DEMONSTRATES: Async persistent publishing with CompletableFuture callbacks");
        logger.info("───────────────────────────────────────────────────────────────");
        NotificationMessage notification = new NotificationMessage();
        notification.setNotificationId(UUID.randomUUID().toString());
        notification.setUserId(userId);
        notification.setMessage(messageText);
        notification.setType(notificationType);
        notification.setTimestamp(System.currentTimeMillis());

        // Send async to avoid blocking the calling thread
        // Use persistent messaging for reliable notification delivery
        solacePublisher.publishPersistentToTopicAsync("notifications/" + notificationType.toLowerCase(), notification)
            .thenRun(() -> logger.debug("Notification sent successfully: {}", notification.getNotificationId()))
            .exceptionally(throwable -> {
                logger.error("Failed to send notification: {}", notification.getNotificationId(), throwable);
                return null;
            });
    }

    /**
     * Example: Publish a status update to a topic for broadcasting.
     */
    public void publishStatusUpdate(String entityId, String status, String details) {
        logger.info("───────────────────────────────────────────────────────────────");
        logger.info("EXAMPLE: publishStatusUpdate");
        logger.info("DEMONSTRATES: Topic-based event broadcasting for status changes");
        logger.info("  - Publishes to dynamic topic: status/updates/{entityId}");
        logger.info("───────────────────────────────────────────────────────────────");
        StatusUpdate statusUpdate = new StatusUpdate();
        statusUpdate.setEntityId(entityId);
        statusUpdate.setStatus(status);
        statusUpdate.setDetails(details);
        statusUpdate.setTimestamp(System.currentTimeMillis());

        // Publish to topic for all interested subscribers
        solacePublisher.publishToTopic("status/updates/" + entityId, statusUpdate);

        logger.info("Status update published for entity {}: {}", entityId, status);
    }

    /**
     * Handle incoming order messages.
     */
    private void handleOrderMessage(OrderMessage order, InboundMessage originalMessage) {
        logger.info("───────────────────────────────────────────────────────────────");
        logger.info("EXAMPLE: handleOrderMessage (Consumer Callback)");
        logger.info("DEMONSTRATES: Message handler with InboundMessage access");
        logger.info("  - Receives deserialized OrderMessage + raw InboundMessage");
        logger.info("  - Shows workflow: process -> status update -> notification");
        logger.info("───────────────────────────────────────────────────────────────");
        logger.info("Processing order: {}", order.getOrderId());

        try {
            // Simulate order processing
            Thread.sleep(1000); // Simulate processing time

            // Update order status
            order.setStatus("PROCESSING");

            // Publish status update
            publishStatusUpdate(order.getOrderId(), "PROCESSING", "Order is being processed");

            // Simulate successful processing
            order.setStatus("COMPLETED");

            // Send completion notification
            sendNotification(
                order.getCustomerId(),
                "Your order " + order.getOrderId() + " has been completed!",
                "ORDER_COMPLETED"
            );

            // Publish final status
            publishStatusUpdate(order.getOrderId(), "COMPLETED", "Order processing completed successfully");

            logger.info("Order {} processed successfully", order.getOrderId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Order processing interrupted: {}", order.getOrderId(), e);
            throw new RuntimeException("Order processing interrupted", e);
        } catch (Exception e) {
            logger.error("Failed to process order: {}", order.getOrderId(), e);

            // Update status to failed
            order.setStatus("FAILED");
            publishStatusUpdate(order.getOrderId(), "FAILED", "Order processing failed: " + e.getMessage());

            // Re-throw to trigger message NACK for potential retry
            throw new RuntimeException("Order processing failed", e);
        }
    }

    /**
     * Handle incoming notification messages.
     */
    private void handleNotificationMessage(NotificationMessage notification,
                                         InboundMessage originalMessage) {
        logger.info("───────────────────────────────────────────────────────────────");
        logger.info("EXAMPLE: handleNotificationMessage (Consumer Callback)");
        logger.info("DEMONSTRATES: Routing messages by type field");
        logger.info("  - Switch on notification.type to dispatch to appropriate handler");
        logger.info("───────────────────────────────────────────────────────────────");
        logger.info("Processing notification: {}", notification.getNotificationId());

        try {
            // Simulate notification processing (e.g., send email, SMS, push notification)
            switch (notification.getType()) {
                case "EMAIL":
                    sendEmailNotification(notification);
                    break;
                case "SMS":
                    sendSmsNotification(notification);
                    break;
                case "PUSH":
                    sendPushNotification(notification);
                    break;
                case "ORDER_COMPLETED":
                    sendOrderCompletedNotification(notification);
                    break;
                default:
                    logger.warn("Unknown notification type: {}", notification.getType());
            }

            logger.info("Notification {} processed successfully", notification.getNotificationId());

        } catch (Exception e) {
            logger.error("Failed to process notification: {}", notification.getNotificationId(), e);
            throw new RuntimeException("Notification processing failed", e);
        }
    }

    // Mock notification methods
    private void sendEmailNotification(NotificationMessage notification) {
        logger.info("Sending email to user {}: {}", notification.getUserId(), notification.getMessage());
    }

    private void sendSmsNotification(NotificationMessage notification) {
        logger.info("Sending SMS to user {}: {}", notification.getUserId(), notification.getMessage());
    }

    private void sendPushNotification(NotificationMessage notification) {
        logger.info("Sending push notification to user {}: {}", notification.getUserId(), notification.getMessage());
    }

    private void sendOrderCompletedNotification(NotificationMessage notification) {
        logger.info("Sending order completion notification to user {}: {}",
                   notification.getUserId(), notification.getMessage());
    }

    /**
     * Get consumer health status for monitoring.
     */
    public boolean isHealthy() {
        var orderStatus = consumerManager.getConsumerStatus(orderConsumerId);
        var notificationStatus = consumerManager.getConsumerStatus(notificationConsumerId);

        return orderStatus != null && orderStatus.isRunning() &&
               notificationStatus != null && notificationStatus.isRunning();
    }

    // Message classes
    public static class OrderMessage {
        private String orderId;
        private String customerId;
        private String productId;
        private double amount;
        private long timestamp;
        private String status;

        public OrderMessage() {}

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }

        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }

        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }

        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        @Override
        public String toString() {
            return String.format("OrderMessage{orderId='%s', customerId='%s', productId='%s', amount=%.2f, status='%s'}",
                               orderId, customerId, productId, amount, status);
        }
    }

    public static class NotificationMessage {
        private String notificationId;
        private String userId;
        private String message;
        private String type;
        private long timestamp;

        public NotificationMessage() {}

        public String getNotificationId() { return notificationId; }
        public void setNotificationId(String notificationId) { this.notificationId = notificationId; }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return String.format("NotificationMessage{id='%s', userId='%s', type='%s', message='%s'}",
                               notificationId, userId, type, message);
        }
    }

    public static class StatusUpdate {
        private String entityId;
        private String status;
        private String details;
        private long timestamp;

        public StatusUpdate() {}

        public String getEntityId() { return entityId; }
        public void setEntityId(String entityId) { this.entityId = entityId; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public String getDetails() { return details; }
        public void setDetails(String details) { this.details = details; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return String.format("StatusUpdate{entityId='%s', status='%s', details='%s'}",
                               entityId, status, details);
        }
    }
}
