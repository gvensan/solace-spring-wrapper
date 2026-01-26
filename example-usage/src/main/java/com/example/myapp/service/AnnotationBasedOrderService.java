package com.example.myapp.service;

import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.annotation.SolacePublish;
import com.solace.messaging.receiver.InboundMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Example service demonstrating annotation-based Solace integration.
 * This shows how to use @SolacePublish and @SolaceConsumer annotations
 * for declarative message publishing and consuming.
 */
@Service
public class AnnotationBasedOrderService {

    private static final Logger logger = LoggerFactory.getLogger(AnnotationBasedOrderService.class);

    /**
     * Creates an order and automatically publishes it to a topic.
     *
     * ANNOTATION FEATURES DEMONSTRATED:
     * - destination: Static topic "orders/created"
     * - correlationId: SpEL expression using result field "#{result.orderId}"
     * - userProperties: Multiple SpEL expressions to set message properties
     * - async: Non-blocking publish (fire-and-forget)
     */
    @SolacePublish(
        destination = "orders/created",
        correlationId = "#{result.orderId}",
        userProperties = {
            "orderType=#{result.orderType}",
            "priority=#{result.priority}",
            "customerId=#{result.customerId}"
        },
        async = true
    )
    public OrderMessage createOrder(String customerId, String productId, double amount, String orderType) {
        logger.info("Creating order for customer: {}, product: {}", customerId, productId);

        OrderMessage order = new OrderMessage();
        order.setOrderId(UUID.randomUUID().toString());
        order.setCustomerId(customerId);
        order.setProductId(productId);
        order.setAmount(amount);
        order.setOrderType(orderType);
        order.setPriority(determinePriority(amount));
        order.setStatus("CREATED");
        order.setTimestamp(System.currentTimeMillis());

        return order;
    }

    /**
     * Updates order status and publishes status update.
     *
     * ANNOTATION FEATURES DEMONSTRATED:
     * - destination: Dynamic SpEL destination using method parameter
     * - condition: SpEL condition to skip publish if no change
     * - timeToLive: Message expiration in milliseconds
     */
    @SolacePublish(
        destination = "orders/status/#{#param0}",
        correlationId = "#{#param0}",
        timeToLive = 300000,
        userProperties = {
            "previousStatus=#{#param1}",
            "newStatus=#{#param2}",
            "timestamp=#{T(System).currentTimeMillis()}"
        },
        condition = "#{#param2 != #param1}"
    )
    public StatusUpdate updateOrderStatus(String orderId, String currentStatus, String newStatus) {
        logger.info("Updating order {} status from {} to {}", orderId, currentStatus, newStatus);

        StatusUpdate update = new StatusUpdate();
        update.setOrderId(orderId);
        update.setPreviousStatus(currentStatus);
        update.setNewStatus(newStatus);
        update.setTimestamp(LocalDateTime.now().toString());

        return update;
    }

    /**
     * Sends order to processing topic with routing based on order type.
     *
     * ANNOTATION FEATURES DEMONSTRATED:
     * - destination: SpEL ternary operator for dynamic routing using 'result'
     * - correlationId: SpEL expression accessing result object field
     * - userProperties: SpEL with result object for dynamic values
     */
    @SolacePublish(
        destination = "orders/processing/#{result.orderType == 'VIP' ? 'vip' : 'standard'}",
        correlationId = "#{result.orderId}",
        userProperties = {
            "sourceMethod=sendToProcessing",
            "customerType=#{result.orderType}",
            "priorityLevel=#{result.orderType == 'VIP' ? 'HIGH' : 'NORMAL'}"
        }
    )
    public OrderMessage sendToProcessing(OrderMessage order) {
        logger.info("Sending order {} to processing topic", order.getOrderId());
        order.setStatus("QUEUED_FOR_PROCESSING");
        return order;
    }

    /**
     * Consumer for processing standard orders.
     *
     * ANNOTATION FEATURES DEMONSTRATED:
     * - queue: Named queue for persistent messaging
     * - topics: Topic subscription for queue attraction
     * - maxRetries + retryDelay: Local retry configuration
     */
    @SolaceConsumer(
        queue = "orders-standard-processing",
        topics = {"orders/processing/standard"},
        consumerIdPrefix = "order-service",
        consumerId = "standard-processor",
        maxRetries = 3,
        retryDelay = 2000
    )
    public void processStandardOrder(OrderMessage order, InboundMessage originalMessage) {
        logger.info("Processing standard order: {}", order.getOrderId());

        try {
            Thread.sleep(1000);
            updateOrderStatus(order.getOrderId(), order.getStatus(), "PROCESSING");
            Thread.sleep(2000);
            updateOrderStatus(order.getOrderId(), "PROCESSING", "COMPLETED");
            logger.info("Standard order {} processed successfully", order.getOrderId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Order processing interrupted", e);
        }
    }

    /**
     * Consumer for processing VIP orders with higher priority.
     */
    @SolaceConsumer(
        queue = "orders-vip-processing",
        topics = {"orders/processing/vip"},
        consumerIdPrefix = "order-service",
        consumerId = "vip-processor",
        maxRetries = 5,
        retryDelay = 1000
    )
    public void processVipOrder(OrderMessage order) {
        logger.info("Processing VIP order: {} with priority handling", order.getOrderId());

        try {
            Thread.sleep(500);
            updateOrderStatus(order.getOrderId(), order.getStatus(), "VIP_PROCESSING");
            Thread.sleep(1000);
            updateOrderStatus(order.getOrderId(), "VIP_PROCESSING", "COMPLETED");
            sendVipCompletionNotification(order);
            logger.info("VIP order {} processed successfully", order.getOrderId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Consumer for handling order status updates with condition filtering.
     */
    @SolaceConsumer(
        queue = "order-status-notifications",
        topics = {"orders/status/*"},
        consumerIdPrefix = "order-service",
        condition = "#{message.newStatus == 'COMPLETED' or message.newStatus == 'FAILED'}"
    )
    public void handleFinalStatusUpdates(StatusUpdate statusUpdate) {
        logger.info("Handling final status update for order: {} - Status: {}",
                   statusUpdate.getOrderId(), statusUpdate.getNewStatus());

        if ("COMPLETED".equals(statusUpdate.getNewStatus())) {
            logger.info("Sending completion email for order: {}", statusUpdate.getOrderId());
        } else if ("FAILED".equals(statusUpdate.getNewStatus())) {
            logger.info("Sending failure notification for order: {}", statusUpdate.getOrderId());
        }
    }

    /**
     * Sends VIP completion notification with high priority.
     */
    @SolacePublish(
        destination = "notifications/vip",
        correlationId = "#{result.orderId}",
        priority = 9,
        userProperties = {
            "notificationType=VIP_ORDER_COMPLETED",
            "customerId=#{result.customerId}"
        }
    )
    public NotificationMessage sendVipCompletionNotification(OrderMessage order) {
        NotificationMessage notification = new NotificationMessage();
        notification.setNotificationId(UUID.randomUUID().toString());
        notification.setOrderId(order.getOrderId());
        notification.setCustomerId(order.getCustomerId());
        notification.setMessage("Your VIP order has been completed successfully!");
        notification.setType("VIP_COMPLETION");
        notification.setTimestamp(System.currentTimeMillis());
        return notification;
    }

    /**
     * Publishes bulk order updates with conditional publishing.
     */
    @SolacePublish(
        destination = "orders/bulk-update",
        condition = "#{result != null and result.orderIds.size() > 0}",
        userProperties = {
            "updateType=BULK",
            "orderCount=#{result.orderIds.size()}"
        }
    )
    public BulkUpdateMessage updateMultipleOrders(String[] orderIds, String newStatus) {
        logger.info("Updating {} orders to status: {}", orderIds.length, newStatus);

        if (orderIds.length == 0) {
            return null;
        }

        BulkUpdateMessage bulkUpdate = new BulkUpdateMessage();
        bulkUpdate.setOrderIds(java.util.Arrays.asList(orderIds));
        bulkUpdate.setNewStatus(newStatus);
        bulkUpdate.setTimestamp(System.currentTimeMillis());

        return bulkUpdate;
    }

    private String determinePriority(double amount) {
        if (amount > 1000) return "HIGH";
        if (amount > 100) return "MEDIUM";
        return "LOW";
    }

    // Message classes
    public static class OrderMessage {
        private String orderId;
        private String customerId;
        private String productId;
        private double amount;
        private String orderType;
        private String priority;
        private String status;
        private long timestamp;

        public OrderMessage() {}

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
        public String getOrderType() { return orderType; }
        public void setOrderType(String orderType) { this.orderType = orderType; }
        public String getPriority() { return priority; }
        public void setPriority(String priority) { this.priority = priority; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return String.format("OrderMessage{orderId='%s', customerId='%s', status='%s', amount=%.2f}",
                               orderId, customerId, status, amount);
        }
    }

    public static class StatusUpdate {
        private String orderId;
        private String previousStatus;
        private String newStatus;
        private String timestamp;

        public StatusUpdate() {}

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getPreviousStatus() { return previousStatus; }
        public void setPreviousStatus(String previousStatus) { this.previousStatus = previousStatus; }
        public String getNewStatus() { return newStatus; }
        public void setNewStatus(String newStatus) { this.newStatus = newStatus; }
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    }

    public static class NotificationMessage {
        private String notificationId;
        private String orderId;
        private String customerId;
        private String message;
        private String type;
        private long timestamp;

        public NotificationMessage() {}

        public String getNotificationId() { return notificationId; }
        public void setNotificationId(String notificationId) { this.notificationId = notificationId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    public static class BulkUpdateMessage {
        private java.util.List<String> orderIds;
        private String newStatus;
        private long timestamp;

        public BulkUpdateMessage() {}

        public java.util.List<String> getOrderIds() { return orderIds; }
        public void setOrderIds(java.util.List<String> orderIds) { this.orderIds = orderIds; }
        public String getNewStatus() { return newStatus; }
        public void setNewStatus(String newStatus) { this.newStatus = newStatus; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }
}
