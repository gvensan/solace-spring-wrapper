package com.example.myapp.notifications;

import com.example.myapp.model.ChargeResult;
import com.example.myapp.model.OrderStatusChanged;
import com.example.myapp.observability.OrderMetrics;
import com.solace.wrapper.annotation.SolaceConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Final stage: turns billing outcomes and status changes into customer notifications. Both handlers
 * have a {@code void} return, so there is no chained publish — this is a pure sink.
 *
 * <ul>
 *   <li>{@link #onCharged} — persistent queue consumer with a SpEL {@code condition} that filters to
 *       terminal billing outcomes only.</li>
 *   <li>{@link #onStatusChange} — DIRECT consumer on a <strong>wildcard</strong> topic
 *       ({@code orders/status/*}) with {@code autoStart = false}; it is started on demand by the
 *       driver through {@link com.solace.wrapper.consumer.SolaceConsumerManager}, demonstrating
 *       deferred consumer lifecycle.</li>
 * </ul>
 */
@Service
public class NotificationService {

    private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);

    private final OrderMetrics metrics;

    public NotificationService(OrderMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Only fires for terminal billing states.
     *
     * <p><strong>Note the SpEL form:</strong> a consumer {@code condition} is evaluated as a
     * <em>raw</em> SpEL expression (no {@code #{...}} template wrapper — that is publish-side syntax).
     * The deserialized payload is the root object, also exposed as the {@code #message} variable, and
     * the raw message as {@code #originalMessage}.</p>
     */
    @SolaceConsumer(
            queue = "order-notifications",
            topics = {"billing/charged"},
            consumerIdPrefix = "notifications",
            consumerId = "notifications-billing",
            condition = "#message.status == 'CHARGED' or #message.status == 'DECLINED'"
    )
    public void onCharged(ChargeResult result) {
        metrics.notificationSent("billing");
        metrics.orderCompleted();
        if ("CHARGED".equals(result.status)) {
            logger.info("📧 Notifying customer {}: order {} confirmed, charged {}",
                    result.customerId, result.orderId, result.amount);
        } else {
            logger.info("📧 Notifying customer {}: order {} payment DECLINED",
                    result.customerId, result.orderId);
        }
    }

    /**
     * Wildcard DIRECT consumer over every order's status topic. Disabled at startup
     * ({@code autoStart = false}) and started explicitly by the driver to show deferred activation.
     */
    @SolaceConsumer(
            topics = {"orders/status/*"},
            mode = SolaceConsumer.MessagingMode.DIRECT,
            consumerIdPrefix = "notifications",
            consumerId = "notifications-status",
            autoStart = false
    )
    public void onStatusChange(OrderStatusChanged change) {
        metrics.notificationSent("status");
        logger.info("🔔 Order {} moved {} -> {}", change.orderId, change.previousStatus, change.newStatus);
    }
}
