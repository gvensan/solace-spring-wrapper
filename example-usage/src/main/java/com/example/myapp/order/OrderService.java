package com.example.myapp.order;

import com.example.myapp.model.OrderCancelled;
import com.example.myapp.model.OrderCreated;
import com.example.myapp.model.OrderStatusChanged;
import com.example.myapp.observability.OrderMetrics;
import com.solace.wrapper.annotation.SolacePublish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Entry point of the order-fulfillment pipeline. Every public method is purely declarative:
 * it builds and returns a domain object, and {@link SolacePublish} turns that return value into a
 * Solace message — the service itself never touches the messaging API.
 *
 * <p>This class is the showcase for the <strong>full {@code @SolacePublish} attribute surface</strong>,
 * spread across three realistic operations:</p>
 * <ul>
 *   <li>{@link #createOrder} — rich event publish: async, delivery mode, class-of-service,
 *       application message id/type, client name, SpEL user-properties.</li>
 *   <li>{@link #cancelOrder} — conditional publish driven by <em>named</em> method parameters
 *       (requires the {@code -parameters} compiler flag, enabled in this module's pom).</li>
 *   <li>{@link #changeStatus} — high-frequency "tick" publish: TTL, priority, eliding eligibility,
 *       absolute message expiration and a monotonic sequence number.</li>
 * </ul>
 */
@Service
public class OrderService {

    private static final Logger logger = LoggerFactory.getLogger(OrderService.class);

    /** Drives the {@code sequenceNumber} attribute and the business "orders created" counter. */
    private final AtomicLong sequence = new AtomicLong();
    private final OrderMetrics metrics;

    public OrderService(OrderMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Accepts an order and publishes an {@link OrderCreated} event to {@code orders/created}.
     *
     * <p>Attributes demonstrated: {@code async} (fire-and-forget), {@code deliveryMode} chosen
     * dynamically by tier, {@code classOfService} (VIP rides a higher CoS), {@code applicationMessageId}
     * and {@code applicationMessageType} (for downstream routing/tracing), {@code clientName}
     * (publisher identity), {@code correlationId} and SpEL-populated {@code userProperties}.</p>
     */
    @SolacePublish(
            destination = "orders/created",
            async = true,
            clientName = "order-publisher",
            correlationId = "#{result.orderId}",
            applicationMessageId = "#{result.orderId}",
            applicationMessageType = "OrderCreatedEvent",
            classOfService = "#{result.orderType == 'VIP' ? 2 : 1}",
            deliveryMode = "#{result.orderType == 'VIP' ? 'PERSISTENT' : 'DIRECT'}",
            userProperties = {
                    "orderType=#{result.orderType}",
                    "customerId=#{result.customerId}",
                    "amount=#{result.amount}"
            }
    )
    public OrderCreated createOrder(String customerId, String sku, int qty, double amount, String orderType) {
        OrderCreated event = new OrderCreated();
        event.orderId = "ord-" + sequence.incrementAndGet();
        event.customerId = customerId;
        event.sku = sku;
        event.qty = qty;
        event.amount = amount;
        event.orderType = orderType;
        event.sequence = sequence.get();
        event.createdAt = Instant.now().toString();

        metrics.orderCreated(orderType);
        logger.info("Accepted {} order {} for customer {} ({} x {} = {})",
                orderType, event.orderId, customerId, qty, sku, amount);
        return event;
    }

    /**
     * Cancels an order, but only publishes when a reason is supplied.
     *
     * <p>Attributes demonstrated: {@code condition} and {@code correlationId} expressed with
     * <em>named</em> parameter references ({@code #orderId}, {@code #reason}) rather than positional
     * {@code #p0}/{@code #p1}. Returning the event while the condition is false simply suppresses the
     * publish — the method still runs normally.</p>
     */
    @SolacePublish(
            destination = "orders/cancelled",
            correlationId = "#{#orderId}",
            condition = "#{#reason != null and !#reason.isBlank()}",
            userProperties = {"reason=#{#reason}"}
    )
    public OrderCancelled cancelOrder(String orderId, String reason) {
        OrderCancelled event = new OrderCancelled();
        event.orderId = orderId;
        event.reason = reason;
        event.cancelledAt = Instant.now().toString();
        if (reason == null || reason.isBlank()) {
            logger.info("Cancel request for {} has no reason — publish will be skipped by condition", orderId);
        } else {
            logger.info("Cancelling order {} (reason: {})", orderId, reason);
        }
        return event;
    }

    /**
     * Emits an order status transition to {@code orders/status/{orderId}}.
     *
     * <p>Status changes are frequent and quickly stale, so this publish uses delivery controls suited
     * to that: {@code timeToLive} (static 5-min TTL), {@code priority} (static), {@code elidingEligible}
     * (let the broker drop superseded ticks to slow consumers), {@code messageExpiration} (absolute
     * expiry timestamp) and {@code sequenceNumber} (ordering hint). The destination is built from a
     * named parameter.</p>
     */
    @SolacePublish(
            destination = "orders/status/#{#orderId}",
            correlationId = "#{#orderId}",
            timeToLive = 300_000,
            priority = 4,
            elidingEligible = "#{true}",
            messageExpiration = "#{T(System).currentTimeMillis() + 300000}",
            sequenceNumber = "#{result.sequence}",
            userProperties = {
                    "previousStatus=#{#previousStatus}",
                    "newStatus=#{#newStatus}"
            }
    )
    public OrderStatusChanged changeStatus(String orderId, String previousStatus, String newStatus) {
        OrderStatusChanged event = new OrderStatusChanged();
        event.orderId = orderId;
        event.previousStatus = previousStatus;
        event.newStatus = newStatus;
        event.sequence = sequence.incrementAndGet();
        event.changedAt = Instant.now().toString();
        logger.info("Order {} status {} -> {}", orderId, previousStatus, newStatus);
        return event;
    }
}
