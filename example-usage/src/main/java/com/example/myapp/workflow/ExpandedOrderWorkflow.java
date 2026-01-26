package com.example.myapp.workflow;

import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.annotation.SolacePublish;
import com.solace.wrapper.consumer.SolaceAckContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * Advanced example showing a multi-service workflow with annotations.
 * This models: order creation -> inventory reservation -> billing -> notifications.
 *
 * IMPORTANT: This class is commented out by default because it defines multiple
 * @SolaceConsumer annotations that would conflict with AnnotationBasedOrderService
 * when running the example app. Uncomment the @Service annotations to activate.
 *
 * Key patterns demonstrated:
 * - Chained @SolaceConsumer + @SolacePublish on same method (message transformation)
 * - Manual ack with SolaceAckContext
 * - DIRECT mode consumers (topic subscription without queue)
 * - Request-reply pattern with dynamic replyTo
 * - Condition-based filtering on consumers
 * - Local retry with exponential backoff
 */
public class ExpandedOrderWorkflow {

    // Uncomment @Service to activate this service
    // @Service
    public static class OrderService {
        private static final Logger logger = LoggerFactory.getLogger(OrderService.class);

        @SolacePublish(
            destination = "orders/created",
            correlationId = "#{result.orderId}",
            userProperties = {
                "customerId=#{result.customerId}",
                "orderType=#{result.orderType}",
                "amount=#{result.amount}"
            },
            async = true
        )
        public OrderCreated createOrder(String customerId, String sku, int qty, double amount, String orderType) {
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("EXAMPLE: OrderService.createOrder");
            logger.info("DEMONSTRATES: @SolacePublish with SpEL-based userProperties and async");
            logger.info("  - correlationId from result.orderId");
            logger.info("  - userProperties populated from return value fields");
            logger.info("───────────────────────────────────────────────────────────────");
            OrderCreated evt = new OrderCreated();
            evt.orderId = UUID.randomUUID().toString();
            evt.customerId = customerId;
            evt.sku = sku;
            evt.qty = qty;
            evt.amount = amount;
            evt.orderType = orderType;
            evt.createdAt = Instant.now().toString();
            logger.info("Created order {}", evt.orderId);
            return evt;
        }

        @SolacePublish(
            destination = "orders/canceled",
            correlationId = "#{#p0}",
            condition = "#{#p1 != null}"
        )
        public OrderCanceled cancelOrder(String orderId, String reason) {
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("EXAMPLE: OrderService.cancelOrder");
            logger.info("DEMONSTRATES: @SolacePublish with condition and #p0/#p1 parameter references");
            logger.info("  - condition='#{#p1 != null}' - only publishes if reason provided");
            logger.info("  - correlationId='#{#p0}' - uses first method parameter");
            logger.info("───────────────────────────────────────────────────────────────");
            OrderCanceled evt = new OrderCanceled();
            evt.orderId = orderId;
            evt.reason = reason;
            evt.canceledAt = Instant.now().toString();
            return evt;
        }

        @SolacePublish(
            destination = "quotes/request",
            correlationId = "#{result.correlationId}",
            replyTo = "#{result.replyTo}"
        )
        public QuoteRequest requestQuote(String customerId, String sku, int qty) {
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("EXAMPLE: OrderService.requestQuote");
            logger.info("DEMONSTRATES: Request-reply pattern with SpEL replyTo");
            logger.info("  - replyTo='#{result.replyTo}' - dynamic reply destination");
            logger.info("  - Enables async request-reply messaging pattern");
            logger.info("───────────────────────────────────────────────────────────────");
            QuoteRequest req = new QuoteRequest();
            req.correlationId = UUID.randomUUID().toString();
            req.replyTo = "quotes/reply/" + req.correlationId;
            req.customerId = customerId;
            req.sku = sku;
            req.qty = qty;
            return req;
        }
    }

    // Uncomment @Service to activate this service
    // @Service
    public static class InventoryService {
        private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);

        @SolaceConsumer(
            queue = "inventory-reserve",
            topics = {"inventory/reserve"},
            consumerIdPrefix = "inventory",
            ackMode = SolaceConsumer.AckMode.MANUAL,
            localMaxAttempts = 2,
            localBackoffInitialMs = 250,
            localBackoffMultiplier = 2.0,
            localBackoffMaxMs = 1000
        )
        @SolacePublish(
            destination = "billing/charge",
            correlationId = "#{#p0.orderId}",
            deliveryMode = "PERSISTENT"
        )
        public OrderChargeRequest reserveInventory(InventoryReserveRequest req, InboundMessage inbound, SolaceAckContext ack) {
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("EXAMPLE: InventoryService.reserveInventory");
            logger.info("DEMONSTRATES: Combined @SolaceConsumer + @SolacePublish (chained messaging)");
            logger.info("  - MANUAL ack with SolaceAckContext (ack.ack() / ack.fail())");
            logger.info("  - Local retry with backoff (localMaxAttempts, localBackoffInitialMs)");
            logger.info("  - Access to InboundMessage for raw message inspection");
            logger.info("  - Return value automatically published to billing/charge");
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("Reserving inventory for order {}", req.orderId);
            if (req.qty <= 0) {
                ack.fail();
                return null;
            }
            ack.ack();
            OrderChargeRequest charge = new OrderChargeRequest();
            charge.orderId = req.orderId;
            charge.amount = 19.99 * req.qty;
            return charge;
        }

        @SolaceConsumer(
            topics = {"orders/created"},
            mode = SolaceConsumer.MessagingMode.DIRECT,
            consumerIdPrefix = "inventory",
            consumerId = "inventory-direct"
        )
        @SolacePublish(
            destination = "inventory/reserve",
            correlationId = "#{#p0.orderId}",
            deliveryMode = "PERSISTENT"
        )
        public InventoryReserveRequest toReserveRequest(OrderCreated created) {
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("EXAMPLE: InventoryService.toReserveRequest");
            logger.info("DEMONSTRATES: DIRECT mode consumer with message transformation");
            logger.info("  - mode=DIRECT for topic subscription without queue");
            logger.info("  - Transforms OrderCreated -> InventoryReserveRequest");
            logger.info("  - Chained @SolacePublish sends transformed message onward");
            logger.info("───────────────────────────────────────────────────────────────");
            InventoryReserveRequest req = new InventoryReserveRequest();
            req.orderId = created.orderId;
            req.sku = created.sku;
            req.qty = created.qty;
            req.customerId = created.customerId;
            return req;
        }
    }

    // Uncomment @Service to activate this service
    // @Service
    public static class BillingService {
        private static final Logger logger = LoggerFactory.getLogger(BillingService.class);

        @SolaceConsumer(
            queue = "billing-charge",
            topics = {"billing/charge"},
            consumerIdPrefix = "billing",
            maxRetries = 2,
            retryDelay = 1000
        )
        @SolacePublish(
            destination = "billing/charged",
            correlationId = "#{#p0.orderId}",
            deliveryMode = "PERSISTENT"
        )
        public ChargeResult charge(OrderChargeRequest req) {
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("EXAMPLE: BillingService.charge");
            logger.info("DEMONSTRATES: Persistent queue consumer with retry configuration");
            logger.info("  - maxRetries=2, retryDelay=1000 for broker-level redelivery");
            logger.info("  - PERSISTENT deliveryMode ensures reliable messaging");
            logger.info("  - Chained publish to billing/charged topic");
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("Charging order {}", req.orderId);
            ChargeResult result = new ChargeResult();
            result.orderId = req.orderId;
            result.amount = req.amount;
            result.status = "CHARGED";
            result.chargedAt = Instant.now().toString();
            return result;
        }
    }

    // Uncomment @Service to activate this service
    // @Service
    public static class NotificationService {
        private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);

        @SolaceConsumer(
            queue = "order-status-notifications",
            topics = {"billing/charged"},
            consumerIdPrefix = "notifications",
            condition = "#{message.status == 'CHARGED' or message.status == 'FAILED'}"
        )
        public void onBillingStatus(ChargeResult result) {
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("EXAMPLE: NotificationService.onBillingStatus");
            logger.info("DEMONSTRATES: @SolaceConsumer with SpEL condition filtering");
            logger.info("  - condition='#{message.status == 'CHARGED' or ...}'");
            logger.info("  - Only processes messages matching condition");
            logger.info("  - Void return (no chained publish)");
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("Notify customer for order {} status {}", result.orderId, result.status);
        }
    }

    // Uncomment @Service to activate this service
    // @Service
    public static class QuoteService {
        private static final Logger logger = LoggerFactory.getLogger(QuoteService.class);

        @SolaceConsumer(queue = "quotes-request", topics = {"quotes/request"}, consumerIdPrefix = "quotes")
        @SolacePublish(
            destination = "#{#p0.replyTo}",
            correlationId = "#{#p0.correlationId}"
        )
        public QuoteResponse respondToQuote(QuoteRequest req) {
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("EXAMPLE: QuoteService.respondToQuote");
            logger.info("DEMONSTRATES: Request-reply responder with dynamic destination");
            logger.info("  - destination='#{#p0.replyTo}' - replies to caller's replyTo");
            logger.info("  - correlationId preserved from request for correlation");
            logger.info("  - Completes request-reply pattern started by OrderService.requestQuote");
            logger.info("───────────────────────────────────────────────────────────────");
            logger.info("Quote requested for {} x{}", req.sku, req.qty);
            QuoteResponse resp = new QuoteResponse();
            resp.correlationId = req.correlationId;
            resp.sku = req.sku;
            resp.qty = req.qty;
            resp.price = 19.99 * req.qty;
            return resp;
        }
    }

    // Message types
    public static class OrderCreated {
        public String orderId;
        public String customerId;
        public String sku;
        public int qty;
        public double amount;
        public String orderType;
        public String createdAt;
    }

    public static class OrderCanceled {
        public String orderId;
        public String reason;
        public String canceledAt;
    }

    public static class InventoryReserveRequest {
        public String orderId;
        public String customerId;
        public String sku;
        public int qty;
    }

    public static class OrderChargeRequest {
        public String orderId;
        public double amount;
    }

    public static class ChargeResult {
        public String orderId;
        public double amount;
        public String status;
        public String chargedAt;
    }

    public static class QuoteRequest {
        public String correlationId;
        public String replyTo;
        public String customerId;
        public String sku;
        public int qty;
    }

    public static class QuoteResponse {
        public String correlationId;
        public String sku;
        public int qty;
        public double price;
    }
}
