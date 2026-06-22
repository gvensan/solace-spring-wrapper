package com.example.myapp.inventory;

import com.example.myapp.model.ChargeRequest;
import com.example.myapp.model.InventoryReserveRequest;
import com.example.myapp.model.OrderCreated;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.annotation.SolacePublish;
import com.solace.wrapper.consumer.SolaceAckContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Second stage of the pipeline. Demonstrates the two ends of the {@link SolaceConsumer} spectrum
 * combined with chained publishing.
 *
 * <ul>
 *   <li>{@link #onOrderCreated} — a <strong>DIRECT</strong> (topic, best-effort) consumer that
 *       transforms an {@link OrderCreated} event into an {@link InventoryReserveRequest} and, via the
 *       chained {@link SolacePublish}, forwards it persistently. No queue, no ack.</li>
 *   <li>{@link #reserve} — a <strong>persistent queue</strong> consumer using
 *       {@link SolaceConsumer.AckMode#MANUAL MANUAL} acknowledgement and local backoff retry. It
 *       inspects the raw {@link InboundMessage}, decides the reservation outcome, and explicitly
 *       {@code ack()}s or {@code fail()}s through the {@link SolaceAckContext}.</li>
 * </ul>
 */
@Service
public class InventoryService {

    private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);

    /**
     * DIRECT-mode consumer: subscribes straight to the {@code orders/created} topic (no queue) and
     * fans the event into a reservation command. The chained {@link SolacePublish} sends the returned
     * object to {@code inventory/reserve} with persistent delivery so the reservation is not lost.
     */
    @SolaceConsumer(
            topics = {"orders/created"},
            mode = SolaceConsumer.MessagingMode.DIRECT,
            consumerIdPrefix = "inventory",
            consumerId = "inventory-order-listener"
    )
    @SolacePublish(
            destination = "inventory/reserve",
            deliveryMode = "PERSISTENT",
            correlationId = "#{result.orderId}",
            userProperties = {"orderId=#{result.orderId}", "orderType=#{result.orderType}"}
    )
    public InventoryReserveRequest onOrderCreated(OrderCreated order) {
        logger.info("Inventory received order {} — preparing reservation", order.orderId);
        InventoryReserveRequest req = new InventoryReserveRequest();
        req.orderId = order.orderId;
        req.customerId = order.customerId;
        req.sku = order.sku;
        req.qty = order.qty;
        req.amount = order.amount;
        req.orderType = order.orderType;
        return req;
    }

    /**
     * Persistent queue consumer with MANUAL acknowledgement and local backoff retry.
     *
     * <p>Attributes demonstrated: {@code queue} + {@code topics} (the queue is subscribed to the
     * reserve topic), {@code mode = PERSISTENT} (explicit), {@code ackMode = MANUAL}, and the local
     * retry/backoff knobs ({@code localMaxAttempts}, {@code localBackoffInitialMs},
     * {@code localBackoffMultiplier}, {@code localBackoffMaxMs}). {@code autoCreateQueue = true}
     * provisions the queue on a broker that allows it.</p>
     *
     * <p>The method takes the deserialized request, the raw {@link InboundMessage} (to read headers),
     * and a {@link SolaceAckContext}. A non-positive quantity is treated as a poison message and
     * {@code fail()}ed; otherwise we {@code ack()} and forward a {@link ChargeRequest} to billing.</p>
     */
    @SolaceConsumer(
            queue = "inventory-reserve",
            topics = {"inventory/reserve"},
            mode = SolaceConsumer.MessagingMode.PERSISTENT,
            ackMode = SolaceConsumer.AckMode.MANUAL,
            autoCreateQueue = true,
            consumerIdPrefix = "inventory",
            consumerId = "inventory-reserve-worker",
            localMaxAttempts = 3,
            localBackoffInitialMs = 250,
            localBackoffMultiplier = 2.0,
            localBackoffMaxMs = 2000
    )
    @SolacePublish(
            destination = "billing/charge",
            deliveryMode = "PERSISTENT",
            correlationId = "#{result.orderId}",
            userProperties = {"orderType=#{result.orderType}"}
    )
    public ChargeRequest reserve(InventoryReserveRequest req, InboundMessage inbound, SolaceAckContext ack) {
        // Demonstrate raw InboundMessage access: read a user property set by the upstream publish.
        Object orderIdHeader = inbound.getProperty("orderId");
        logger.info("Reserving stock for order {} (raw header orderId={})", req.orderId, orderIdHeader);

        if (req.qty <= 0) {
            logger.warn("Order {} has invalid quantity {} — failing message (no charge published)",
                    req.orderId, req.qty);
            ack.fail();
            return null; // returning null suppresses the chained publish
        }

        ack.ack();
        ChargeRequest charge = new ChargeRequest();
        charge.orderId = req.orderId;
        charge.customerId = req.customerId;
        charge.amount = req.amount;
        charge.orderType = req.orderType;
        logger.info("Reserved stock for order {} — requesting charge of {}", req.orderId, charge.amount);
        return charge;
    }
}
