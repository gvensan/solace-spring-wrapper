package com.example.myapp.billing;

import com.example.myapp.model.ChargeRequest;
import com.example.myapp.model.ChargeResult;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.annotation.SolacePublish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Third stage: charges the customer. A straightforward persistent queue consumer that relies on
 * <strong>broker redelivery</strong> rather than local retry.
 *
 * <p>Attributes demonstrated: {@code ackMode = AUTO} (explicit — the wrapper acks on normal return
 * and nacks on exception), {@code maxRetries}/{@code retryDelay} (broker-level redelivery tuning),
 * {@code messageType} (explicit fully-qualified payload type instead of relying on parameter
 * inference), and {@code clientName} (connection identity). The chained {@link SolacePublish}
 * emits a {@link ChargeResult} to {@code billing/charged}.</p>
 */
@Service
public class BillingService {

    private static final Logger logger = LoggerFactory.getLogger(BillingService.class);

    @SolaceConsumer(
            queue = "billing-charge",
            topics = {"billing/charge"},
            mode = SolaceConsumer.MessagingMode.PERSISTENT,
            ackMode = SolaceConsumer.AckMode.AUTO,
            messageType = "com.example.myapp.model.ChargeRequest",
            clientName = "billing-worker",
            consumerIdPrefix = "billing",
            consumerId = "billing-charge-worker",
            maxRetries = 2,
            retryDelay = 1500
    )
    @SolacePublish(
            destination = "billing/charged",
            deliveryMode = "PERSISTENT",
            correlationId = "#{result.orderId}",
            userProperties = {
                    "status=#{result.status}",
                    "amount=#{result.amount}"
            }
    )
    public ChargeResult charge(ChargeRequest request) {
        logger.info("Charging {} for order {}", request.amount, request.orderId);

        ChargeResult result = new ChargeResult();
        result.orderId = request.orderId;
        result.customerId = request.customerId;
        result.amount = request.amount;
        // Trivial demo rule: decline absurdly large charges, otherwise approve.
        result.status = request.amount > 100_000 ? "DECLINED" : "CHARGED";
        result.chargedAt = Instant.now().toString();

        logger.info("Order {} charge {}", request.orderId, result.status);
        return result;
    }
}
