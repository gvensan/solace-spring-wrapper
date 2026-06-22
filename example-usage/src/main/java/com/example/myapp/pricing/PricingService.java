package com.example.myapp.pricing;

import com.example.myapp.model.QuoteRequest;
import com.example.myapp.model.QuoteResponse;
import com.solace.wrapper.annotation.SolaceReplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Server side of the native request-reply pattern. Unlike the "fake" request-reply that publishes to
 * a SpEL {@code replyTo} topic, {@link SolaceReplier} uses the Solace API's built-in request-reply:
 * correlation and the reply-to inbox are handled automatically, and the method's <strong>return value
 * is sent back as the reply</strong>.
 *
 * <p>Attributes demonstrated: {@code topic} (the request subscription), {@code shareName} (so multiple
 * instances load-balance incoming requests as a shared subscription), and explicit
 * {@code backpressure}/{@code backpressureCapacity} on the reply path. Returning {@code null} (or a
 * {@code void} method) would send no reply, which the requestor observes as a timeout.</p>
 */
@Component
public class PricingService {

    private static final Logger logger = LoggerFactory.getLogger(PricingService.class);

    @SolaceReplier(
            topic = "pricing/quote/v1",
            shareName = "pricing-workers",
            replierIdPrefix = "pricing",
            backpressure = SolaceReplier.Backpressure.WAIT,
            backpressureCapacity = 256
    )
    public QuoteResponse quote(QuoteRequest request) {
        double base = basePrice(request.sku);
        double discount = "VIP".equalsIgnoreCase(request.customerTier) ? 0.85 : 1.0;
        double unit = round(base * discount);

        QuoteResponse response = new QuoteResponse();
        response.sku = request.sku;
        response.qty = request.qty;
        response.unitPrice = unit;
        response.totalPrice = round(unit * request.qty);
        response.currency = "USD";

        logger.info("Quoted {} x {} for tier {} -> {} {}",
                request.qty, request.sku, request.customerTier, response.totalPrice, response.currency);
        return response;
    }

    private static double basePrice(String sku) {
        // Deterministic pseudo-price so the example is reproducible without external data.
        return 10.0 + (Math.abs(sku == null ? 0 : sku.hashCode()) % 90);
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
