package com.example.myapp.observability;

import com.solace.wrapper.metrics.SolaceMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Business-level metrics for the example, layered on top of the wrapper's {@link SolaceMetrics}.
 *
 * <p>{@link SolaceMetrics} is auto-configured by the wrapper whenever Micrometer is on the classpath
 * (this module pulls it in via {@code spring-boot-starter-actuator} +
 * {@code micrometer-registry-prometheus}). It already records publish/consume/request/reply counters
 * and latencies. Here we add a few <em>domain</em> meters — orders created, notifications sent, and a
 * live gauge of orders in flight — and register them on the same {@link io.micrometer.core.instrument.MeterRegistry}
 * so everything is exported together on {@code /actuator/prometheus}.</p>
 *
 * <p>If metrics are disabled ({@code solace.metrics.enabled=false}) the injected {@link SolaceMetrics}
 * reports {@link SolaceMetrics#isEnabled() not-enabled} and these calls become no-ops.</p>
 */
@Component
public class OrderMetrics {

    private static final Logger logger = LoggerFactory.getLogger(OrderMetrics.class);

    private final SolaceMetrics solaceMetrics;
    private final AtomicLong ordersInFlight = new AtomicLong();

    public OrderMetrics(SolaceMetrics solaceMetrics) {
        this.solaceMetrics = solaceMetrics;
        // Expose a live business gauge alongside the wrapper's own messaging gauges.
        solaceMetrics.registerGauge("example.orders.in_flight", ordersInFlight::get);
        logger.info("OrderMetrics initialised (SolaceMetrics enabled={})", solaceMetrics.isEnabled());
    }

    /** A new order entered the pipeline. */
    public void orderCreated(String orderType) {
        ordersInFlight.incrementAndGet();
        solaceMetrics.getRegistry().counter("example.orders.created", "type", orderType).increment();
    }

    /** An order reached a terminal state (charged/declined) and left the pipeline. */
    public void orderCompleted() {
        ordersInFlight.updateAndGet(v -> v > 0 ? v - 1 : 0);
    }

    /** A customer notification was emitted, tagged by source channel. */
    public void notificationSent(String source) {
        solaceMetrics.getRegistry().counter("example.notifications.sent", "source", source).increment();
    }

    public long ordersInFlight() {
        return ordersInFlight.get();
    }
}
