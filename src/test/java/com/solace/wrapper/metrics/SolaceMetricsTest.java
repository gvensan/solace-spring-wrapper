package com.solace.wrapper.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link SolaceMetrics} records meters with the documented names and tags, and that
 * it degrades to a safe no-op when constructed without a registry.
 */
class SolaceMetricsTest {

    @Test
    void noopWhenRegistryIsNull() {
        SolaceMetrics metrics = new SolaceMetrics(null);
        assertFalse(metrics.isEnabled(), "metrics should be disabled without a registry");

        // None of these should throw despite the absence of a registry.
        metrics.recordPublish(true, "DIRECT", "topic/a", "client-1");
        metrics.recordPublishLatency(false, "PERSISTENT", "topic/b", "client-1", 1_000_000L);
        metrics.recordPublishRejected("DIRECT", "topic/c", "client-1");
        metrics.recordConsume(true, "queue/a", "consumer-1");
        metrics.recordConsumeLatency(false, "queue/a", "consumer-1", 2_000_000L);
        metrics.recordConsumeRetry("queue/a", "consumer-1");

        // The timing helpers must still execute the supplied action.
        StringBuilder ran = new StringBuilder();
        metrics.timePublish("DIRECT", "topic/a", "client-1", () -> ran.append("x"));
        assertEquals("x", ran.toString());
    }

    @Test
    void recordsPublishSuccessCounterAndTimer() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);
        assertTrue(metrics.isEnabled());

        metrics.recordPublishLatency(true, "DIRECT", "topic/orders", "pub-1", 5_000_000L);

        Counter counter = registry.find(SolaceMetrics.PUBLISH_COUNTER)
                .tag("outcome", SolaceMetrics.OUTCOME_SUCCESS)
                .tag("deliveryMode", "DIRECT")
                .tag("destination", "topic/orders")
                .tag("clientName", "pub-1")
                .counter();
        assertNotNull(counter, "publish counter should be registered with the expected tags");
        assertEquals(1.0, counter.count(), 0.0001);

        Timer timer = registry.find(SolaceMetrics.PUBLISH_TIMER)
                .tag("outcome", SolaceMetrics.OUTCOME_SUCCESS)
                .timer();
        assertNotNull(timer, "publish timer should be registered");
        assertEquals(1L, timer.count());
    }

    @Test
    void recordsPublishFailureSeparatelyFromSuccess() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        metrics.recordPublish(true, "PERSISTENT", "topic/x", "pub-1");
        metrics.recordPublish(false, "PERSISTENT", "topic/x", "pub-1");
        metrics.recordPublish(false, "PERSISTENT", "topic/x", "pub-1");

        double failures = registry.find(SolaceMetrics.PUBLISH_COUNTER)
                .tag("outcome", SolaceMetrics.OUTCOME_FAILURE)
                .counter().count();
        double successes = registry.find(SolaceMetrics.PUBLISH_COUNTER)
                .tag("outcome", SolaceMetrics.OUTCOME_SUCCESS)
                .counter().count();
        assertEquals(2.0, failures, 0.0001);
        assertEquals(1.0, successes, 0.0001);
    }

    @Test
    void omitsDestinationTagWhenDisabled() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, false);

        metrics.recordConsume(true, "queue/a", "consumer-1");

        Counter counter = registry.find(SolaceMetrics.CONSUME_COUNTER)
                .tag("consumerId", "consumer-1")
                .counter();
        assertNotNull(counter);
        assertEquals(0, counter.getId().getTags().stream()
                .filter(t -> t.getKey().equals("destination")).count(),
                "destination tag should be omitted when includeDestinationTag=false");
    }

    @Test
    void recordsConsumeRetries() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        metrics.recordConsumeRetry("queue/a", "consumer-1");
        metrics.recordConsumeRetry("queue/a", "consumer-1");

        double retries = registry.find(SolaceMetrics.CONSUME_RETRY_COUNTER)
                .tag("consumerId", "consumer-1")
                .counter().count();
        assertEquals(2.0, retries, 0.0001);
    }

    @Test
    void recordsBackpressureRejections() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        metrics.recordPublishRejected("DIRECT", "topic/a", "pub-1");

        Counter counter = registry.find(SolaceMetrics.PUBLISH_REJECTED_COUNTER)
                .tag("deliveryMode", "DIRECT")
                .counter();
        assertNotNull(counter);
        assertEquals(1.0, counter.count(), 0.0001);
    }

    @Test
    void registersConnectionAndCountGauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        java.util.concurrent.atomic.AtomicBoolean connected = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicInteger publishers = new java.util.concurrent.atomic.AtomicInteger(2);
        java.util.concurrent.atomic.AtomicInteger consumers = new java.util.concurrent.atomic.AtomicInteger(3);

        metrics.registerConnectionStatusGauge(connected::get);
        metrics.registerActivePublishersGauge(publishers::get);
        metrics.registerActiveConsumersGauge(consumers::get);

        assertEquals(1.0, registry.get(SolaceMetrics.CONNECTION_GAUGE).gauge().value(), 0.0001);
        assertEquals(2.0, registry.get(SolaceMetrics.PUBLISHERS_GAUGE).gauge().value(), 0.0001);
        assertEquals(3.0, registry.get(SolaceMetrics.CONSUMERS_GAUGE).gauge().value(), 0.0001);

        // Gauges re-poll their supplier on read, so state changes are reflected live.
        connected.set(false);
        publishers.set(5);
        assertEquals(0.0, registry.get(SolaceMetrics.CONNECTION_GAUGE).gauge().value(), 0.0001);
        assertEquals(5.0, registry.get(SolaceMetrics.PUBLISHERS_GAUGE).gauge().value(), 0.0001);
    }

    @Test
    void gaugeRegistrationIsNoopWithoutRegistry() {
        SolaceMetrics metrics = new SolaceMetrics(null);
        // Must not throw even though there is no registry.
        metrics.registerConnectionStatusGauge(() -> true);
        metrics.registerActivePublishersGauge(() -> 1);
        metrics.registerActiveConsumersGauge(() -> 1);
        metrics.registerGauge("solace.test", () -> 1);
        assertFalse(metrics.isEnabled());
    }

    @Test
    void recordsRequestSuccessCounterAndTimer() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        metrics.recordRequest(true, false, "rr/quote", 3_000_000L);

        assertEquals(1.0, registry.find(SolaceMetrics.REQUEST_COUNTER)
                .tag("outcome", SolaceMetrics.OUTCOME_SUCCESS).tag("destination", "rr/quote")
                .counter().count(), 0.0001);
        assertEquals(1L, registry.find(SolaceMetrics.REQUEST_TIMER)
                .tag("outcome", SolaceMetrics.OUTCOME_SUCCESS).timer().count());
        // No timeout counter on success.
        assertNull(registry.find(SolaceMetrics.REQUEST_TIMEOUT_COUNTER).counter());
    }

    @Test
    void recordsRequestTimeoutSeparately() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        metrics.recordRequest(false, true, "rr/quote", 5_000_000L);

        assertEquals(1.0, registry.find(SolaceMetrics.REQUEST_COUNTER)
                .tag("outcome", SolaceMetrics.OUTCOME_FAILURE).counter().count(), 0.0001);
        assertNotNull(registry.find(SolaceMetrics.REQUEST_TIMEOUT_COUNTER).counter());
        assertEquals(1.0, registry.find(SolaceMetrics.REQUEST_TIMEOUT_COUNTER).counter().count(), 0.0001);
    }

    @Test
    void recordsReplyOutcomes() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        metrics.recordReply(SolaceMetrics.OUTCOME_SUCCESS, "rr/quote", "replier-1");
        metrics.recordReply(SolaceMetrics.OUTCOME_FAILURE, "rr/quote", "replier-1");
        metrics.recordReply(SolaceMetrics.OUTCOME_NO_REPLY, "rr/quote", "replier-1");

        assertEquals(1.0, registry.find(SolaceMetrics.REPLY_COUNTER)
                .tag("outcome", SolaceMetrics.OUTCOME_SUCCESS).tag("replierId", "replier-1")
                .counter().count(), 0.0001);
        assertEquals(1.0, registry.find(SolaceMetrics.REPLY_COUNTER)
                .tag("outcome", SolaceMetrics.OUTCOME_FAILURE).counter().count(), 0.0001);
        // Intentional no-reply is tracked distinctly from success and failure.
        assertEquals(1.0, registry.find(SolaceMetrics.REPLY_COUNTER)
                .tag("outcome", SolaceMetrics.OUTCOME_NO_REPLY).counter().count(), 0.0001);
    }

    @Test
    void requestReplyMetricsAreNoopWithoutRegistry() {
        SolaceMetrics metrics = new SolaceMetrics(null);
        metrics.recordRequest(true, false, "rr/x", 1L);
        metrics.recordRequest(false, true, "rr/x", 1L);
        metrics.recordReply(SolaceMetrics.OUTCOME_SUCCESS, "rr/x", "r");
        assertFalse(metrics.isEnabled());
    }
}
