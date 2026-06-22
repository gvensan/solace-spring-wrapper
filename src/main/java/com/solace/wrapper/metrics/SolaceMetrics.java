package com.solace.wrapper.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Central, null-safe facade for emitting Micrometer metrics from the Solace wrapper.
 *
 * <p>This class is intentionally tolerant of a {@code null} {@link MeterRegistry}: when no
 * registry is supplied (for example in unit tests that construct the publisher/consumer
 * directly, or when metrics are disabled) every method becomes a cheap no-op. This keeps
 * the existing public constructors of {@code SolacePublisher}, {@code SolaceConsumerManager}
 * and {@code SolaceConsumer} unchanged and avoids forcing Micrometer onto callers.</p>
 *
 * <p>Meter names follow the {@code solace.*} convention so they are easy to select in
 * Prometheus/Grafana:</p>
 * <ul>
 *   <li>{@code solace.publish.total} / {@code solace.publish.latency} (tags: outcome, deliveryMode, destination, clientName)</li>
 *   <li>{@code solace.consume.total} / {@code solace.consume.latency} (tags: outcome, destination, consumerId)</li>
 *   <li>{@code solace.publish.backpressure.rejected} / {@code solace.consume.retries.total}</li>
 *   <li>{@code solace.connection.up}, {@code solace.publishers.active}, {@code solace.consumers.active} (gauges)</li>
 * </ul>
 */
public class SolaceMetrics {

    private static final Logger logger = LoggerFactory.getLogger(SolaceMetrics.class);

    /** Outcome tag values. */
    public static final String OUTCOME_SUCCESS = "success";
    public static final String OUTCOME_FAILURE = "failure";
    /** Replier handled the request but intentionally produced no reply (void/null result). */
    public static final String OUTCOME_NO_REPLY = "no_reply";

    static final String PUBLISH_COUNTER = "solace.publish.total";
    static final String PUBLISH_TIMER = "solace.publish.latency";
    static final String PUBLISH_REJECTED_COUNTER = "solace.publish.backpressure.rejected";
    static final String CONSUME_COUNTER = "solace.consume.total";
    static final String CONSUME_TIMER = "solace.consume.latency";
    static final String CONSUME_RETRY_COUNTER = "solace.consume.retries.total";
    static final String CONNECTION_GAUGE = "solace.connection.up";
    static final String PUBLISHERS_GAUGE = "solace.publishers.active";
    static final String CONSUMERS_GAUGE = "solace.consumers.active";
    static final String REQUEST_COUNTER = "solace.request.total";
    static final String REQUEST_TIMER = "solace.request.latency";
    static final String REQUEST_TIMEOUT_COUNTER = "solace.request.timeouts.total";
    static final String REPLY_COUNTER = "solace.reply.total";

    private static final String UNKNOWN = "unknown";

    private final MeterRegistry registry;
    private final boolean includeDestinationTag;

    // Cache meters by their fully-resolved key to avoid repeated registry look-ups on the hot path.
    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();
    // Strong references to gauge state suppliers: Micrometer holds only a weak reference to the
    // state object, so we must retain them ourselves to prevent the gauge reading NaN after GC.
    private final List<Object> gaugeStateRefs = new CopyOnWriteArrayList<>();

    /**
     * Creates a metrics facade.
     *
     * @param registry the Micrometer registry, or {@code null} to disable metrics (no-op mode)
     * @param includeDestinationTag whether to tag publish/consume meters with the destination
     *        (topic/queue). Disabling reduces cardinality on systems with many destinations.
     */
    public SolaceMetrics(MeterRegistry registry, boolean includeDestinationTag) {
        this.registry = registry;
        this.includeDestinationTag = includeDestinationTag;
        if (registry == null) {
            logger.debug("SolaceMetrics initialized without a MeterRegistry; metrics are disabled (no-op).");
        } else {
            logger.info("SolaceMetrics initialized with registry {} (includeDestinationTag={})",
                    registry.getClass().getSimpleName(), includeDestinationTag);
        }
    }

    /** Convenience constructor that includes the destination tag. */
    public SolaceMetrics(MeterRegistry registry) {
        this(registry, true);
    }

    /** @return {@code true} if a registry is present and metrics are being recorded. */
    public boolean isEnabled() {
        return registry != null;
    }

    /** @return the underlying registry, or {@code null} when disabled. */
    public MeterRegistry getRegistry() {
        return registry;
    }

    // ---------------------------------------------------------------------
    // Publish metrics
    // ---------------------------------------------------------------------

    /**
     * Records a publish outcome.
     *
     * @param success whether the publish succeeded
     * @param deliveryMode delivery mode (e.g. {@code DIRECT} or {@code PERSISTENT})
     * @param destination topic or queue name
     * @param clientName the Solace client name (may be {@code null})
     */
    public void recordPublish(boolean success, String deliveryMode, String destination, String clientName) {
        if (registry == null) {
            return;
        }
        Tags tags = publishTags(success ? OUTCOME_SUCCESS : OUTCOME_FAILURE, deliveryMode, destination, clientName);
        counter(PUBLISH_COUNTER, tags).increment();
    }

    /**
     * Records publish latency (and the outcome counter) for an operation.
     *
     * @param nanos elapsed time in nanoseconds
     */
    public void recordPublishLatency(boolean success, String deliveryMode, String destination,
                                     String clientName, long nanos) {
        if (registry == null) {
            return;
        }
        Tags tags = publishTags(success ? OUTCOME_SUCCESS : OUTCOME_FAILURE, deliveryMode, destination, clientName);
        counter(PUBLISH_COUNTER, tags).increment();
        timer(PUBLISH_TIMER, tags).record(nanos, TimeUnit.NANOSECONDS);
    }

    /** Records a publish rejected due to backpressure on the publisher. */
    public void recordPublishRejected(String deliveryMode, String destination, String clientName) {
        if (registry == null) {
            return;
        }
        Tags tags = publishTags(OUTCOME_FAILURE, deliveryMode, destination, clientName);
        counter(PUBLISH_REJECTED_COUNTER, tags).increment();
    }

    /**
     * Times the supplied publish action, recording latency and success/failure automatically.
     * Exceptions are re-thrown after being counted as a failure.
     */
    public <T> T timePublish(String deliveryMode, String destination, String clientName, Supplier<T> action) {
        if (registry == null) {
            return action.get();
        }
        long start = System.nanoTime();
        boolean success = false;
        try {
            T result = action.get();
            success = true;
            return result;
        } finally {
            recordPublishLatency(success, deliveryMode, destination, clientName, System.nanoTime() - start);
        }
    }

    /** Times a publish action with no return value. */
    public void timePublish(String deliveryMode, String destination, String clientName, Runnable action) {
        timePublish(deliveryMode, destination, clientName, () -> {
            action.run();
            return null;
        });
    }

    // ---------------------------------------------------------------------
    // Consume metrics
    // ---------------------------------------------------------------------

    /**
     * Records a consume outcome.
     *
     * @param success whether the message was processed successfully
     * @param destination queue or topic the message was received from
     * @param consumerId the consumer identifier
     */
    public void recordConsume(boolean success, String destination, String consumerId) {
        if (registry == null) {
            return;
        }
        Tags tags = consumeTags(success ? OUTCOME_SUCCESS : OUTCOME_FAILURE, destination, consumerId);
        counter(CONSUME_COUNTER, tags).increment();
    }

    /** Records consume latency together with the outcome counter. */
    public void recordConsumeLatency(boolean success, String destination, String consumerId, long nanos) {
        if (registry == null) {
            return;
        }
        Tags tags = consumeTags(success ? OUTCOME_SUCCESS : OUTCOME_FAILURE, destination, consumerId);
        counter(CONSUME_COUNTER, tags).increment();
        timer(CONSUME_TIMER, tags).record(nanos, TimeUnit.NANOSECONDS);
    }

    /** Records a consumer retry (local backoff re-attempt). */
    public void recordConsumeRetry(String destination, String consumerId) {
        if (registry == null) {
            return;
        }
        Tags tags = consumeTags(null, destination, consumerId);
        counter(CONSUME_RETRY_COUNTER, tags).increment();
    }

    // ---------------------------------------------------------------------
    // Request-reply metrics
    // ---------------------------------------------------------------------

    /**
     * Records a request-reply request from the requestor side: an outcome counter, a latency timer,
     * and (when applicable) a dedicated timeout counter.
     *
     * @param success whether a reply was received and deserialized successfully
     * @param timedOut whether the failure was a reply timeout (only meaningful when {@code !success})
     * @param destination the request topic
     * @param nanos elapsed request-to-reply time in nanoseconds
     */
    public void recordRequest(boolean success, boolean timedOut, String destination, long nanos) {
        if (registry == null) {
            return;
        }
        Tags tags = requestTags(success ? OUTCOME_SUCCESS : OUTCOME_FAILURE, destination);
        counter(REQUEST_COUNTER, tags).increment();
        timer(REQUEST_TIMER, tags).record(nanos, TimeUnit.NANOSECONDS);
        if (!success && timedOut) {
            counter(REQUEST_TIMEOUT_COUNTER, requestTags(null, destination)).increment();
        }
    }

    /**
     * Records a reply outcome from the replier side.
     *
     * @param outcome one of {@link #OUTCOME_SUCCESS} (reply delivered), {@link #OUTCOME_NO_REPLY}
     *        (handled but no reply sent — void/null), or {@link #OUTCOME_FAILURE} (handler threw or
     *        reply delivery failed)
     * @param destination the request topic
     * @param replierId the replier identifier
     */
    public void recordReply(String outcome, String destination, String replierId) {
        if (registry == null) {
            return;
        }
        counter(REPLY_COUNTER, replyTags(outcome, destination, replierId)).increment();
    }

    // ---------------------------------------------------------------------
    // Gauges
    // ---------------------------------------------------------------------

    /**
     * Registers a polling gauge backed by the supplied function. The supplier is strongly
     * referenced for the lifetime of this facade so the gauge does not read {@code NaN} after the
     * caller's reference is garbage-collected. No-op when metrics are disabled.
     *
     * @param name meter name
     * @param supplier value supplier, evaluated on each scrape; {@code null} values report {@code NaN}
     */
    public void registerGauge(String name, Supplier<Number> supplier) {
        if (registry == null || supplier == null) {
            return;
        }
        gaugeStateRefs.add(supplier);
        Gauge.builder(name, supplier, s -> {
            Number value = s.get();
            return value == null ? Double.NaN : value.doubleValue();
        }).register(registry);
    }

    /**
     * Registers the {@code solace.connection.up} gauge: {@code 1} when connected, {@code 0} otherwise.
     */
    public void registerConnectionStatusGauge(BooleanSupplier connected) {
        if (connected == null) {
            return;
        }
        registerGauge(CONNECTION_GAUGE, () -> connected.getAsBoolean() ? 1 : 0);
    }

    /** Registers the {@code solace.publishers.active} gauge. */
    public void registerActivePublishersGauge(Supplier<Number> count) {
        registerGauge(PUBLISHERS_GAUGE, count);
    }

    /** Registers the {@code solace.consumers.active} gauge. */
    public void registerActiveConsumersGauge(Supplier<Number> count) {
        registerGauge(CONSUMERS_GAUGE, count);
    }

    // ---------------------------------------------------------------------
    // Tag helpers
    // ---------------------------------------------------------------------

    private Tags publishTags(String outcome, String deliveryMode, String destination, String clientName) {
        List<Tag> tags = new ArrayList<>(4);
        tags.add(Tag.of("outcome", safe(outcome)));
        tags.add(Tag.of("deliveryMode", safe(deliveryMode)));
        if (includeDestinationTag) {
            tags.add(Tag.of("destination", safe(destination)));
        }
        tags.add(Tag.of("clientName", safe(clientName)));
        return Tags.of(tags);
    }

    private Tags consumeTags(String outcome, String destination, String consumerId) {
        List<Tag> tags = new ArrayList<>(3);
        if (outcome != null) {
            tags.add(Tag.of("outcome", safe(outcome)));
        }
        if (includeDestinationTag) {
            tags.add(Tag.of("destination", safe(destination)));
        }
        tags.add(Tag.of("consumerId", safe(consumerId)));
        return Tags.of(tags);
    }

    private Tags requestTags(String outcome, String destination) {
        List<Tag> tags = new ArrayList<>(2);
        if (outcome != null) {
            tags.add(Tag.of("outcome", safe(outcome)));
        }
        if (includeDestinationTag) {
            tags.add(Tag.of("destination", safe(destination)));
        }
        return Tags.of(tags);
    }

    private Tags replyTags(String outcome, String destination, String replierId) {
        List<Tag> tags = new ArrayList<>(3);
        tags.add(Tag.of("outcome", safe(outcome)));
        if (includeDestinationTag) {
            tags.add(Tag.of("destination", safe(destination)));
        }
        tags.add(Tag.of("replierId", safe(replierId)));
        return Tags.of(tags);
    }

    private static String safe(String value) {
        return (value == null || value.isEmpty()) ? UNKNOWN : value;
    }

    private Counter counter(String name, Tags tags) {
        return counters.computeIfAbsent(name + tags, k -> registry.counter(name, tags));
    }

    private Timer timer(String name, Tags tags) {
        return timers.computeIfAbsent(name + tags, k -> Timer.builder(name)
                .tags(tags)
                .publishPercentileHistogram()
                .register(registry));
    }
}
