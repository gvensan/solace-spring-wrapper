package com.solace.wrapper.requestreply;

import com.solace.messaging.MessagingService;
import com.solace.messaging.PubSubPlusClientException;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.RequestReplyMessagePublisher;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.resources.Topic;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.exception.SolaceRequestException;
import com.solace.wrapper.exception.SolaceRequestTimeoutException;
import com.solace.wrapper.metrics.SolaceMetrics;
import com.solace.wrapper.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Programmatic Solace request-reply requestor (the client side). Sends a request to a topic and
 * returns the reply, synchronously or asynchronously, reusing the wrapper's pooled connections and
 * serializer. Correlation and the reply-to inbox are handled by the Solace API.
 *
 * <pre>{@code
 * Quote q = requestor.request("pricing/quote/v1", req, Quote.class, Duration.ofSeconds(5));
 * CompletableFuture<Quote> f = requestor.requestAsync("pricing/quote/v1", req, Quote.class);
 * }</pre>
 */
public class SolaceRequestor {

    private static final Logger logger = LoggerFactory.getLogger(SolaceRequestor.class);

    private static final String PRIMARY_KEY = "primary";

    private final SolaceConnectionManager connectionManager;
    private final MessageSerializer serializer;
    private final long defaultTimeoutMs;
    private final Map<String, RequestReplyMessagePublisher> publishers = new ConcurrentHashMap<>();
    private long terminationTimeoutMs = 5000;
    /** Optional metrics facade; never null (defaults to a disabled no-op). */
    private volatile SolaceMetrics metrics = new SolaceMetrics(null);

    public SolaceRequestor(SolaceConnectionManager connectionManager, MessageSerializer serializer) {
        this(connectionManager, serializer, 5000);
    }

    public SolaceRequestor(SolaceConnectionManager connectionManager, MessageSerializer serializer,
                           long defaultTimeoutMs) {
        this.connectionManager = connectionManager;
        this.serializer = serializer;
        this.defaultTimeoutMs = defaultTimeoutMs > 0 ? defaultTimeoutMs : 5000;
        this.terminationTimeoutMs = connectionManager.getProperties().getTerminationTimeoutMs();
    }

    /** Injects the metrics facade. Optional: when not set, request metrics are skipped. */
    public void setMetrics(SolaceMetrics metrics) {
        if (metrics != null) {
            this.metrics = metrics;
        }
    }

    // ---------------------------------------------------------------------
    // Blocking
    // ---------------------------------------------------------------------

    /** Sends a request using the configured default timeout and returns the deserialized reply. */
    public <R> R request(String topic, Object payload, Class<R> replyType) {
        return request(topic, payload, replyType, Duration.ofMillis(defaultTimeoutMs));
    }

    /**
     * Sends a request and blocks until the reply arrives or the timeout elapses.
     *
     * @param topic the request topic
     * @param payload the request payload (serialized via the configured {@link MessageSerializer})
     * @param replyType the type to deserialize the reply into
     * @param timeout the reply timeout (must be positive)
     * @return the deserialized reply
     * @throws SolaceRequestTimeoutException if no reply arrives within {@code timeout}
     * @throws SolaceRequestException on any other failure
     */
    public <R> R request(String topic, Object payload, Class<R> replyType, Duration timeout) {
        long timeoutMs = resolveTimeout(timeout);
        RequestReplyMessagePublisher publisher = getOrCreatePublisher(PRIMARY_KEY, null);
        MessagingService service = connectionManager.createPublisherService(PRIMARY_KEY, null);
        OutboundMessage request = serializer.serialize(service, payload);
        long start = System.nanoTime();
        boolean success = false;
        boolean timedOut = false;
        try {
            InboundMessage reply = publisher.publishAwaitResponse(request, Topic.of(topic), timeoutMs);
            R result = serializer.deserialize(reply, replyType);
            success = true;
            return result;
        } catch (PubSubPlusClientException.TimeoutException e) {
            timedOut = true;
            throw new SolaceRequestTimeoutException(
                    "Request to '" + topic + "' timed out after " + timeoutMs + "ms", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolaceRequestException("Request to '" + topic + "' was interrupted", e);
        } catch (Exception e) {
            throw new SolaceRequestException("Request to '" + topic + "' failed: " + e.getMessage(), e);
        } finally {
            metrics.recordRequest(success, timedOut, topic, System.nanoTime() - start);
        }
    }

    // ---------------------------------------------------------------------
    // Async
    // ---------------------------------------------------------------------

    /** Async request using the configured default timeout. */
    public <R> CompletableFuture<R> requestAsync(String topic, Object payload, Class<R> replyType) {
        return requestAsync(topic, payload, replyType, Duration.ofMillis(defaultTimeoutMs));
    }

    /**
     * Sends a request and returns a future completed with the reply (or completed exceptionally with
     * {@link SolaceRequestTimeoutException} / {@link SolaceRequestException}).
     */
    public <R> CompletableFuture<R> requestAsync(String topic, Object payload, Class<R> replyType,
                                                 Duration timeout) {
        long timeoutMs = resolveTimeout(timeout);
        CompletableFuture<R> future = new CompletableFuture<>();
        long start = System.nanoTime();
        try {
            RequestReplyMessagePublisher publisher = getOrCreatePublisher(PRIMARY_KEY, null);
            MessagingService service = connectionManager.createPublisherService(PRIMARY_KEY, null);
            OutboundMessage request = serializer.serialize(service, payload);

            publisher.publish(request, (reply, userContext, exception) -> {
                long elapsed = System.nanoTime() - start;
                if (exception != null) {
                    boolean timedOut = exception instanceof PubSubPlusClientException.TimeoutException;
                    metrics.recordRequest(false, timedOut, topic, elapsed);
                    future.completeExceptionally(mapAsyncException(topic, timeoutMs, exception));
                } else {
                    try {
                        R result = serializer.deserialize(reply, replyType);
                        metrics.recordRequest(true, false, topic, elapsed);
                        future.complete(result);
                    } catch (Exception de) {
                        metrics.recordRequest(false, false, topic, elapsed);
                        future.completeExceptionally(
                                new SolaceRequestException("Failed to deserialize reply from '" + topic + "'", de));
                    }
                }
            }, null, Topic.of(topic), timeoutMs);
        } catch (Exception e) {
            metrics.recordRequest(false, false, topic, System.nanoTime() - start);
            future.completeExceptionally(
                    new SolaceRequestException("Failed to send request to '" + topic + "': " + e.getMessage(), e));
        }
        return future;
    }

    private SolaceRequestException mapAsyncException(String topic, long timeoutMs, Throwable exception) {
        if (exception instanceof PubSubPlusClientException.TimeoutException) {
            return new SolaceRequestTimeoutException(
                    "Request to '" + topic + "' timed out after " + timeoutMs + "ms", exception);
        }
        return new SolaceRequestException("Request to '" + topic + "' failed: " + exception.getMessage(), exception);
    }

    // ---------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------

    private long resolveTimeout(Duration timeout) {
        long ms = (timeout != null) ? timeout.toMillis() : defaultTimeoutMs;
        if (ms <= 0) {
            throw new SolaceRequestException("Request timeout must be positive (was " + ms + "ms)");
        }
        return ms;
    }

    private RequestReplyMessagePublisher getOrCreatePublisher(String key, String clientName) {
        return publishers.computeIfAbsent(key, k -> {
            try {
                MessagingService service = connectionManager.createPublisherService(k, clientName);
                RequestReplyMessagePublisher publisher = service.requestReply()
                        .createRequestReplyMessagePublisherBuilder()
                        .build();
                publisher.start();
                logger.info("RequestReplyMessagePublisher initialized for key={}", k);
                return publisher;
            } catch (Exception e) {
                throw new SolaceRequestException("Failed to create request-reply publisher for " + k, e);
            }
        });
    }

    /** @return the number of active request-reply publishers (for monitoring/tests). */
    public int getActivePublisherCount() {
        return publishers.size();
    }

    @PreDestroy
    public void shutdown() {
        publishers.values().forEach(p -> {
            try {
                p.terminate(terminationTimeoutMs);
            } catch (Exception e) {
                logger.warn("Error terminating request-reply publisher", e);
            }
        });
        publishers.clear();
    }
}
