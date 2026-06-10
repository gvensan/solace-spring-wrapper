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
        long start = System.nanoTime();
        boolean success = false;
        boolean timedOut = false;
        try {
            InboundMessage reply = sendAwaitWithRetry(topic, payload, timeoutMs);
            R result = serializer.deserialize(reply, replyType);
            success = true;
            return result;
        } catch (SolaceRequestTimeoutException e) {
            timedOut = true;
            throw e;
        } finally {
            metrics.recordRequest(success, timedOut, topic, System.nanoTime() - start);
        }
    }

    /**
     * Sends a request and awaits the reply, retrying once with a freshly rebuilt publisher when the
     * first attempt fails for a reason other than a reply timeout (a timeout means no replier
     * answered, which a rebuild cannot fix). This prevents a single poisoned publisher from
     * stranding all subsequent request-reply calls.
     */
    private InboundMessage sendAwaitWithRetry(String topic, Object payload, long timeoutMs) {
        try {
            return sendAwait(topic, payload, timeoutMs);
        } catch (PubSubPlusClientException.TimeoutException e) {
            throw timeoutException(topic, timeoutMs, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolaceRequestException("Request to '" + topic + "' was interrupted", e);
        } catch (Exception e) {
            logger.warn("Request to '{}' failed, reinitializing request-reply publisher and retrying", topic, e);
            reinitPublisher(PRIMARY_KEY);
            try {
                return sendAwait(topic, payload, timeoutMs);
            } catch (PubSubPlusClientException.TimeoutException e2) {
                throw timeoutException(topic, timeoutMs, e2);
            } catch (InterruptedException e2) {
                Thread.currentThread().interrupt();
                throw new SolaceRequestException("Request to '" + topic + "' was interrupted", e2);
            } catch (Exception e2) {
                throw new SolaceRequestException(
                        "Request to '" + topic + "' failed after retry: " + e2.getMessage(), e2);
            }
        }
    }

    private InboundMessage sendAwait(String topic, Object payload, long timeoutMs)
            throws InterruptedException {
        RequestReplyMessagePublisher publisher = getOrCreatePublisher(PRIMARY_KEY, null);
        MessagingService service = connectionManager.createPublisherService(PRIMARY_KEY, null);
        OutboundMessage request = serializer.serialize(service, payload);
        return publisher.publishAwaitResponse(request, Topic.of(topic), timeoutMs);
    }

    private SolaceRequestTimeoutException timeoutException(String topic, long timeoutMs, Throwable cause) {
        return new SolaceRequestTimeoutException(
                "Request to '" + topic + "' timed out after " + timeoutMs + "ms", cause);
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
            publishAsyncOnce(topic, payload, replyType, timeoutMs, future, start);
        } catch (Exception e) {
            // Synchronous publish failure (e.g. poisoned publisher): rebuild and retry once.
            logger.warn("Async request to '{}' failed to send, reinitializing publisher and retrying", topic, e);
            reinitPublisher(PRIMARY_KEY);
            try {
                publishAsyncOnce(topic, payload, replyType, timeoutMs, future, start);
            } catch (Exception e2) {
                metrics.recordRequest(false, false, topic, System.nanoTime() - start);
                future.completeExceptionally(new SolaceRequestException(
                        "Failed to send request to '" + topic + "' after retry: " + e2.getMessage(), e2));
            }
        }
        return future;
    }

    private <R> void publishAsyncOnce(String topic, Object payload, Class<R> replyType, long timeoutMs,
                                      CompletableFuture<R> future, long start) {
        RequestReplyMessagePublisher publisher = getOrCreatePublisher(PRIMARY_KEY, null);
        MessagingService service = connectionManager.createPublisherService(PRIMARY_KEY, null);
        OutboundMessage request = serializer.serialize(service, payload);

        publisher.publish(request, (reply, userContext, exception) -> {
            long elapsed = System.nanoTime() - start;
            if (exception != null) {
                boolean timedOut = exception instanceof PubSubPlusClientException.TimeoutException;
                metrics.recordRequest(false, timedOut, topic, elapsed);
                if (!timedOut) {
                    // A non-timeout reply error may indicate a broken publisher; evict so the next
                    // call rebuilds it (no terminate here to avoid blocking the SDK callback thread).
                    publishers.remove(PRIMARY_KEY);
                }
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

    /** Removes and terminates the cached publisher for {@code key} so the next call rebuilds it. */
    private void reinitPublisher(String key) {
        RequestReplyMessagePublisher existing = publishers.remove(key);
        if (existing != null) {
            try {
                existing.terminate(terminationTimeoutMs);
            } catch (Exception e) {
                logger.debug("Error terminating request-reply publisher during reinit: {}", e.getMessage());
            }
        }
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
