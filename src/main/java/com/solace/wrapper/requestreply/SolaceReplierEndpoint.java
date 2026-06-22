package com.solace.wrapper.requestreply;

import com.solace.messaging.MessagingService;
import com.solace.messaging.RequestReplyMessageReceiverBuilder;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.RequestReplyMessageReceiver;
import com.solace.messaging.resources.ShareName;
import com.solace.messaging.resources.TopicSubscription;
import com.solace.wrapper.annotation.SolaceReplier;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.exception.SolaceReplierException;
import com.solace.wrapper.metrics.SolaceMetrics;
import com.solace.wrapper.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns a single Solace request-reply receiver bound to one {@code @SolaceReplier} method.
 *
 * <p>Incoming requests are deserialized to the method's request type, the method is invoked, and a
 * non-{@code null} return value is serialized and sent back as the reply via the SDK-provided
 * {@code Replier} (correlation is automatic). A {@code void}/{@code null} result sends no reply.</p>
 */
public class SolaceReplierEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(SolaceReplierEndpoint.class);

    private final String replierId;
    private final String topic;
    private final String shareName;
    private final String clientName;
    private final Class<?> requestType;
    private final Object bean;
    private final Method method;
    private final SolaceConnectionManager connectionManager;
    private final MessageSerializer serializer;
    private final SolaceReplier.Backpressure backpressure;
    private final int backpressureCapacity;
    private final boolean returnsValue;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private long terminationTimeoutMs = 5000;
    /** Optional metrics facade; never null (defaults to a disabled no-op). */
    private volatile SolaceMetrics metrics = new SolaceMetrics(null);

    private volatile MessagingService messagingService;
    private volatile RequestReplyMessageReceiver receiver;

    public SolaceReplierEndpoint(String replierId, String topic, String shareName, String clientName,
                                 Class<?> requestType, Object bean, Method method,
                                 SolaceConnectionManager connectionManager, MessageSerializer serializer,
                                 SolaceReplier.Backpressure backpressure, int backpressureCapacity) {
        this.replierId = replierId;
        this.topic = topic;
        this.shareName = shareName;
        this.clientName = clientName;
        this.requestType = requestType;
        this.bean = bean;
        this.method = method;
        this.connectionManager = connectionManager;
        this.serializer = serializer;
        this.backpressure = backpressure == null ? SolaceReplier.Backpressure.ELASTIC : backpressure;
        this.backpressureCapacity = Math.max(1, backpressureCapacity);
        this.returnsValue = method.getReturnType() != void.class && method.getReturnType() != Void.class;
        ReflectionUtils.makeAccessible(method);
    }

    /** Sets the termination timeout (ms) used when stopping the receiver. */
    public SolaceReplierEndpoint withTerminationTimeout(long timeoutMs) {
        this.terminationTimeoutMs = Math.max(100, timeoutMs);
        return this;
    }

    /** Injects the metrics facade. Optional: when not set, reply metrics are skipped. */
    public SolaceReplierEndpoint withMetrics(SolaceMetrics metrics) {
        if (metrics != null) {
            this.metrics = metrics;
        }
        return this;
    }

    /** Builds the request-reply receiver and starts serving requests. */
    public synchronized void start() {
        if (running.get() || shutdown.get()) {
            return;
        }
        try {
            messagingService = connectionManager.createConsumerService(replierId, clientName);
            if (messagingService == null || !messagingService.isConnected()) {
                throw new SolaceReplierException("Messaging service is not connected for replier " + replierId);
            }

            RequestReplyMessageReceiverBuilder builder =
                    messagingService.requestReply().createRequestReplyMessageReceiverBuilder();
            applyBackpressure(builder);

            TopicSubscription subscription = TopicSubscription.of(topic);
            receiver = (shareName == null || shareName.isEmpty())
                    ? builder.build(subscription)
                    : builder.build(subscription, ShareName.of(shareName));

            try {
                receiver.setReplyFailureListener(event -> {
                    // A reply that was attempted but failed to deliver (e.g. backpressure/broker side).
                    metrics.recordReply(SolaceMetrics.OUTCOME_FAILURE, topic, replierId);
                    logger.error("Reply delivery failed for replier {} on topic {}: {}", replierId, topic,
                            event != null && event.getException() != null ? event.getException().getMessage() : "unknown");
                });
            } catch (Throwable t) {
                logger.debug("ReplyFailureListener not applied for {}: {}", replierId, t.toString());
            }

            receiver.receiveAsync(this::onRequest);
            receiver.start();
            running.set(true);
            logger.info("Started Solace replier {} on request topic '{}'{}", replierId, topic,
                    (shareName == null || shareName.isEmpty()) ? "" : " (shareName=" + shareName + ")");
        } catch (Exception e) {
            logger.error("Failed to start replier {} on topic {}: {}", replierId, topic, e.getMessage(), e);
            throw new SolaceReplierException("Failed to start replier " + replierId, e);
        }
    }

    private void applyBackpressure(RequestReplyMessageReceiverBuilder builder) {
        try {
            switch (backpressure) {
                case WAIT:
                    builder.onReplierBackPressureWait(backpressureCapacity);
                    break;
                case REJECT:
                    builder.onReplierBackPressureReject(backpressureCapacity);
                    break;
                case ELASTIC:
                default:
                    builder.onReplierBackPressureElastic();
                    break;
            }
        } catch (Throwable t) {
            logger.debug("Replier backpressure config not applied for {}: {}", replierId, t.toString());
        }
    }

    /**
     * Handles a single request: deserialize, invoke the method, and reply with the return value.
     */
    void onRequest(InboundMessage request, RequestReplyMessageReceiver.Replier replier) {
        if (!running.get() || shutdown.get()) {
            return;
        }
        try {
            Object payload = serializer.deserialize(request, requestType);
            Object[] args = buildArguments(payload, request);
            Object result = method.invoke(bean, args);

            if (returnsValue && result != null) {
                OutboundMessage reply = serializer.serialize(messagingService, result);
                replier.reply(reply);
                logger.debug("Replier {} answered request on topic {}", replierId, topic);
                metrics.recordReply(SolaceMetrics.OUTCOME_SUCCESS, topic, replierId);
            } else {
                // Handled, but no reply was produced — the requestor will observe a timeout.
                logger.debug("Replier {} produced no reply for request on topic {} (void/null result)",
                        replierId, topic);
                metrics.recordReply(SolaceMetrics.OUTCOME_NO_REPLY, topic, replierId);
            }
        } catch (Exception e) {
            // No reply is sent on failure; the requestor will observe a timeout.
            metrics.recordReply(SolaceMetrics.OUTCOME_FAILURE, topic, replierId);
            logger.error("Replier {} failed to handle request on topic {}: {}",
                    replierId, topic, e.getMessage(), e);
        }
    }

    private Object[] buildArguments(Object payload, InboundMessage request) {
        Parameter[] parameters = method.getParameters();
        Object[] args = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            Class<?> type = parameters[i].getType();
            if (type.equals(InboundMessage.class)) {
                args[i] = request;
            } else {
                args[i] = payload;
            }
        }
        return args;
    }

    /** Stops the receiver and releases its messaging service. */
    public synchronized void stop() {
        if (!running.get()) {
            return;
        }
        running.set(false);
        try {
            if (receiver != null) {
                receiver.terminate(terminationTimeoutMs);
                receiver = null;
            }
            if (messagingService != null) {
                connectionManager.removeConsumerService(replierId);
                messagingService = null;
            }
            logger.info("Stopped Solace replier {}", replierId);
        } catch (Exception e) {
            logger.error("Error stopping replier {}: {}", replierId, e.getMessage(), e);
        }
    }

    /** Permanently shuts down the replier. */
    public synchronized void shutdown() {
        shutdown.set(true);
        stop();
    }

    public String getReplierId() { return replierId; }
    public String getTopic() { return topic; }
    public Class<?> getRequestType() { return requestType; }
    public boolean isRunning() { return running.get(); }
    public boolean isShutdown() { return shutdown.get(); }
}
