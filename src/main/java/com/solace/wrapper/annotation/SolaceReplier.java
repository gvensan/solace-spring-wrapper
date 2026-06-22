package com.solace.wrapper.annotation;

import java.lang.annotation.*;

/**
 * Marks a method as a Solace <em>request-reply</em> replier (the service side of the
 * request-reply pattern).
 *
 * <p>The annotated method receives the deserialized request as its (first) parameter and its
 * <strong>return value is serialized and sent back as the reply</strong>. A {@code void} method —
 * or one that returns {@code null} — sends no reply, which the requestor observes as a timeout.</p>
 *
 * <p>The method may optionally declare a {@link com.solace.messaging.receiver.InboundMessage}
 * parameter to access the raw request message (headers, properties), mirroring
 * {@link SolaceConsumer}.</p>
 *
 * <p>Request-reply uses direct (topic-based) messaging; correlation and the reply-to inbox are
 * handled automatically by the Solace API.</p>
 *
 * <pre>{@code
 * @Component
 * class PricingService {
 *     @SolaceReplier(topic = "pricing/quote/v1")
 *     public Quote handle(QuoteRequest req) {
 *         return priceEngine.quote(req);   // returned value becomes the reply
 *     }
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SolaceReplier {

    /**
     * The request topic subscription this replier listens on. Supports SpEL (e.g.
     * {@code "#{@cfg.quoteTopic}"}) and wildcards. Required.
     */
    String topic();

    /**
     * Optional shared-subscription name. When set, multiple replier instances sharing this name
     * load-balance incoming requests. SpEL-enabled.
     */
    String shareName() default "";

    /**
     * Explicit request message type class name. If empty, the type is inferred from the first
     * method parameter that is not an {@link com.solace.messaging.receiver.InboundMessage}.
     */
    String messageType() default "";

    /** Optional explicit replier id. If empty, one is generated. */
    String replierId() default "";

    /** Optional prefix for the generated replier id. */
    String replierIdPrefix() default "";

    /** Optional client name override for the replier connection. SpEL-enabled. */
    String clientName() default "";

    /** Whether to start the replier automatically on application startup. */
    boolean autoStart() default true;

    /**
     * Backpressure strategy applied to the replier receiver's outbound (reply) buffer.
     */
    Backpressure backpressure() default Backpressure.ELASTIC;

    /**
     * Buffer capacity for {@link Backpressure#WAIT} / {@link Backpressure#REJECT} strategies.
     * Ignored for {@link Backpressure#ELASTIC}.
     */
    int backpressureCapacity() default 1024;

    /**
     * Replier backpressure strategies, mapping to the Solace request-reply receiver builder.
     */
    enum Backpressure {
        /** Unbounded, elastic buffering (default). */
        ELASTIC,
        /** Block the publishing thread until buffer space is available. */
        WAIT,
        /** Reject (drop) replies when the buffer is full. */
        REJECT
    }
}
