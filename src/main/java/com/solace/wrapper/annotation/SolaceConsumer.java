package com.solace.wrapper.annotation;

import java.lang.annotation.*;

/**
 * Annotation to mark methods as Solace message consumers.
 * Supports both queue-based persistent messaging and topic-based direct messaging.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SolaceConsumer {

    /**
     * The queue name to consume from.
     * If not specified, direct messaging mode will be used with topic subscriptions.
     */
    String queue() default "";

    /**
     * Topics to subscribe to for direct messaging or queue topic subscriptions.
     * Supports wildcards (e.g., "orders/*", "notifications/high").
     * For persistent messaging: topics are subscribed to the queue.
     * For direct messaging: topics are subscribed directly to the consumer.
     */
    String[] topics() default {};

    /**
     * Messaging mode for the consumer.
     * PERSISTENT: Queue-based messaging with guaranteed delivery (default when queue is specified).
     * DIRECT: Topic-based messaging with best-effort delivery (default when only topics are specified).
     */
    MessagingMode mode() default MessagingMode.AUTO;

    /**
     * Whether to automatically create the queue if it doesn't exist.
     * Only applicable for persistent messaging mode.
     * Requires broker configuration to support dynamic queue creation and a client
     * version that supports MissingResourcesCreationStrategy.
     */
    boolean autoCreateQueue() default true;

    /**
     * Consumer ID. If not specified, one will be generated.
     */
    String consumerId() default "";

    /**
     * Optional prefix for the generated consumer ID.
     */
    String consumerIdPrefix() default "";

    /**
     * Optional client name override for the consumer connection.
     */
    String clientName() default "";

    /**
     * The expected message type class name.
     * If not specified, will be inferred from method parameter.
     */
    String messageType() default "";

    /**
     * Whether to start the consumer automatically on application startup.
     */
    boolean autoStart() default true;

    /**
     * Acknowledgement mode for persistent consumers.
     * AUTO: broker/client handles acks automatically (default).
     * MANUAL: application must ack/fail via {@link com.solace.wrapper.consumer.SolaceAckContext}.
     * Ignored for direct messaging.
     */
    AckMode ackMode() default AckMode.AUTO;

    /**
     * Maximum number of retry attempts for failed messages.
     * Only applicable for persistent messaging mode.
     */
    int maxRetries() default 3;

    /**
     * Delay between retries in milliseconds.
     * Only applicable for persistent messaging mode.
     */
    long retryDelay() default 1000;

    /**
     * Local retry attempts before nacking (persistent) or giving up (direct).
     * Set to 1 to disable local backoff retry (default: 1).
     */
    int localMaxAttempts() default 1;

    /**
     * Initial backoff in milliseconds between local attempts (default: 200 ms).
     */
    long localBackoffInitialMs() default 200;

    /**
     * Exponential backoff multiplier between attempts (default: 2.0).
     */
    double localBackoffMultiplier() default 2.0;

    /**
     * Maximum backoff cap in milliseconds (default: 2000 ms).
     */
    long localBackoffMaxMs() default 2000;

    /**
     * Condition for processing messages (SpEL expression).
     * Message will only be processed if this evaluates to true.
     */
    String condition() default "";

    /**
     * Messaging mode enumeration.
     */
    enum MessagingMode {
        /**
         * Automatically determine mode based on annotation parameters.
         * PERSISTENT if queue is specified, DIRECT if only topics are specified.
         */
        AUTO,
        
        /**
         * Persistent messaging with guaranteed delivery.
         * Requires a queue to be specified.
         */
        PERSISTENT,
        
        /**
         * Direct messaging with best-effort delivery.
         * Requires topics to be specified.
         */
        DIRECT
    }

    /**
     * Acknowledgement mode options for persistent messaging.
     */
    enum AckMode {
        AUTO,
        MANUAL
    }
}
