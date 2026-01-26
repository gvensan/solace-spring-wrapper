package com.solace.wrapper.annotation;

import java.lang.annotation.*;

/**
 * Annotation to mark methods for automatic Solace message publishing.
 * When applied to a method, the return value will be published to the specified destination.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SolacePublish {

    /**
     * The destination to publish to (topic or queue).
     * Supports SpEL expressions for dynamic destinations.
     */
    String destination();

    /**
     * Optional client name override for the publishing connection.
     * Supports SpEL expressions.
     */
    String clientName() default "";

    /**
     * Correlation ID for the message.
     * Supports SpEL expressions.
     */
    String correlationId() default "";

    /**
     * Reply-to destination.
     * Supports SpEL expressions.
     */
    String replyTo() default "";

    /**
     * Time to live in milliseconds.
     * Default is -1 (no TTL).
     */
    long timeToLive() default -1;

    /**
     * Message priority (0-255).
     * Default is -1 (no priority set).
     */
    int priority() default -1;

    /**
     * Application message type.
     * Supports SpEL expressions.
     */
    String applicationMessageType() default "";

    /**
     * Whether to publish asynchronously.
     */
    boolean async() default false;

    /**
     * User properties as key=value pairs.
     * Supports SpEL expressions in values.
     */
    String[] userProperties() default {};

    /**
     * Condition for publishing (SpEL expression).
     * Message will only be published if this evaluates to true.
     */
    String condition() default "";

    /**
     * Application message ID for tracking and correlation.
     * Supports SpEL expressions for dynamic ID generation.
     */
    String applicationMessageId() default "";

    /**
     * Whether the message is eligible for eliding (SpEL expression).
     * Evaluates to boolean - true if message should be eligible for eliding.
     * Supports SpEL expressions for dynamic evaluation.
     */
    String elidingEligible() default "";

    /**
     * Class of service for message prioritization (SpEL expression).
     * Should evaluate to integer 0-3 for different priority classes.
     * Supports SpEL expressions for dynamic CoS assignment.
     */
    String classOfService() default "";

    /**
     * Delivery mode for message durability (SpEL expression).
     * Should evaluate to "DIRECT" or "PERSISTENT".
     * Supports SpEL expressions for conditional delivery mode.
     */
    String deliveryMode() default "";

    /**
     * Absolute message expiration timestamp (SpEL expression).
     * Should evaluate to long timestamp in milliseconds.
     * Supports SpEL expressions for dynamic expiration calculation.
     */
    String messageExpiration() default "";

    /**
     * Message sequence number for ordering (SpEL expression).
     * Should evaluate to long sequence number.
     * Supports SpEL expressions for dynamic sequence generation.
     */
    String sequenceNumber() default "";

}
