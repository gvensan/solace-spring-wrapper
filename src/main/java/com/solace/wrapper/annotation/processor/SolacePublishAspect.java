package com.solace.wrapper.annotation.processor;

import com.solace.wrapper.annotation.SolacePublish;
import com.solace.wrapper.publisher.MessageProperties;
import com.solace.wrapper.publisher.SolacePublisher;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.expression.EvaluationContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * AOP aspect for handling @SolacePublish annotations using the new Solace Java API.
 */
@Aspect
@Component
@Order(100)
public class SolacePublishAspect {

    private static final Logger logger = LoggerFactory.getLogger(SolacePublishAspect.class);

    @Autowired
    public SolacePublisher solacePublisher;

    @Autowired
    private SpelExpressionResolver spelResolver;

    /**
     * Setter for SpelExpressionResolver (used for testing).
     */
    public void setSpelResolver(SpelExpressionResolver spelResolver) {
        this.spelResolver = spelResolver;
    }

    @Around("@annotation(solacePublish)")
    public Object handleSolacePublish(ProceedingJoinPoint joinPoint, SolacePublish solacePublish) throws Throwable {
        // Validate SpEL expressions early for security
        validateAnnotationExpressions(solacePublish, joinPoint);

        // Execute the original method
        Object result = joinPoint.proceed();

        // Skip publishing if result is null
        if (result == null) {
            logger.debug("Skipping Solace publish - method returned null");
            return result;
        }

        try {
            // Create evaluation context for SpEL expressions using the new resolver
            EvaluationContext context = spelResolver.createEvaluationContext(
                ((org.aspectj.lang.reflect.MethodSignature) joinPoint.getSignature()).getMethod(),
                joinPoint.getArgs(),
                result
            );
            
            // Check condition if specified
            if (!solacePublish.condition().isEmpty()) {
                Boolean conditionResult = spelResolver.resolveExpression(solacePublish.condition(), context, Boolean.class);
                if (conditionResult == null || !conditionResult) {
                    logger.debug("Skipping Solace publish - condition evaluated to false");
                    return result;
                }
            }
            
            // Resolve destination
            String destination = spelResolver.resolveExpression(solacePublish.destination(), context, String.class);
            if (destination == null || destination.trim().isEmpty()) {
                logger.warn("Skipping Solace publish - destination is empty");
                return result;
            }

            String resolvedClientName = null;
            if (!solacePublish.clientName().isEmpty()) {
                resolvedClientName = spelResolver.resolveExpression(solacePublish.clientName(), context, String.class);
            }
            String publisherKey = resolvePublisherKey(joinPoint, resolvedClientName);

            // Create message properties
            MessageProperties properties = createMessageProperties(solacePublish, context);

            // Publish the message; choose delivery mode based on annotation
            String resolvedMode = null;
            if (!solacePublish.deliveryMode().isEmpty()) {
                resolvedMode = spelResolver.resolveExpression(solacePublish.deliveryMode(), context, String.class);
            }
            boolean persistent = resolvedMode != null && "PERSISTENT".equalsIgnoreCase(resolvedMode.trim());

            if (solacePublish.async()) {
                solacePublisher.publishWithContextAsync(destination, result, properties, persistent,
                        resolvedClientName, publisherKey);
            } else {
                solacePublisher.publishWithContext(destination, result, properties, persistent,
                        resolvedClientName, publisherKey);
            }

            logger.debug("Published message to {}: {}", destination, result.getClass().getSimpleName());

        } catch (Exception e) {
            logger.error("Failed to publish message via @SolacePublish annotation", e);
            // Don't throw the exception to avoid breaking the original method flow
        }

        return result;
    }


    private MessageProperties createMessageProperties(SolacePublish annotation, EvaluationContext context) {
        MessageProperties properties = new MessageProperties();

        // Correlation ID
        if (!annotation.correlationId().isEmpty()) {
            logger.debug("Resolving correlationId expression: '{}'", annotation.correlationId());
            String correlationId = spelResolver.resolveExpression(annotation.correlationId(), context, String.class);
            logger.debug("Resolved correlationId: '{}'", correlationId);
            if (correlationId != null) {
                properties.setCorrelationId(correlationId);
                logger.debug("Set correlationId in properties: '{}'", properties.getCorrelationId());
            }
        }

        // Reply To
        if (!annotation.replyTo().isEmpty()) {
            String replyTo = spelResolver.resolveExpression(annotation.replyTo(), context, String.class);
            if (replyTo != null) {
                properties.setReplyTo(replyTo);
            }
        }

        // Time to Live
        if (annotation.timeToLive() > 0) {
            properties.setTimeToLive(annotation.timeToLive());
        }

        // Priority
        if (annotation.priority() >= 0) {
            properties.setPriority(annotation.priority());
        }

        // Application Message Type
        if (!annotation.applicationMessageType().isEmpty()) {
            String appMessageType = spelResolver.resolveExpression(annotation.applicationMessageType(), context, String.class);
            if (appMessageType != null) {
                properties.setApplicationMessageType(appMessageType);
            }
        }

        // User properties
        if (annotation.userProperties().length > 0) {
            Map<String, Object> userProps = new HashMap<>();
            for (String prop : annotation.userProperties()) {
                String[] keyValue = prop.split("=", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = spelResolver.resolveExpression(keyValue[1].trim(), context, String.class);
                    if (value != null) {
                        userProps.put(key, value);
                    }
                }
            }
            properties.setUserProperties(userProps);
        }

        // New properties with SpEL support

        // Application Message ID
        if (!annotation.applicationMessageId().isEmpty()) {
            String applicationMessageId = spelResolver.resolveExpression(annotation.applicationMessageId(), context, String.class);
            if (applicationMessageId != null) {
                properties.setApplicationMessageId(applicationMessageId);
            }
        }

        // Eliding Eligible
        if (!annotation.elidingEligible().isEmpty()) {
            Boolean elidingEligible = spelResolver.resolveExpression(annotation.elidingEligible(), context, Boolean.class);
            if (elidingEligible != null) {
                properties.setElidingEligible(elidingEligible);
            }
        }

        // Class of Service
        if (!annotation.classOfService().isEmpty()) {
            Integer classOfService = spelResolver.resolveExpression(annotation.classOfService(), context, Integer.class);
            if (classOfService != null) {
                properties.setClassOfService(classOfService);
            }
        }

        // Delivery Mode
        if (!annotation.deliveryMode().isEmpty()) {
            String deliveryMode = spelResolver.resolveExpression(annotation.deliveryMode(), context, String.class);
            if (deliveryMode != null) {
                properties.setDeliveryMode(deliveryMode);
            }
        }

        // Message Expiration
        if (!annotation.messageExpiration().isEmpty()) {
            Long messageExpiration = spelResolver.resolveExpression(annotation.messageExpiration(), context, Long.class);
            if (messageExpiration != null) {
                properties.setMessageExpiration(messageExpiration);
            }
        }

        // Sequence Number
        if (!annotation.sequenceNumber().isEmpty()) {
            Long sequenceNumber = spelResolver.resolveExpression(annotation.sequenceNumber(), context, Long.class);
            if (sequenceNumber != null) {
                properties.setSequenceNumber(sequenceNumber);
            }
        }

        return properties;
    }

    /**
     * Validates all SpEL expressions in the annotation for security.
     * Prevents SpEL injection attacks by checking for dangerous patterns.
     */
    private void validateAnnotationExpressions(SolacePublish annotation, ProceedingJoinPoint joinPoint) {
        String methodName = joinPoint.getSignature().toShortString();
        try {
            // Validate all SpEL expressions that could be user-provided
            spelResolver.validateExpression(annotation.destination());
            spelResolver.validateExpression(annotation.condition());
            spelResolver.validateExpression(annotation.correlationId());
            spelResolver.validateExpression(annotation.replyTo());
            spelResolver.validateExpression(annotation.applicationMessageType());
            spelResolver.validateExpression(annotation.applicationMessageId());
            spelResolver.validateExpression(annotation.elidingEligible());
            spelResolver.validateExpression(annotation.classOfService());
            spelResolver.validateExpression(annotation.deliveryMode());
            spelResolver.validateExpression(annotation.messageExpiration());
            spelResolver.validateExpression(annotation.sequenceNumber());
            spelResolver.validateExpression(annotation.clientName());

            // Validate user properties values (the part after '=')
            for (String prop : annotation.userProperties()) {
                String[] keyValue = prop.split("=", 2);
                if (keyValue.length == 2) {
                    spelResolver.validateExpression(keyValue[1].trim());
                }
            }
        } catch (IllegalArgumentException e) {
            logger.error("Invalid SpEL expression in @SolacePublish annotation on method {}: {}",
                        methodName, e.getMessage());
            throw new IllegalArgumentException(
                "Invalid SpEL expression in @SolacePublish on " + methodName + ": " + e.getMessage(), e);
        }
    }

    /**
     * Resolves the publisher key for isolated publisher connections.
     */
    private String resolvePublisherKey(ProceedingJoinPoint joinPoint, String clientName) {
        if (clientName != null && !clientName.trim().isEmpty()) {
            return "client:" + clientName.trim();
        }
        return joinPoint.getSignature().toShortString();
    }
}
