package com.solace.wrapper.annotation.processor;

import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.consumer.SolaceAckContext;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceManualAckMessageHandler;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Processor for handling @SolaceConsumer annotations using the new Solace Java API.
 * Scans for annotated methods and registers them as Solace consumers.
 */
@Component
public class SolaceConsumerProcessor implements BeanPostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(SolaceConsumerProcessor.class);

    @Autowired
    private SolaceConsumerManager consumerManager;

    @Autowired
    private SpelExpressionResolver spelResolver;

    /**
     * Setter for SpelExpressionResolver (used for testing).
     */
    public void setSpelResolver(SpelExpressionResolver spelResolver) {
        this.spelResolver = spelResolver;
    }

    private final ExpressionParser expressionParser = new SpelExpressionParser();
    private final AtomicInteger consumerCounter = new AtomicInteger(0);

    // Track registered consumer IDs to prevent duplicate registration from CGLIB proxies
    private final Set<String> registeredConsumerIds = ConcurrentHashMap.newKeySet();

    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        Class<?> beanClass = bean.getClass();
        
        // Scan for @SolaceConsumer methods
        ReflectionUtils.doWithMethods(beanClass, method -> {
            Objects.requireNonNull(method, "method");
            SolaceConsumer annotation = AnnotationUtils.findAnnotation(method, SolaceConsumer.class);
            if (annotation != null) {
                registerSolaceConsumer(bean, method, annotation);
            }
        });

        return bean;
    }

    @SuppressWarnings("null")
    private void registerSolaceConsumer(Object bean, Method method, SolaceConsumer annotation) {
        try {
            // Validate method first
            if (!isValidConsumerMethod(method)) {
                return;
            }

            // Validate SpEL expressions for security
            validateAnnotationExpressions(annotation, method);

            // Make method accessible
            ReflectionUtils.makeAccessible(method);

            // Determine message type
            Class<?> messageType = determineMessageType(method, annotation);
            if (messageType == null) {
                logger.error("Cannot determine message type for method {}", getMethodSignature(method));
                return;
            }

            // Generate consumer ID
            String consumerId = generateConsumerId(annotation, method);

            // Skip if already registered (can happen with CGLIB proxies)
            if (!registeredConsumerIds.add(consumerId)) {
                logger.debug("Consumer {} already registered, skipping duplicate registration", consumerId);
                return;
            }

            // Resolve SpEL expressions for queue and topics (supports @beanName references)
            EvaluationContext spelContext = spelResolver.createBeanOnlyContext();
            String resolvedQueue = resolveSpelExpression(annotation.queue(), spelContext);
            String[] resolvedTopics = resolveSpelTopics(annotation.topics(), spelContext);
            String resolvedClientName = resolveSpelExpression(annotation.clientName(), spelContext);

            logger.debug("Resolved destinations for {}: queue='{}', topics={}, clientName='{}'",
                    getMethodSignature(method), resolvedQueue, Arrays.toString(resolvedTopics), resolvedClientName);

            boolean wantsManualAck = hasAckContextParam(method);
            SolaceConsumer.AckMode effectiveAckMode = annotation.ackMode();
            if (wantsManualAck && effectiveAckMode != SolaceConsumer.AckMode.MANUAL) {
                logger.warn("Method {} declares SolaceAckContext but ackMode is {}. Forcing MANUAL mode.",
                        getMethodSignature(method), effectiveAckMode);
                effectiveAckMode = SolaceConsumer.AckMode.MANUAL;
            }

            String resultConsumerId;
            if (wantsManualAck) {
                SolaceManualAckMessageHandler<?> manualHandler =
                        createManualAckMessageHandler(bean, method, annotation, messageType);
                resultConsumerId = consumerManager.createEnhancedConsumerRaw(
                    consumerId,                    // consumerId
                    resolvedQueue,                 // queueName (SpEL resolved)
                    resolvedTopics,                // topics (SpEL resolved)
                    annotation.mode(),             // messagingMode
                    annotation.autoCreateQueue(),  // autoCreateQueue
                    effectiveAckMode,              // ack mode
                    messageType,                   // messageType (Class<?>)
                    manualHandler,                 // manual-ack handler
                    resolvedClientName,            // clientName override (SpEL resolved)
                    annotation.localMaxAttempts(), // local max attempts
                    annotation.localBackoffInitialMs(), // initial backoff
                    annotation.localBackoffMultiplier(), // backoff multiplier
                    annotation.localBackoffMaxMs(), // max backoff
                    annotation.autoStart()          // auto start
                );
            } else {
                // Create message handler using raw types to avoid generic issues
                SolaceMessageHandler<?> messageHandler = createMessageHandler(bean, method, annotation, messageType);
                resultConsumerId = consumerManager.createEnhancedConsumerRaw(
                    consumerId,                    // consumerId
                    resolvedQueue,                 // queueName (SpEL resolved)
                    resolvedTopics,                // topics (SpEL resolved)
                    annotation.mode(),             // messagingMode
                    annotation.autoCreateQueue(),  // autoCreateQueue
                    effectiveAckMode,              // ack mode
                    messageType,                   // messageType (Class<?>)
                    messageHandler,                // messageHandler (SolaceMessageHandler<?>)
                    resolvedClientName,            // clientName override (SpEL resolved)
                    annotation.localMaxAttempts(), // local max attempts
                    annotation.localBackoffInitialMs(), // initial backoff
                    annotation.localBackoffMultiplier(), // backoff multiplier
                    annotation.localBackoffMaxMs(), // max backoff
                    annotation.autoStart()          // auto start
                );
            }

            // Configure local backoff retry on the created consumer if supported by manager
            try {
                // Best effort: manager exposes consumer instances internally; here we can't fetch it directly
                // so we document that programmatic API can configure backoff. Annotation values are available here
                // for future extension to manager APIs.
                logger.debug("Local backoff config for {} -> attempts={}, initialMs={}, mult={}, maxMs={}",
                        resultConsumerId,
                        annotation.localMaxAttempts(),
                        annotation.localBackoffInitialMs(),
                        annotation.localBackoffMultiplier(),
                        annotation.localBackoffMaxMs());
            } catch (Exception e) {
                logger.debug("Local backoff config logging skipped: {}", e.getMessage());
            }

            // Enhanced logging with mode and destination information
            String destination = buildDestinationDescription(annotation);
            logger.info("Registered Solace consumer '{}' in {} mode for {} with message type '{}' from method {}", 
                       resultConsumerId, annotation.mode(), destination, messageType.getSimpleName(), getMethodSignature(method));

        } catch (Exception e) {
            logger.error("Failed to register Solace consumer for method {}", 
                        getMethodSignature(method), e);
        }
    }

    private Class<?> determineMessageType(Method method, SolaceConsumer annotation) {
        // First, check if explicitly specified in annotation
        if (StringUtils.hasText(annotation.messageType())) {
            try {
                return Class.forName(annotation.messageType());
            } catch (ClassNotFoundException e) {
                logger.warn("Cannot find message type class: {}", annotation.messageType());
            }
        }

        // Infer from method parameters
        Parameter[] parameters = method.getParameters();
        
        // Look for the first parameter that's not InboundMessage or SolaceAckContext
        for (Parameter param : parameters) {
            Class<?> paramType = param.getType();
            if (!paramType.equals(InboundMessage.class) && !paramType.equals(SolaceAckContext.class)) {
                return paramType;
            }
        }

        // Default to String if no specific type found
        return String.class;
    }

    private String generateConsumerId(SolaceConsumer annotation, Method method) {
        StringBuilder consumerId = new StringBuilder();

        // Add consumer prefix if available
        if (StringUtils.hasText(annotation.consumerIdPrefix())) {
            consumerId.append(annotation.consumerIdPrefix()).append("-");
        }

        // Use explicit consumer ID if provided
        if (StringUtils.hasText(annotation.consumerId())) {
            consumerId.append(annotation.consumerId());
        } else {
            // Generate from queue name and method name
            consumerId.append(annotation.queue())
                     .append("-")
                     .append(method.getName())
                     .append("-")
                     .append(consumerCounter.incrementAndGet());
        }

        return consumerId.toString();
    }

    /**
     * Creates a message handler using raw types to avoid generic capture issues.
     */
    @SuppressWarnings("rawtypes")
    private SolaceMessageHandler<?> createMessageHandler(Object bean, Method method, 
                                                        SolaceConsumer annotation, Class<?> messageType) {
        return new SolaceMessageHandler() {
            @Override
            public void handleMessage(Object message, InboundMessage originalMessage) {
                try {
                    // Check condition if specified
                    if (StringUtils.hasText(annotation.condition())) {
                        EvaluationContext context = createEvaluationContext(message, originalMessage);
                        @SuppressWarnings("null")
                        Expression conditionExpr = expressionParser.parseExpression(annotation.condition());
                        @SuppressWarnings("null")
                        Boolean conditionResult = conditionExpr.getValue(context, Boolean.class);
                        if (conditionResult == null || !conditionResult) {
                            logger.debug("Skipping message processing - condition evaluated to false for method {}", 
                                       getMethodSignature(method));
                            return;
                        }
                    }

                    // Prepare method arguments
                    Object[] args = prepareMethodArguments(method, message, originalMessage, null);

                    // Invoke method with retry logic
                    invokeWithRetry(bean, method, args, annotation.maxRetries(), annotation.retryDelay());

                } catch (Exception e) {
                    logger.error("Failed to process message in method {}", getMethodSignature(method), e);
                    throw new RuntimeException("Message processing failed", e);
                }
            }
        };
    }

    @SuppressWarnings("rawtypes")
    private SolaceManualAckMessageHandler<?> createManualAckMessageHandler(Object bean, Method method,
                                                                           SolaceConsumer annotation, Class<?> messageType) {
        return new SolaceManualAckMessageHandler() {
            @Override
            public void handleMessage(Object message, InboundMessage originalMessage, SolaceAckContext ackContext) {
                try {
                    // Check condition if specified
                    if (StringUtils.hasText(annotation.condition())) {
                        EvaluationContext context = createEvaluationContext(message, originalMessage);
                        Expression conditionExpr = expressionParser.parseExpression(annotation.condition());
                        Boolean conditionResult = conditionExpr.getValue(context, Boolean.class);
                        if (conditionResult == null || !conditionResult) {
                            logger.debug("Skipping message processing - condition evaluated to false for method {}",
                                       getMethodSignature(method));
                            return;
                        }
                    }

                    // Prepare method arguments
                    Object[] args = prepareMethodArguments(method, message, originalMessage, ackContext);

                    // Invoke method with retry logic
                    invokeWithRetry(bean, method, args, annotation.maxRetries(), annotation.retryDelay());

                } catch (Exception e) {
                    logger.error("Failed to process message in method {}", getMethodSignature(method), e);
                    throw new RuntimeException("Message processing failed", e);
                }
            }
        };
    }

    private boolean hasAckContextParam(Method method) {
        return Arrays.stream(method.getParameters())
                .anyMatch(param -> param.getType().equals(SolaceAckContext.class));
    }

    private EvaluationContext createEvaluationContext(Object message, InboundMessage originalMessage) {
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.setVariable("message", message);
        context.setVariable("originalMessage", originalMessage);
        context.setRootObject(message);
        return context;
    }

    private Object[] prepareMethodArguments(Method method, Object message,
                                            InboundMessage originalMessage, SolaceAckContext ackContext) {
        Parameter[] parameters = method.getParameters();
        Object[] args = new Object[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
            Class<?> paramType = parameters[i].getType();
            
            if (paramType.equals(InboundMessage.class)) {
                args[i] = originalMessage;
            } else if (paramType.equals(SolaceAckContext.class)) {
                args[i] = ackContext;
            } else if (paramType.isAssignableFrom(message.getClass())) {
                args[i] = message;
            } else {
                // Try to convert or use null
                args[i] = null;
                logger.debug("Unable to map parameter {} of type {} for method {}", 
                           i, paramType.getSimpleName(), getMethodSignature(method));
            }
        }

        return args;
    }

    private void invokeWithRetry(Object bean, Method method, Object[] args, int maxRetries, long retryDelay) 
            throws Exception {
        Exception lastException = null;
        
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                method.invoke(bean, args);
                return; // Success, exit retry loop
            } catch (Exception e) {
                lastException = e;
                if (attempt < maxRetries) {
                    logger.warn("Method invocation failed (attempt {}/{}), retrying in {}ms for method {}", 
                               attempt + 1, maxRetries + 1, retryDelay, getMethodSignature(method), e);
                    try {
                        Thread.sleep(retryDelay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }
        
        // All retries exhausted
        throw new RuntimeException("Method invocation failed after " + (maxRetries + 1) + " attempts for method " + 
                                 getMethodSignature(method), lastException);
    }

    /**
     * Computes local backoff sleep in milliseconds using exponential backoff with cap.
     */
    @SuppressWarnings("unused")
    private long computeBackoff(long initialMs, double multiplier, long maxMs, int attemptIndex) {
        // attemptIndex starts at 0
        double delay = initialMs * Math.pow(multiplier, attemptIndex);
        delay = Math.min(delay, maxMs);
        return (long) delay;
    }

    /**
     * Validates SpEL expressions in the annotation for security.
     * Prevents SpEL injection attacks by checking for dangerous patterns.
     */
    private void validateAnnotationExpressions(SolaceConsumer annotation, Method method) {
        try {
            // Validate condition expression if present
            if (StringUtils.hasText(annotation.condition())) {
                spelResolver.validateExpression(annotation.condition());
            }
        } catch (IllegalArgumentException e) {
            logger.error("Invalid SpEL expression in @SolaceConsumer annotation on method {}: {}",
                        getMethodSignature(method), e.getMessage());
            throw new IllegalArgumentException(
                "Invalid SpEL expression in @SolaceConsumer on " + getMethodSignature(method) + ": " + e.getMessage(), e);
        }
    }

    /**
     * Helper method to validate that a method is suitable for @SolaceConsumer annotation.
     */
    private boolean isValidConsumerMethod(Method method) {
        // Method should not be static
        if (Modifier.isStatic(method.getModifiers())) {
            logger.warn("@SolaceConsumer method {} cannot be static", getMethodSignature(method));
            return false;
        }

        // Method should have at least one parameter
        if (method.getParameterCount() == 0) {
            logger.warn("@SolaceConsumer method {} must have at least one parameter", getMethodSignature(method));
            return false;
        }

        // Method should return void (recommended)
        if (!method.getReturnType().equals(Void.TYPE)) {
            logger.debug("@SolaceConsumer method {} return value will be ignored", getMethodSignature(method));
        }

        return true;
    }

    /**
     * Helper method to get method signature for logging.
     */
    private String getMethodSignature(Method method) {
        StringBuilder signature = new StringBuilder();
        signature.append(method.getDeclaringClass().getSimpleName())
                 .append(".")
                 .append(method.getName())
                 .append("(");
        
        Parameter[] parameters = method.getParameters();
        for (int i = 0; i < parameters.length; i++) {
            if (i > 0) signature.append(", ");
            signature.append(parameters[i].getType().getSimpleName());
        }
        
        signature.append(")");
        return signature.toString();
    }

    /**
     * Builds a description of the consumer destination for logging.
     */
    private String buildDestinationDescription(SolaceConsumer annotation) {
        StringBuilder description = new StringBuilder();

        if (!annotation.queue().isEmpty()) {
            description.append("queue=").append(annotation.queue());
        }

        if (annotation.topics().length > 0) {
            if (description.length() > 0) {
                description.append(", ");
            }
            description.append("topics=").append(Arrays.toString(annotation.topics()));
        }

        return description.toString();
    }

    /**
     * Resolves a single SpEL expression string.
     * Returns the original value if it's not a SpEL expression or resolution fails.
     */
    private String resolveSpelExpression(String expression, EvaluationContext context) {
        if (expression == null || expression.isEmpty()) {
            return expression;
        }
        try {
            String resolved = spelResolver.resolveExpression(expression, context, String.class);
            return resolved != null ? resolved : expression;
        } catch (Exception e) {
            logger.warn("Failed to resolve SpEL expression '{}', using original value: {}", expression, e.getMessage());
            return expression;
        }
    }

    /**
     * Resolves SpEL expressions in an array of topic strings.
     * Each topic can contain #{@beanName.property} expressions.
     */
    private String[] resolveSpelTopics(String[] topics, EvaluationContext context) {
        if (topics == null || topics.length == 0) {
            return topics;
        }
        String[] resolved = new String[topics.length];
        for (int i = 0; i < topics.length; i++) {
            resolved[i] = resolveSpelExpression(topics[i], context);
        }
        return resolved;
    }

    /**
     * Gets statistics about processed annotations.
     */
    public AnnotationProcessorStats getStats() {
        return new AnnotationProcessorStats(
            consumerCounter.get(),
            consumerManager != null ? consumerManager.getTotalConsumerCount() : 0
        );
    }

    /**
     * Statistics class for annotation processor.
     */
    public static class AnnotationProcessorStats {
        private final int processedAnnotations;
        private final int activeConsumers;

        public AnnotationProcessorStats(int processedAnnotations, int activeConsumers) {
            this.processedAnnotations = processedAnnotations;
            this.activeConsumers = activeConsumers;
        }

        public int getProcessedAnnotations() { return processedAnnotations; }
        public int getActiveConsumers() { return activeConsumers; }

        @Override
        public String toString() {
            return String.format("AnnotationProcessorStats{processed=%d, active=%d}", 
                               processedAnnotations, activeConsumers);
        }
    }
}
