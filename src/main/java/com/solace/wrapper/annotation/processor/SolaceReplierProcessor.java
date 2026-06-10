package com.solace.wrapper.annotation.processor;

import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.annotation.SolaceReplier;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.requestreply.SolaceReplierEndpoint;
import com.solace.wrapper.serialization.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.EvaluationContext;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import jakarta.annotation.PreDestroy;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Discovers {@link SolaceReplier}-annotated methods and registers a request-reply replier for each,
 * mirroring {@link SolaceConsumerProcessor}. The method's return value is sent as the reply.
 */
@Component
public class SolaceReplierProcessor implements BeanPostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(SolaceReplierProcessor.class);

    @Autowired
    private SolaceConnectionManager connectionManager;

    @Autowired
    private MessageSerializer messageSerializer;

    @Autowired
    private SpelExpressionResolver spelResolver;

    private final AtomicInteger replierCounter = new AtomicInteger(0);
    private final Set<String> registeredReplierIds = ConcurrentHashMap.newKeySet();
    private final Map<String, SolaceReplierEndpoint> endpoints = new ConcurrentHashMap<>();

    /** Setter for the SpEL resolver (used for testing). */
    public void setSpelResolver(SpelExpressionResolver spelResolver) {
        this.spelResolver = spelResolver;
    }

    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        ReflectionUtils.doWithMethods(bean.getClass(), method -> {
            Objects.requireNonNull(method, "method");
            SolaceReplier annotation = AnnotationUtils.findAnnotation(method, SolaceReplier.class);
            if (annotation != null) {
                registerReplier(bean, method, annotation);
            }
        });
        return bean;
    }

    private void registerReplier(Object bean, Method method, SolaceReplier annotation) {
        try {
            if (!isValidReplierMethod(method)) {
                return;
            }
            spelResolver.validateExpression(annotation.topic());
            spelResolver.validateExpression(annotation.clientName());
            spelResolver.validateExpression(annotation.shareName());

            Class<?> requestType = determineRequestType(method, annotation);

            String replierId = generateReplierId(annotation, method);
            if (!registeredReplierIds.add(replierId)) {
                logger.debug("Replier {} already registered, skipping duplicate", replierId);
                return;
            }

            EvaluationContext ctx = spelResolver.createBeanOnlyContext();
            String topic = resolve(annotation.topic(), ctx);
            String shareName = resolve(annotation.shareName(), ctx);
            String clientName = resolve(annotation.clientName(), ctx);

            if (topic == null || topic.trim().isEmpty()) {
                logger.error("@SolaceReplier on {} has an empty topic; skipping", getMethodSignature(method));
                registeredReplierIds.remove(replierId);
                return;
            }

            SolaceReplierEndpoint endpoint = new SolaceReplierEndpoint(
                    replierId, topic, shareName, clientName, requestType, bean, method,
                    connectionManager, messageSerializer,
                    annotation.backpressure(), annotation.backpressureCapacity());
            endpoint.withTerminationTimeout(connectionManager.getProperties().getTerminationTimeoutMs());

            endpoints.put(replierId, endpoint);
            if (annotation.autoStart()) {
                endpoint.start();
                logger.info("Registered and started Solace replier '{}' on topic '{}' for {} (request type {})",
                        replierId, topic, getMethodSignature(method), requestType.getSimpleName());
            } else {
                logger.info("Registered Solace replier '{}' on topic '{}' for {} (autoStart=false)",
                        replierId, topic, getMethodSignature(method));
            }
        } catch (Exception e) {
            logger.error("Failed to register Solace replier for method {}", getMethodSignature(method), e);
        }
    }

    private boolean isValidReplierMethod(Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            logger.warn("@SolaceReplier method {} cannot be static", getMethodSignature(method));
            return false;
        }
        return true;
    }

    private Class<?> determineRequestType(Method method, SolaceReplier annotation) {
        if (StringUtils.hasText(annotation.messageType())) {
            try {
                return Class.forName(annotation.messageType());
            } catch (ClassNotFoundException e) {
                logger.warn("Cannot find replier messageType class: {}", annotation.messageType());
            }
        }
        for (Parameter param : method.getParameters()) {
            if (!param.getType().equals(InboundMessage.class)) {
                return param.getType();
            }
        }
        return String.class;
    }

    private String generateReplierId(SolaceReplier annotation, Method method) {
        StringBuilder id = new StringBuilder();
        if (StringUtils.hasText(annotation.replierIdPrefix())) {
            id.append(annotation.replierIdPrefix()).append("-");
        }
        if (StringUtils.hasText(annotation.replierId())) {
            id.append(annotation.replierId());
        } else {
            id.append("replier-").append(method.getName()).append("-").append(replierCounter.incrementAndGet());
        }
        return id.toString();
    }

    private String resolve(String expression, EvaluationContext context) {
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

    private String getMethodSignature(Method method) {
        return method.getDeclaringClass().getSimpleName() + "." + method.getName();
    }

    // ----- management / lifecycle -----

    /** @return an immutable snapshot of the registered replier endpoints. */
    public Collection<SolaceReplierEndpoint> getEndpoints() {
        return endpoints.values();
    }

    /** @return the endpoint for the given id, or {@code null}. */
    public SolaceReplierEndpoint getEndpoint(String replierId) {
        return endpoints.get(replierId);
    }

    /** @return number of registered repliers. */
    public int getReplierCount() {
        return endpoints.size();
    }

    /** Starts any registered replier that is not yet running (e.g. autoStart=false ones). */
    public void startAll() {
        endpoints.values().forEach(e -> {
            if (!e.isRunning() && !e.isShutdown()) {
                e.start();
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down {} Solace replier(s)...", endpoints.size());
        endpoints.values().forEach(SolaceReplierEndpoint::shutdown);
        endpoints.clear();
    }
}
