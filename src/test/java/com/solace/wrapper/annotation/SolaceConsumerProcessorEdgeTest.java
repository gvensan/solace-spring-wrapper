package com.solace.wrapper.annotation;

import com.solace.wrapper.annotation.processor.SolaceConsumerProcessor;
import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge-case coverage for {@link SolaceConsumerProcessor}: method validation (static / no-param /
 * non-void), unmappable-parameter handling, and rejection of dangerous SpEL conditions.
 */
class SolaceConsumerProcessorEdgeTest {

    static class RecordingManager extends SolaceConsumerManager {
        final List<String> queues = new ArrayList<>();
        final AtomicReference<SolaceMessageHandler<?>> lastHandler = new AtomicReference<>();
        RecordingManager() { super(null, null); }

        @Override
        public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                                SolaceConsumer.MessagingMode messagingMode, boolean autoCreateQueue,
                                                SolaceConsumer.AckMode ackMode, Class<?> messageType,
                                                SolaceMessageHandler<?> messageHandler, String clientName,
                                                int a, long b, double c, long d, boolean autoStart) {
            queues.add(queueName);
            lastHandler.set(messageHandler);
            return consumerId;
        }
    }

    private RecordingManager manager;
    private SolaceConsumerProcessor processor;

    @BeforeEach
    void setup() throws Exception {
        manager = new RecordingManager();
        processor = new SolaceConsumerProcessor();
        Field f = SolaceConsumerProcessor.class.getDeclaredField("consumerManager");
        f.setAccessible(true);
        f.set(processor, manager);
        processor.setSpelResolver(new SpelExpressionResolver());
    }

    static class InvalidMethods {
        @SolaceConsumer(queue = "q.static")
        public static void staticMethod(String body) { }   // static -> skipped

        @SolaceConsumer(queue = "q.noargs")
        public void noArgs() { }                            // no parameters -> skipped

        @SolaceConsumer(queue = "q.nonvoid")
        public String nonVoid(String body) { return body; } // non-void -> still registered
    }

    @Test
    void static_and_no_arg_methods_are_skipped_nonvoid_registered() {
        processor.postProcessAfterInitialization(new InvalidMethods(), "im");
        assertThat(manager.queues).contains("q.nonvoid")
                .doesNotContain("q.static", "q.noargs");
    }

    static class UnmappableBean {
        final AtomicReference<Object> extra = new AtomicReference<>("sentinel");
        @SolaceConsumer(queue = "q.unmappable")
        public void handle(String body, Long unmappable) { extra.set(unmappable); }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void unmappable_parameter_is_passed_as_null() {
        UnmappableBean bean = new UnmappableBean();
        processor.postProcessAfterInitialization(bean, "ub");
        assertThat(manager.queues).contains("q.unmappable");

        // Invoke the generated handler with a String message; the Long parameter can't be mapped -> null.
        ((SolaceMessageHandler) manager.lastHandler.get()).handleMessage("payload", dummyInbound());
        assertThat(bean.extra.get()).isNull();
    }

    static class DangerousConditionBean {
        @SolaceConsumer(queue = "q.danger", condition = "T(java.lang.Runtime).getRuntime().exec('x') != null")
        public void handle(String body) { }
    }

    @Test
    void dangerous_spel_condition_prevents_registration() {
        processor.postProcessAfterInitialization(new DangerousConditionBean(), "db");
        // The SpEL security validation rejects the condition, so no consumer is registered.
        assertThat(manager.queues).doesNotContain("q.danger");
    }

    private static com.solace.messaging.receiver.InboundMessage dummyInbound() {
        return (com.solace.messaging.receiver.InboundMessage) Proxy.newProxyInstance(
                SolaceConsumerProcessorEdgeTest.class.getClassLoader(),
                new Class<?>[]{com.solace.messaging.receiver.InboundMessage.class}, (p, m, a) -> null);
    }
}
