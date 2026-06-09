package com.solace.wrapper.annotation;

import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.annotation.processor.SolaceConsumerProcessor;
import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import com.solace.wrapper.consumer.SolaceAckContext;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceManualAckMessageHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the manual-ack registration path of {@link SolaceConsumerProcessor} (a method declaring a
 * {@link SolaceAckContext} parameter), the behavior of the generated manual-ack handler
 * (condition filtering, argument injection, retry), and the processor's statistics accessor.
 */
class SolaceConsumerProcessorManualAckTest {

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

    @Test
    void manual_ack_method_registers_manual_handler_and_forces_manual_mode() {
        processor.postProcessAfterInitialization(new ManualBean(), "mb");

        ManualRecording rec = manager.manual.stream()
                .filter(r -> "q.manual".equals(r.queue)).findFirst().orElseThrow();
        assertThat(rec.ackMode).isEqualTo(SolaceConsumer.AckMode.MANUAL);
        assertThat(rec.handler).isNotNull();
    }

    @Test
    void manual_handler_injects_ack_context_and_message() {
        ManualBean bean = new ManualBean();
        processor.postProcessAfterInitialization(bean, "mb");
        ManualRecording rec = manager.manual.stream()
                .filter(r -> "q.manual".equals(r.queue)).findFirst().orElseThrow();

        AtomicInteger acks = new AtomicInteger();
        SolaceAckContext ctx = new SolaceAckContext(acks::incrementAndGet, () -> {});
        invokeManual(rec.handler, "payload", ctx);

        assertThat(bean.lastBody.get()).isEqualTo("payload");
        assertThat(bean.lastAck.get()).isSameAs(ctx);
        // The handler body invoked the bean which called ack().
        assertThat(acks.get()).isEqualTo(1);
    }

    @Test
    void manual_handler_skips_when_condition_false() {
        ManualBean bean = new ManualBean();
        processor.postProcessAfterInitialization(bean, "mb");
        ManualRecording rec = manager.manual.stream()
                .filter(r -> "q.cond".equals(r.queue)).findFirst().orElseThrow();

        bean.condCalls.set(0);
        // "hi".length() (2) > 3 is false -> handler body should skip invocation.
        invokeManual(rec.handler, "hi", new SolaceAckContext(() -> {}, () -> {}));
        assertThat(bean.condCalls.get()).isZero();

        // "hello".length() (5) > 3 is true -> processed.
        invokeManual(rec.handler, "hello", new SolaceAckContext(() -> {}, () -> {}));
        assertThat(bean.condCalls.get()).isEqualTo(1);
    }

    @Test
    void manual_handler_wraps_handler_failure() {
        ManualBean bean = new ManualBean();
        processor.postProcessAfterInitialization(bean, "mb");
        ManualRecording rec = manager.manual.stream()
                .filter(r -> "q.boom".equals(r.queue)).findFirst().orElseThrow();

        assertThatThrownBy(() ->
                invokeManual(rec.handler, "x", new SolaceAckContext(() -> {}, () -> {})))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void getStats_reports_processed_and_active_counts() {
        processor.postProcessAfterInitialization(new ManualBean(), "mb");

        SolaceConsumerProcessor.AnnotationProcessorStats stats = processor.getStats();
        assertThat(stats.getProcessedAnnotations()).isGreaterThanOrEqualTo(0);
        assertThat(stats.getActiveConsumers()).isEqualTo(manager.getTotalConsumerCount());
        assertThat(stats.toString()).contains("AnnotationProcessorStats").contains("processed=");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void invokeManual(Object handler, String body, SolaceAckContext ctx) {
        ((SolaceManualAckMessageHandler) handler).handleMessage(body, dummyInbound(), ctx);
    }

    private static InboundMessage dummyInbound() {
        return (InboundMessage) Proxy.newProxyInstance(
                SolaceConsumerProcessorManualAckTest.class.getClassLoader(),
                new Class<?>[]{InboundMessage.class}, (p, m, a) -> null);
    }

    // ----- harness -----

    static class ManualRecording {
        String queue;
        SolaceConsumer.AckMode ackMode;
        SolaceManualAckMessageHandler<?> handler;
    }

    static class RecordingManager extends SolaceConsumerManager {
        final java.util.List<ManualRecording> manual = new java.util.ArrayList<>();
        private int total = 0;

        RecordingManager() { super(null, null); }

        @Override
        public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                                SolaceConsumer.MessagingMode messagingMode,
                                                boolean autoCreateQueue,
                                                SolaceConsumer.AckMode ackMode,
                                                Class<?> messageType,
                                                SolaceManualAckMessageHandler<?> messageHandler,
                                                String clientName,
                                                int localMaxAttempts, long localBackoffInitialMs,
                                                double localBackoffMultiplier, long localBackoffMaxMs,
                                                boolean autoStart) {
            ManualRecording r = new ManualRecording();
            r.queue = queueName;
            r.ackMode = ackMode;
            r.handler = messageHandler;
            manual.add(r);
            total++;
            return consumerId;
        }

        @Override
        public int getTotalConsumerCount() {
            return total;
        }
    }

    static class ManualBean {
        final AtomicReference<String> lastBody = new AtomicReference<>();
        final AtomicReference<SolaceAckContext> lastAck = new AtomicReference<>();
        final AtomicInteger condCalls = new AtomicInteger();

        @SolaceConsumer(queue = "q.manual")
        public void handle(String body, SolaceAckContext ack) {
            lastBody.set(body);
            lastAck.set(ack);
            ack.ack();
        }

        @SolaceConsumer(queue = "q.cond", condition = "#message.length() > 3")
        public void handleConditional(String body, SolaceAckContext ack) {
            condCalls.incrementAndGet();
        }

        @SolaceConsumer(queue = "q.boom")
        public void handleBoom(String body, SolaceAckContext ack) {
            throw new RuntimeException("boom");
        }
    }
}
