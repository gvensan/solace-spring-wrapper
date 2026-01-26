package com.solace.wrapper.consumer;

import com.solace.messaging.MessagingService;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive unit tests for SolaceConsumer covering:
 * - Mode detection logic
 * - Validation errors
 * - Backoff calculation
 * - Lifecycle management (start/stop/shutdown)
 * - Manual ack context behavior
 * - Getters and configuration
 */
public class SolaceConsumerComprehensiveTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceConsumerComprehensiveTest.class);

    static volatile Env CURRENT;

    static class Env {
        volatile PersistentMessageReceiver persistentReceiver;
        volatile DirectMessageReceiver directReceiver;
        final AtomicInteger ackCount = new AtomicInteger();
        final AtomicInteger settleCount = new AtomicInteger();
        final AtomicInteger directStart = new AtomicInteger();
        final AtomicInteger persistentStart = new AtomicInteger();
        final AtomicInteger directTerminate = new AtomicInteger();
        final AtomicInteger persistentTerminate = new AtomicInteger();
        volatile boolean failOnStart = false;
        volatile boolean serviceDisconnected = false;

        MessagingService service() {
            return (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return !serviceDisconnected;
                            case "createPersistentMessageReceiverBuilder":
                                return createPersistentBuilder();
                            case "createDirectMessageReceiverBuilder":
                                return createDirectBuilder();
                        }
                        if (m.getReturnType().equals(boolean.class)) return false;
                        if (m.getReturnType().equals(int.class)) return 0;
                        return null;
                    });
        }

        private Object createPersistentBuilder() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{com.solace.messaging.PersistentMessageReceiverBuilder.class}, (bp, bm, ba) -> {
                        String n = bm.getName();
                        if ("withMissingResourcesCreationStrategy".equals(n) ||
                            "withMessageClientAcknowledgement".equals(n) ||
                            "withMessageAutoAcknowledgement".equals(n) ||
                            "withRequiredMessageClientOutcomeOperationSupport".equals(n) ||
                            "withSubscriptions".equals(n)) {
                            return bp; // fluent
                        }
                        if ("build".equals(n)) {
                            if (failOnStart) throw new RuntimeException("simulated build failure");
                            Env.this.persistentReceiver = (PersistentMessageReceiver) Proxy.newProxyInstance(
                                    getClass().getClassLoader(), new Class[]{PersistentMessageReceiver.class}, (rp, rm, ra) -> {
                                        switch (rm.getName()) {
                                            case "start": persistentStart.incrementAndGet(); return null;
                                            case "ack": ackCount.incrementAndGet(); return null;
                                            case "settle": settleCount.incrementAndGet(); return null;
                                            case "terminate": persistentTerminate.incrementAndGet(); return null;
                                            case "receiveAsync": return null;
                                            case "setReceiveFailureListener": return null;
                                            case "isRunning": return persistentStart.get() > persistentTerminate.get();
                                            default: return null;
                                        }
                                    });
                            return Env.this.persistentReceiver;
                        }
                        return null;
                    });
        }

        private Object createDirectBuilder() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{com.solace.messaging.DirectMessageReceiverBuilder.class}, (bp, bm, ba) -> {
                        String n = bm.getName();
                        if ("withSubscriptions".equals(n)) return bp;
                        if ("build".equals(n)) {
                            if (failOnStart) throw new RuntimeException("simulated build failure");
                            Env.this.directReceiver = (DirectMessageReceiver) Proxy.newProxyInstance(
                                    getClass().getClassLoader(), new Class[]{DirectMessageReceiver.class}, (rp, rm, ra) -> {
                                        switch (rm.getName()) {
                                            case "start": directStart.incrementAndGet(); return null;
                                            case "receiveAsync": return null;
                                            case "terminate": directTerminate.incrementAndGet(); return null;
                                            case "isRunning": return directStart.get() > directTerminate.get();
                                            default: return null;
                                        }
                                    });
                            return Env.this.directReceiver;
                        }
                        return null;
                    });
        }
    }

    static class NoopCM extends SolaceConnectionManager {
        NoopCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
        @Override public MessagingService createMessagingService() { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String consumerId) { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String consumerId, String clientName) { return CURRENT.service(); }
        @Override public void removeConsumerService(String consumerId) { /* no-op */ }
    }

    static class StubSerializer implements MessageSerializer {
        final Object deserialized;
        StubSerializer(Object d) { this.deserialized = d; }
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService messagingService, Object object) { return null; }
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService messagingService, Object object, Object destination) { return null; }
        @Override public byte[] serializeToBytes(MessagingService messagingService, Object object) {
            if (object instanceof byte[]) return (byte[]) object;
            return object != null ? object.toString().getBytes(StandardCharsets.UTF_8) : new byte[0];
        }
        @SuppressWarnings("unchecked")
        @Override public <T> T deserialize(InboundMessage message, Class<T> targetType) { return (T) deserialized; }
        @Override public String deserializeToString(InboundMessage message) { return null; }
    }

    private Env env;
    private SolaceConnectionManager cm;

    @BeforeEach
    void init() {
        env = new Env();
        CURRENT = env;
        var props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555");
        props.setMsgVpn("default");
        props.setClientUsername("default");
        props.setClientPassword("");
        cm = new NoopCM(props);
    }

    @AfterEach
    void cleanup() {
        CURRENT = null;
    }

    private static InboundMessage inbound(String s) {
        return (InboundMessage) Proxy.newProxyInstance(
                SolaceConsumerComprehensiveTest.class.getClassLoader(), new Class[]{InboundMessage.class}, (p, m, a) -> {
                    if ("getPayloadAsString".equals(m.getName())) return s;
                    if ("getPayloadAsBytes".equals(m.getName())) return s.getBytes(StandardCharsets.UTF_8);
                    return null;
                });
    }

    // ─────────────────────────────────────────────────────────────────
    // MODE DETECTION TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void mode_detection_queue_only_is_persistent() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: mode_detection_queue_only_is_persistent\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "myQueue", new String[0], null, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        assertThat(cons.getMessagingMode()).isEqualTo(SolaceConsumer.MessagingMode.PERSISTENT);
        assertThat(cons.getQueueName()).isEqualTo("myQueue");
        log.info("Queue-only configuration correctly detected as PERSISTENT mode");
    }

    @Test
    void mode_detection_topics_only_is_direct() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: mode_detection_topics_only_is_direct\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "", new String[]{"topic/a", "topic/b"}, null, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        assertThat(cons.getMessagingMode()).isEqualTo(SolaceConsumer.MessagingMode.DIRECT);
        assertThat(cons.getTopics()).containsExactly("topic/a", "topic/b");
        log.info("Topics-only configuration correctly detected as DIRECT mode");
    }

    @Test
    void mode_detection_queue_with_topics_is_persistent() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: mode_detection_queue_with_topics_is_persistent\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "myQueue", new String[]{"topic/a"}, null, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        // Queue + topics defaults to persistent with topic subscriptions
        assertThat(cons.getMessagingMode()).isEqualTo(SolaceConsumer.MessagingMode.PERSISTENT);
        log.info("Queue+topics configuration correctly detected as PERSISTENT mode");
    }

    @Test
    void mode_detection_explicit_direct_overrides() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: mode_detection_explicit_direct_overrides\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "", new String[]{"topic/a"}, SolaceConsumer.MessagingMode.DIRECT, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        assertThat(cons.getMessagingMode()).isEqualTo(SolaceConsumer.MessagingMode.DIRECT);
        log.info("Explicit DIRECT mode preserved");
    }

    // ─────────────────────────────────────────────────────────────────
    // VALIDATION TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void validation_fails_when_no_queue_or_topics() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validation_fails_when_no_queue_or_topics\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};

        assertThatThrownBy(() -> new SolaceConsumer<>(
                "c1", "", new String[0], null, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        )).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Either queue or topics must be specified");

        log.info("Correctly rejected configuration with no queue or topics");
    }

    @Test
    void validation_fails_when_persistent_without_queue() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validation_fails_when_persistent_without_queue\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};

        assertThatThrownBy(() -> new SolaceConsumer<>(
                "c1", "", new String[]{"topic/a"}, SolaceConsumer.MessagingMode.PERSISTENT, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        )).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Queue name is required for persistent messaging mode");

        log.info("Correctly rejected PERSISTENT mode without queue");
    }

    @Test
    void validation_fails_when_direct_without_topics() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validation_fails_when_direct_without_topics\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};

        assertThatThrownBy(() -> new SolaceConsumer<>(
                "c1", "myQueue", new String[0], SolaceConsumer.MessagingMode.DIRECT, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        )).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Topics are required for direct messaging mode");

        log.info("Correctly rejected DIRECT mode without topics");
    }

    @Test
    void validation_fails_when_consumer_id_empty() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validation_fails_when_consumer_id_empty\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};

        assertThatThrownBy(() -> new SolaceConsumer<>(
                "", "myQueue", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        )).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Consumer ID cannot be null or empty");

        log.info("Correctly rejected empty consumer ID");
    }

    @Test
    void validation_fails_when_no_handler() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: validation_fails_when_no_handler\n" +
                "───────────────────────────────────────────────────────────────");

        assertThatThrownBy(() -> new SolaceConsumer<>(
                "c1", "myQueue", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, false,
                SolaceConsumer.AckMode.AUTO, String.class, (SolaceMessageHandler<String>) null, cm, new StubSerializer("x")
        )).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Message handler cannot be null");

        log.info("Correctly rejected null handler");
    }

    // ─────────────────────────────────────────────────────────────────
    // LIFECYCLE TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void start_stop_lifecycle_persistent() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: start_stop_lifecycle_persistent\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        assertThat(cons.isRunning()).isFalse();
        assertThat(cons.isShutdown()).isFalse();

        cons.start();
        assertThat(cons.isRunning()).isTrue();
        assertThat(env.persistentStart.get()).isEqualTo(1);

        cons.stop();
        assertThat(cons.isRunning()).isFalse();
        assertThat(env.persistentTerminate.get()).isEqualTo(1);

        // Should be able to restart after stop (not shutdown)
        cons.start();
        assertThat(cons.isRunning()).isTrue();
        assertThat(env.persistentStart.get()).isEqualTo(2);

        log.info("Persistent consumer lifecycle (start/stop/restart) works correctly");
    }

    @Test
    void start_stop_lifecycle_direct() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: start_stop_lifecycle_direct\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "", new String[]{"topic/>"}, SolaceConsumer.MessagingMode.DIRECT, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        cons.start();
        assertThat(cons.isRunning()).isTrue();
        assertThat(env.directStart.get()).isEqualTo(1);

        cons.stop();
        assertThat(cons.isRunning()).isFalse();
        assertThat(env.directTerminate.get()).isEqualTo(1);

        log.info("Direct consumer lifecycle (start/stop) works correctly");
    }

    @Test
    void shutdown_prevents_restart() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: shutdown_prevents_restart\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        cons.start();
        assertThat(cons.isRunning()).isTrue();

        cons.shutdown();
        assertThat(cons.isRunning()).isFalse();
        assertThat(cons.isShutdown()).isTrue();

        // Attempt to restart after shutdown should be ignored
        cons.start();
        assertThat(cons.isRunning()).isFalse();
        assertThat(env.persistentStart.get()).isEqualTo(1); // Only started once

        log.info("Shutdown correctly prevents restart");
    }

    @Test
    void double_start_is_idempotent() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: double_start_is_idempotent\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        cons.start();
        cons.start(); // Second start should be no-op

        assertThat(env.persistentStart.get()).isEqualTo(1);
        log.info("Double start is correctly idempotent");
    }

    @Test
    void double_stop_is_idempotent() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: double_stop_is_idempotent\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        cons.start();
        cons.stop();
        cons.stop(); // Second stop should be no-op

        assertThat(env.persistentTerminate.get()).isEqualTo(1);
        log.info("Double stop is correctly idempotent");
    }

    // ─────────────────────────────────────────────────────────────────
    // BACKOFF CONFIGURATION TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void withLocalBackoff_configures_retry_parameters() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: withLocalBackoff_configures_retry_parameters\n" +
                "───────────────────────────────────────────────────────────────");

        AtomicInteger attempts = new AtomicInteger();
        SolaceMessageHandler<String> handler = (msg, in) -> {
            if (attempts.incrementAndGet() < 3) throw new RuntimeException("fail");
        };

        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer("x")
        ).withLocalBackoff(3, 10, 2.0, 100);

        cons.start();
        cons.onMessage(inbound("test"));

        assertThat(attempts.get()).isEqualTo(3); // 2 failures + 1 success
        assertThat(env.ackCount.get()).isEqualTo(1);
        assertThat(env.settleCount.get()).isEqualTo(0);

        log.info("Local backoff retry worked: {} attempts, ack sent on success", attempts.get());
    }

    @Test
    void withLocalBackoff_normalizes_invalid_values() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: withLocalBackoff_normalizes_invalid_values\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        ).withLocalBackoff(-5, -100, -1.0, -500); // All invalid values

        // Consumer should still be valid (values normalized internally)
        cons.start();
        assertThat(cons.isRunning()).isTrue();

        log.info("Invalid backoff values were normalized without error");
    }

    @Test
    void withTerminationTimeout_configures_timeout() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: withTerminationTimeout_configures_timeout\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        ).withTerminationTimeout(10000);

        // Verify it was accepted (no error)
        cons.start();
        cons.stop();

        log.info("Termination timeout configured successfully");
    }

    // ─────────────────────────────────────────────────────────────────
    // MANUAL ACK HANDLER TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void manual_ack_handler_receives_context() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: manual_ack_handler_receives_context\n" +
                "───────────────────────────────────────────────────────────────");

        AtomicInteger handled = new AtomicInteger();
        SolaceManualAckMessageHandler<String> handler = (msg, in, ctx) -> {
            handled.incrementAndGet();
            assertThat(ctx).isNotNull();
            assertThat(ctx.getStatus()).isEqualTo(SolaceAckContext.Status.NONE);
            ctx.ack();
            assertThat(ctx.getStatus()).isEqualTo(SolaceAckContext.Status.ACKED);
        };

        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer("x"), null
        );
        cons.start();
        cons.onMessage(inbound("test"));

        assertThat(handled.get()).isEqualTo(1);
        assertThat(env.ackCount.get()).isEqualTo(1);

        log.info("Manual ack handler received context and acked successfully");
    }

    @Test
    void manual_ack_handler_can_fail_message() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: manual_ack_handler_can_fail_message\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceManualAckMessageHandler<String> handler = (msg, in, ctx) -> {
            ctx.fail(); // Explicitly fail the message
        };

        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer("x"), null
        );
        cons.start();
        cons.onMessage(inbound("test"));

        assertThat(env.ackCount.get()).isEqualTo(0);
        assertThat(env.settleCount.get()).isEqualTo(1);

        log.info("Manual ack handler can explicitly fail messages");
    }

    // ─────────────────────────────────────────────────────────────────
    // GETTER TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void getters_return_correct_values() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getters_return_correct_values\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "test-consumer", "test-queue", new String[]{"topic/a", "topic/b"},
                SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer("x")
        );

        assertThat(cons.getConsumerId()).isEqualTo("test-consumer");
        assertThat(cons.getQueueName()).isEqualTo("test-queue");
        assertThat(cons.getTopics()).containsExactly("topic/a", "topic/b");
        assertThat(cons.getMessagingMode()).isEqualTo(SolaceConsumer.MessagingMode.PERSISTENT);
        assertThat(cons.isAutoCreateQueue()).isTrue();
        assertThat(cons.getMessageType()).isEqualTo(String.class);
        assertThat(cons.getDescription()).contains("test-consumer", "PERSISTENT", "test-queue");

        log.info("All getters return correct values");
    }

    @Test
    void getTopics_returns_defensive_copy() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getTopics_returns_defensive_copy\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "", new String[]{"topic/a"}, SolaceConsumer.MessagingMode.DIRECT, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        String[] topics = cons.getTopics();
        topics[0] = "modified";

        // Original should be unchanged
        assertThat(cons.getTopics()[0]).isEqualTo("topic/a");

        log.info("getTopics() returns defensive copy");
    }

    // ─────────────────────────────────────────────────────────────────
    // ERROR HANDLING TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void start_throws_when_service_disconnected() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: start_throws_when_service_disconnected\n" +
                "───────────────────────────────────────────────────────────────");

        env.serviceDisconnected = true;

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        assertThatThrownBy(cons::start)
            .isInstanceOf(com.solace.wrapper.exception.SolaceConsumerException.class)
            .hasMessageContaining("Failed to start consumer")
            .hasCauseInstanceOf(com.solace.wrapper.exception.SolaceConsumerException.class);

        log.info("Start correctly throws when service is disconnected");
    }

    @Test
    void onMessage_ignored_when_not_running() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: onMessage_ignored_when_not_running\n" +
                "───────────────────────────────────────────────────────────────");

        AtomicInteger handled = new AtomicInteger();
        SolaceMessageHandler<String> handler = (msg, in) -> handled.incrementAndGet();

        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        // Don't start - just call onMessage directly
        cons.onMessage(inbound("test"));

        assertThat(handled.get()).isEqualTo(0); // Message ignored

        log.info("Messages correctly ignored when consumer not running");
    }

    @Test
    void onMessage_ignored_when_shutdown() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: onMessage_ignored_when_shutdown\n" +
                "───────────────────────────────────────────────────────────────");

        AtomicInteger handled = new AtomicInteger();
        SolaceMessageHandler<String> handler = (msg, in) -> handled.incrementAndGet();

        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );

        cons.start();
        cons.shutdown();
        cons.onMessage(inbound("test"));

        assertThat(handled.get()).isEqualTo(0); // Message ignored after shutdown

        log.info("Messages correctly ignored after shutdown");
    }
}
