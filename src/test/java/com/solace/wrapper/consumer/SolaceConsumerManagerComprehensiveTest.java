package com.solace.wrapper.consumer;

import com.solace.messaging.MessagingService;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.config.SolaceProperties;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.exception.SolaceConsumerException;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive unit tests for SolaceConsumerManager covering:
 * - Consumer creation and lifecycle
 * - Duplicate consumer detection
 * - Consumer queries (by queue, by type)
 * - Stop/start/restart operations
 * - Shutdown behavior
 * - Status tracking
 */
public class SolaceConsumerManagerComprehensiveTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceConsumerManagerComprehensiveTest.class);

    private SolaceConsumerManager manager;
    private TestConnectionManager testCM;
    private TestSerializer testSerializer;

    static class TestConnectionManager extends SolaceConnectionManager {
        final AtomicInteger serviceCreations = new AtomicInteger();

        TestConnectionManager(SolaceProperties props) {
            super(props);
        }

        @Override
        protected void initializePrimaryService() {
            // Skip actual initialization in tests
        }

        @Override
        public MessagingService createMessagingService() {
            return createStubService();
        }

        @Override
        public MessagingService createConsumerService(String consumerId) {
            return createStubService();
        }

        @Override
        public MessagingService createConsumerService(String consumerId, String clientName) {
            return createStubService();
        }

        @Override
        public void removeConsumerService(String consumerId) {
            // no-op for test
        }

        private MessagingService createStubService() {
            serviceCreations.incrementAndGet();
            return (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return true;
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
                        if (n.startsWith("with") || n.equals("build")) {
                            if ("build".equals(n)) {
                                return createPersistentReceiver();
                            }
                            return bp; // fluent
                        }
                        return null;
                    });
        }

        private Object createPersistentReceiver() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{com.solace.messaging.receiver.PersistentMessageReceiver.class}, (rp, rm, ra) -> {
                        switch (rm.getName()) {
                            case "start":
                            case "ack":
                            case "settle":
                            case "terminate":
                            case "receiveAsync":
                            case "setReceiveFailureListener":
                                return null;
                            case "isRunning":
                                return true;
                        }
                        return null;
                    });
        }

        private Object createDirectBuilder() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{com.solace.messaging.DirectMessageReceiverBuilder.class}, (bp, bm, ba) -> {
                        String n = bm.getName();
                        if (n.startsWith("with") || n.equals("build")) {
                            if ("build".equals(n)) {
                                return createDirectReceiver();
                            }
                            return bp;
                        }
                        return null;
                    });
        }

        private Object createDirectReceiver() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{com.solace.messaging.receiver.DirectMessageReceiver.class}, (rp, rm, ra) -> {
                        switch (rm.getName()) {
                            case "start":
                            case "receiveAsync":
                            case "terminate":
                                return null;
                            case "isRunning":
                                return true;
                        }
                        return null;
                    });
        }
    }

    static class TestSerializer implements MessageSerializer {
        @Override
        public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService svc, Object obj) {
            return null;
        }

        @Override
        public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService svc, Object obj, Object dest) {
            return null;
        }

        @Override
        public byte[] serializeToBytes(MessagingService svc, Object obj) {
            return obj != null ? obj.toString().getBytes(StandardCharsets.UTF_8) : new byte[0];
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T deserialize(InboundMessage msg, Class<T> type) {
            return (T) "test-message";
        }

        @Override
        public String deserializeToString(InboundMessage msg) {
            return "test";
        }
    }

    @BeforeEach
    void setup() {
        SolaceProperties props = new SolaceProperties();
        props.setHost("tcp://noop:55555");
        props.setMsgVpn("default");
        props.setClientUsername("default");
        props.setClientPassword("");
        testCM = new TestConnectionManager(props);
        testSerializer = new TestSerializer();
        manager = new SolaceConsumerManager(testCM, testSerializer);
    }

    @AfterEach
    void cleanup() {
        manager.shutdown();
    }

    // ─────────────────────────────────────────────────────────────────
    // CONSUMER CREATION TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void createConsumer_creates_and_starts_consumer() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumer_creates_and_starts_consumer\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        String consumerId = manager.createConsumer("test-queue", String.class, handler);

        assertThat(consumerId).isNotNull();
        assertThat(manager.hasConsumer(consumerId)).isTrue();
        assertThat(manager.isConsumerRunning(consumerId)).isTrue();
        assertThat(manager.getTotalConsumerCount()).isEqualTo(1);
        assertThat(manager.getActiveConsumerCount()).isEqualTo(1);

        log.info("Consumer created with ID: {}", consumerId);
    }

    @Test
    void createConsumer_with_explicit_id_uses_provided_id() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumer_with_explicit_id_uses_provided_id\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        String consumerId = manager.createConsumer("my-consumer-id", "test-queue", String.class, handler);

        assertThat(consumerId).isEqualTo("my-consumer-id");
        assertThat(manager.hasConsumer("my-consumer-id")).isTrue();

        log.info("Consumer created with explicit ID: {}", consumerId);
    }

    @Test
    void createConsumer_throws_on_duplicate_id() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumer_throws_on_duplicate_id\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("dup-id", "queue1", String.class, handler);

        assertThatThrownBy(() -> manager.createConsumer("dup-id", "queue2", String.class, handler))
            .isInstanceOf(SolaceConsumerException.class)
            .hasMessageContaining("already exists");

        log.info("Duplicate ID correctly rejected");
    }

    @Test
    void createConsumerRaw_works_with_raw_types() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumerRaw_works_with_raw_types\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<?> handler = (msg, in) -> {};
        String consumerId = manager.createConsumerRaw("raw-consumer", "test-queue", String.class, handler);

        assertThat(consumerId).isEqualTo("raw-consumer");
        assertThat(manager.hasConsumer(consumerId)).isTrue();

        log.info("Raw consumer created successfully");
    }

    @Test
    void createEnhancedConsumerRaw_supports_direct_mode() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createEnhancedConsumerRaw_supports_direct_mode\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<?> handler = (msg, in) -> {};
        String consumerId = manager.createEnhancedConsumerRaw(
                "direct-consumer", "", new String[]{"topic/a", "topic/b"},
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.DIRECT,
                false,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO,
                String.class, handler
        );

        assertThat(consumerId).isEqualTo("direct-consumer");
        SolaceConsumerManager.ConsumerStatus status = manager.getConsumerStatus(consumerId);
        assertThat(status.getMessagingMode()).isEqualTo("DIRECT");

        log.info("Direct mode consumer created successfully");
    }

    @Test
    void createEnhancedConsumerRaw_supports_manual_ack_handler() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createEnhancedConsumerRaw_supports_manual_ack_handler\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceManualAckMessageHandler<?> handler = (msg, in, ctx) -> ctx.ack();
        String consumerId = manager.createEnhancedConsumerRaw(
                "manual-consumer", "test-queue", new String[0],
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT,
                true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.MANUAL,
                String.class, handler
        );

        assertThat(consumerId).isEqualTo("manual-consumer");
        assertThat(manager.isConsumerRunning(consumerId)).isTrue();

        log.info("Manual ack consumer created successfully");
    }

    @Test
    void createEnhancedConsumerRaw_with_autoStart_false() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createEnhancedConsumerRaw_with_autoStart_false\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<?> handler = (msg, in) -> {};
        String consumerId = manager.createEnhancedConsumerRaw(
                "no-auto-start", "test-queue", new String[0],
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT,
                true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO,
                String.class, handler, null,
                1, 200, 2.0, 2000, false // autoStart = false
        );

        assertThat(manager.hasConsumer(consumerId)).isTrue();
        assertThat(manager.isConsumerRunning(consumerId)).isFalse(); // Not started

        log.info("Consumer registered but not started (autoStart=false)");
    }

    // ─────────────────────────────────────────────────────────────────
    // CONSUMER LIFECYCLE TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void stopConsumer_stops_running_consumer() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: stopConsumer_stops_running_consumer\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        String consumerId = manager.createConsumer("stop-test", "queue", String.class, handler);

        assertThat(manager.isConsumerRunning(consumerId)).isTrue();

        manager.stopConsumer(consumerId);
        assertThat(manager.isConsumerRunning(consumerId)).isFalse();
        assertThat(manager.hasConsumer(consumerId)).isTrue(); // Still registered

        log.info("Consumer stopped but still registered");
    }

    @Test
    void stopConsumer_handles_unknown_consumer() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: stopConsumer_handles_unknown_consumer\n" +
                "───────────────────────────────────────────────────────────────");

        // Should not throw
        manager.stopConsumer("nonexistent");

        log.info("Unknown consumer handled gracefully");
    }

    @Test
    void removeConsumer_removes_and_shuts_down() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: removeConsumer_removes_and_shuts_down\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        String consumerId = manager.createConsumer("remove-test", "queue", String.class, handler);

        manager.removeConsumer(consumerId);

        assertThat(manager.hasConsumer(consumerId)).isFalse();
        assertThat(manager.getTotalConsumerCount()).isEqualTo(0);

        log.info("Consumer removed and shut down");
    }

    @Test
    void restartConsumer_stops_and_starts() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: restartConsumer_stops_and_starts\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        String consumerId = manager.createConsumer("restart-test", "queue", String.class, handler);

        manager.restartConsumer(consumerId);

        assertThat(manager.isConsumerRunning(consumerId)).isTrue();

        log.info("Consumer restarted successfully");
    }

    @Test
    void stopAllConsumers_stops_all() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: stopAllConsumers_stops_all\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("c1", "q1", String.class, handler);
        manager.createConsumer("c2", "q2", String.class, handler);
        manager.createConsumer("c3", "q3", String.class, handler);

        assertThat(manager.getActiveConsumerCount()).isEqualTo(3);

        manager.stopAllConsumers();

        assertThat(manager.getActiveConsumerCount()).isEqualTo(0);
        assertThat(manager.getTotalConsumerCount()).isEqualTo(3); // Still registered

        log.info("All consumers stopped");
    }

    @Test
    void startAllConsumers_starts_stopped_consumers() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: startAllConsumers_starts_stopped_consumers\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("c1", "q1", String.class, handler);
        manager.createConsumer("c2", "q2", String.class, handler);

        manager.stopAllConsumers();
        assertThat(manager.getActiveConsumerCount()).isEqualTo(0);

        manager.startAllConsumers();
        assertThat(manager.getActiveConsumerCount()).isEqualTo(2);

        log.info("All consumers started");
    }

    @Test
    void shutdown_clears_all_consumers() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: shutdown_clears_all_consumers\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("c1", "q1", String.class, handler);
        manager.createConsumer("c2", "q2", String.class, handler);

        manager.shutdown();

        assertThat(manager.getTotalConsumerCount()).isEqualTo(0);

        log.info("Shutdown cleared all consumers");
    }

    // ─────────────────────────────────────────────────────────────────
    // CONSUMER QUERY TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void getConsumerIds_returns_all_ids() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getConsumerIds_returns_all_ids\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("alpha", "q1", String.class, handler);
        manager.createConsumer("beta", "q2", String.class, handler);
        manager.createConsumer("gamma", "q3", String.class, handler);

        Set<String> ids = manager.getConsumerIds();

        assertThat(ids).containsExactlyInAnyOrder("alpha", "beta", "gamma");

        log.info("All consumer IDs retrieved: {}", ids);
    }

    @Test
    void getConsumersByQueue_finds_matching_consumers() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getConsumersByQueue_finds_matching_consumers\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("c1", "shared-queue", String.class, handler);
        manager.createConsumer("c2", "shared-queue", String.class, handler);
        manager.createConsumer("c3", "other-queue", String.class, handler);

        List<String> sharedQueueConsumers = manager.getConsumersByQueue("shared-queue");

        assertThat(sharedQueueConsumers).containsExactlyInAnyOrder("c1", "c2");

        log.info("Found consumers by queue: {}", sharedQueueConsumers);
    }

    @Test
    void getConsumersByMessageType_finds_matching_consumers() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getConsumersByMessageType_finds_matching_consumers\n" +
                "───────────────────────────────────────────────────────────────");

        manager.createConsumer("str1", "q1", String.class, (msg, in) -> {});
        manager.createConsumer("str2", "q2", String.class, (msg, in) -> {});
        manager.createConsumer("int1", "q3", Integer.class, (msg, in) -> {});

        List<String> stringConsumers = manager.getConsumersByMessageType(String.class);

        assertThat(stringConsumers).containsExactlyInAnyOrder("str1", "str2");

        log.info("Found consumers by type: {}", stringConsumers);
    }

    // ─────────────────────────────────────────────────────────────────
    // STATUS TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void getConsumerStatus_returns_detailed_status() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getConsumerStatus_returns_detailed_status\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("status-test", "my-queue", String.class, handler);

        SolaceConsumerManager.ConsumerStatus status = manager.getConsumerStatus("status-test");

        assertThat(status).isNotNull();
        assertThat(status.getConsumerId()).isEqualTo("status-test");
        assertThat(status.getQueueName()).isEqualTo("my-queue");
        assertThat(status.getMessageType()).isEqualTo("String");
        assertThat(status.isRunning()).isTrue();
        assertThat(status.isShutdown()).isFalse();
        assertThat(status.getDestinationDescription()).contains("my-queue");

        log.info("Status retrieved: {}", status);
    }

    @Test
    void getConsumerStatus_returns_null_for_unknown() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getConsumerStatus_returns_null_for_unknown\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceConsumerManager.ConsumerStatus status = manager.getConsumerStatus("nonexistent");

        assertThat(status).isNull();

        log.info("Null returned for unknown consumer");
    }

    @Test
    void getAllConsumerStatuses_returns_all() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getAllConsumerStatuses_returns_all\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("c1", "q1", String.class, handler);
        manager.createConsumer("c2", "q2", String.class, handler);

        Map<String, SolaceConsumerManager.ConsumerStatus> statuses = manager.getAllConsumerStatuses();

        assertThat(statuses).hasSize(2);
        assertThat(statuses).containsKeys("c1", "c2");

        log.info("All statuses retrieved: {}", statuses.keySet());
    }

    // ─────────────────────────────────────────────────────────────────
    // CONSUMER STATUS CLASS TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void consumerStatus_getTopics_returns_defensive_copy() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: consumerStatus_getTopics_returns_defensive_copy\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceConsumerManager.ConsumerStatus status = new SolaceConsumerManager.ConsumerStatus(
                "id", "queue", new String[]{"topic/a"}, "PERSISTENT", "String", true, false
        );

        String[] topics = status.getTopics();
        topics[0] = "modified";

        assertThat(status.getTopics()[0]).isEqualTo("topic/a");

        log.info("getTopics returns defensive copy");
    }

    @Test
    void consumerStatus_getDestinationDescription_formats_correctly() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: consumerStatus_getDestinationDescription_formats_correctly\n" +
                "───────────────────────────────────────────────────────────────");

        // Queue only
        SolaceConsumerManager.ConsumerStatus queueOnly = new SolaceConsumerManager.ConsumerStatus(
                "id", "my-queue", new String[0], "PERSISTENT", "String", true, false
        );
        assertThat(queueOnly.getDestinationDescription()).isEqualTo("queue=my-queue");

        // Topics only
        SolaceConsumerManager.ConsumerStatus topicsOnly = new SolaceConsumerManager.ConsumerStatus(
                "id", "", new String[]{"t1", "t2"}, "DIRECT", "String", true, false
        );
        assertThat(topicsOnly.getDestinationDescription()).isEqualTo("topics=[t1, t2]");

        // Both
        SolaceConsumerManager.ConsumerStatus both = new SolaceConsumerManager.ConsumerStatus(
                "id", "q", new String[]{"t1"}, "PERSISTENT", "String", true, false
        );
        assertThat(both.getDestinationDescription()).isEqualTo("queue=q, topics=[t1]");

        log.info("Destination descriptions formatted correctly");
    }

    // ─────────────────────────────────────────────────────────────────
    // COUNTER TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void generated_consumer_id_is_unique() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: generated_consumer_id_is_unique\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        String id1 = manager.createConsumer("queue", String.class, handler);
        String id2 = manager.createConsumer("queue", String.class, handler);
        String id3 = manager.createConsumer("queue", String.class, handler);

        assertThat(id1).isNotEqualTo(id2);
        assertThat(id2).isNotEqualTo(id3);
        assertThat(id1).contains("queue-consumer-");
        assertThat(id2).contains("queue-consumer-");
        assertThat(id3).contains("queue-consumer-");

        log.info("Generated IDs are unique: {}, {}, {}", id1, id2, id3);
    }

    @Test
    void active_and_total_counts_are_accurate() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: active_and_total_counts_are_accurate\n" +
                "───────────────────────────────────────────────────────────────");

        SolaceMessageHandler<String> handler = (msg, in) -> {};
        manager.createConsumer("c1", "q1", String.class, handler);
        manager.createConsumer("c2", "q2", String.class, handler);

        assertThat(manager.getTotalConsumerCount()).isEqualTo(2);
        assertThat(manager.getActiveConsumerCount()).isEqualTo(2);

        manager.stopConsumer("c1");

        assertThat(manager.getTotalConsumerCount()).isEqualTo(2);
        assertThat(manager.getActiveConsumerCount()).isEqualTo(1);

        manager.removeConsumer("c2");

        assertThat(manager.getTotalConsumerCount()).isEqualTo(1);
        assertThat(manager.getActiveConsumerCount()).isEqualTo(0);

        log.info("Counts are accurate throughout lifecycle");
    }
}
