package com.solace.wrapper.consumer;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.config.SolaceProperties;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the lifecycle and lookup surface of {@link SolaceConsumerManager}: stop/restart/remove
 * (including unknown-id no-op branches), start/stop-all, status snapshots, and lookup by queue /
 * message type — using a broker-free stub connection manager.
 */
class SolaceConsumerManagerLifecycleTest {

    private SolaceConsumerManager manager;

    static class StubCM extends SolaceConnectionManager {
        StubCM(SolaceProperties p) { super(p); }
        @Override protected void initializePrimaryService() { }
        @Override public MessagingService createMessagingService() { return stub(); }
        @Override public MessagingService createConsumerService(String id) { return stub(); }
        @Override public MessagingService createConsumerService(String id, String clientName) { return stub(); }
        @Override public void removeConsumerService(String id) { }

        private MessagingService stub() {
            return (MessagingService) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return true;
                            case "createPersistentMessageReceiverBuilder": return persistentBuilder();
                            case "createDirectMessageReceiverBuilder": return directBuilder();
                            default:
                                if (m.getReturnType().equals(boolean.class)) return false;
                                if (m.getReturnType().equals(int.class)) return 0;
                                return null;
                        }
                    });
        }

        private Object persistentBuilder() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{com.solace.messaging.PersistentMessageReceiverBuilder.class}, (bp, bm, ba) -> {
                        if ("build".equals(bm.getName())) {
                            return Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class[]{com.solace.messaging.receiver.PersistentMessageReceiver.class},
                                    (rp, rm, ra) -> "isRunning".equals(rm.getName()) ? Boolean.TRUE : null);
                        }
                        return bp;
                    });
        }

        private Object directBuilder() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{com.solace.messaging.DirectMessageReceiverBuilder.class}, (bp, bm, ba) -> {
                        if ("build".equals(bm.getName())) {
                            return Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class[]{com.solace.messaging.receiver.DirectMessageReceiver.class},
                                    (rp, rm, ra) -> "isRunning".equals(rm.getName()) ? Boolean.TRUE : null);
                        }
                        return bp;
                    });
        }
    }

    static class StubSerializer implements MessageSerializer {
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService s, Object o) { return null; }
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService s, Object o, Object d) { return null; }
        @Override public byte[] serializeToBytes(MessagingService s, Object o) {
            return o != null ? o.toString().getBytes(StandardCharsets.UTF_8) : new byte[0];
        }
        @SuppressWarnings("unchecked") @Override public <T> T deserialize(com.solace.messaging.receiver.InboundMessage m, Class<T> t) { return (T) "x"; }
        @Override public String deserializeToString(com.solace.messaging.receiver.InboundMessage m) { return "x"; }
    }

    @BeforeEach
    void setup() {
        SolaceProperties props = new SolaceProperties();
        props.setHost("tcp://noop:55555");
        props.setMsgVpn("default");
        props.setClientUsername("default");
        props.setClientPassword("");
        manager = new SolaceConsumerManager(new StubCM(props), new StubSerializer());
    }

    @AfterEach
    void teardown() {
        manager.shutdown();
    }

    private String createConsumer(String queue) {
        return manager.createConsumer(queue, String.class, (msg, in) -> { });
    }

    @Test
    void stop_restart_and_remove_lifecycle() {
        String id = createConsumer("q.life");
        assertThat(manager.isConsumerRunning(id)).isTrue();

        manager.stopConsumer(id);
        assertThat(manager.isConsumerRunning(id)).isFalse();

        manager.restartConsumer(id);
        assertThat(manager.isConsumerRunning(id)).isTrue();

        manager.removeConsumer(id);
        assertThat(manager.hasConsumer(id)).isFalse();
        assertThat(manager.getTotalConsumerCount()).isZero();
    }

    @Test
    void operations_on_unknown_consumer_are_noops() {
        // These hit the "consumer not found" warning branches without throwing.
        manager.stopConsumer("nope");
        manager.restartConsumer("nope");
        manager.removeConsumer("nope");
        assertThat(manager.isConsumerRunning("nope")).isFalse();
        assertThat(manager.hasConsumer("nope")).isFalse();
        assertThat(manager.getConsumerStatus("nope")).isNull();
    }

    @Test
    void start_all_and_stop_all() {
        String a = createConsumer("q.a");
        String b = createConsumer("q.b");

        manager.stopAllConsumers();
        assertThat(manager.getActiveConsumerCount()).isZero();

        manager.startAllConsumers();
        assertThat(manager.getActiveConsumerCount()).isEqualTo(2);
        assertThat(manager.isConsumerRunning(a)).isTrue();
        assertThat(manager.isConsumerRunning(b)).isTrue();
    }

    @Test
    void status_snapshots_and_lookups() {
        String a = createConsumer("q.lookup");
        createConsumer("q.other");

        assertThat(manager.getConsumerIds()).contains(a).hasSize(2);
        assertThat(manager.getAllConsumerStatuses()).hasSize(2).containsKey(a);

        SolaceConsumerManager.ConsumerStatus status = manager.getConsumerStatus(a);
        assertThat(status).isNotNull();
        assertThat(status.getQueueName()).isEqualTo("q.lookup");
        assertThat(status.isRunning()).isTrue();
        assertThat(status.getMessageType()).isEqualTo("String");
        assertThat(status.getDestinationDescription()).contains("queue=q.lookup");
        assertThat(status.toString()).contains("q.lookup").contains("running=true");

        assertThat(manager.getConsumersByQueue("q.lookup")).containsExactly(a);
        assertThat(manager.getConsumersByQueue("absent")).isEmpty();
        assertThat(manager.getConsumersByMessageType(String.class)).contains(a).hasSize(2);
        assertThat(manager.getConsumersByMessageType(Integer.class)).isEmpty();
    }

    @Test
    void enhanced_overloads_delegate_and_register() {
        SolaceMessageHandler<Object> auto = (msg, in) -> { };
        SolaceManualAckMessageHandler<Object> manual = (msg, in, ack) -> { };

        // Backward-compatible overload without ackMode (defaults to AUTO).
        manager.createEnhancedConsumerRaw("e1", "q.e1", new String[0],
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT, true,
                String.class, auto);

        // Short overload with ackMode + auto-ack handler.
        manager.createEnhancedConsumerRaw("e2", "q.e2", new String[0],
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.AUTO, true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO, String.class, auto);

        // Short overload with manual-ack handler (forces MANUAL).
        manager.createEnhancedConsumerRaw("e3", "q.e3", new String[0],
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT, true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.MANUAL, String.class, manual);

        // Overload with explicit local backoff parameters.
        manager.createEnhancedConsumerRaw("e4", "q.e4", new String[0],
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT, true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO, String.class, auto,
                2, 100, 2.0, 1000);

        // Overload with clientName + local backoff.
        manager.createEnhancedConsumerRaw("e5", "q.e5", new String[0],
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT, true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO, String.class, auto,
                null, 2, 100, 2.0, 1000);

        // Raw consumer (no generics) overload.
        manager.createConsumerRaw("e6", "q.e6", String.class, auto);

        assertThat(manager.getTotalConsumerCount()).isEqualTo(6);
        assertThat(manager.getConsumerStatus("e3").getMessagingMode()).isEqualTo("PERSISTENT");
        assertThat(manager.getConsumersByQueue("q.e4")).containsExactly("e4");
    }

    @Test
    void duplicate_consumer_id_is_rejected() {
        manager.createConsumer("dup", "q.x", String.class, (msg, in) -> { });
        assertThatThrownBy(() -> manager.createConsumer("dup", "q.y", String.class, (msg, in) -> { }))
                .isInstanceOf(com.solace.wrapper.exception.SolaceConsumerException.class)
                .hasMessageContaining("already exists");
    }

    @Test
    void auto_mode_with_queue_and_topics_resolves_to_persistent() {
        manager.createEnhancedConsumerRaw("auto1", "q.both", new String[]{"t/a", "t/b"},
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.AUTO, true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO, String.class,
                (com.solace.wrapper.consumer.SolaceMessageHandler<Object>) (msg, in) -> { });
        assertThat(manager.getConsumerStatus("auto1").getMessagingMode()).isEqualTo("PERSISTENT");
    }

    @Test
    void auto_mode_with_neither_queue_nor_topics_is_rejected() {
        assertThatThrownBy(() -> manager.createEnhancedConsumerRaw("auto2", "", new String[0],
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.AUTO, true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO, String.class,
                (com.solace.wrapper.consumer.SolaceMessageHandler<Object>) (msg, in) -> { }))
                .isInstanceOf(com.solace.wrapper.exception.SolaceConsumerException.class);
        assertThat(manager.hasConsumer("auto2")).isFalse();
    }

    @Test
    void explicit_direct_mode_with_topics() {
        manager.createEnhancedConsumerRaw("direct1", "", new String[]{"t/x"},
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.DIRECT, false,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO, String.class,
                (com.solace.wrapper.consumer.SolaceMessageHandler<Object>) (msg, in) -> { });
        assertThat(manager.getConsumerStatus("direct1").getMessagingMode()).isEqualTo("DIRECT");
    }
}
