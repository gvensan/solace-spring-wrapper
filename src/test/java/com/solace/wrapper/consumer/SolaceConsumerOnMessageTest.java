package com.solace.wrapper.consumer;

import com.solace.messaging.MessagingService;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Targets the {@link SolaceConsumer#onMessage} outcome branches not exercised elsewhere:
 * AUTO-ack success/failure, direct-mode delivery, the deserialize-exception path, the
 * not-running short-circuit, termination callbacks, and {@code getDescription}.
 */
class SolaceConsumerOnMessageTest {

    static volatile Env CURRENT;

    static class Env {
        final AtomicInteger ackCount = new AtomicInteger();
        final AtomicInteger settleCount = new AtomicInteger();
        volatile PersistentMessageReceiver persistentReceiver;
        volatile DirectMessageReceiver directReceiver;

        MessagingService service() {
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
                            persistentReceiver = (PersistentMessageReceiver) Proxy.newProxyInstance(
                                    getClass().getClassLoader(), new Class[]{PersistentMessageReceiver.class},
                                    (rp, rm, ra) -> {
                                        switch (rm.getName()) {
                                            case "ack": ackCount.incrementAndGet(); return null;
                                            case "settle": settleCount.incrementAndGet(); return null;
                                            default: return null;
                                        }
                                    });
                            return persistentReceiver;
                        }
                        return bp;
                    });
        }

        private Object directBuilder() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{com.solace.messaging.DirectMessageReceiverBuilder.class}, (bp, bm, ba) -> {
                        if ("build".equals(bm.getName())) {
                            directReceiver = (DirectMessageReceiver) Proxy.newProxyInstance(
                                    getClass().getClassLoader(), new Class[]{DirectMessageReceiver.class},
                                    (rp, rm, ra) -> null);
                            return directReceiver;
                        }
                        return bp;
                    });
        }
    }

    static class StubCM extends SolaceConnectionManager {
        StubCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
        @Override protected void initializePrimaryService() { }
        @Override public MessagingService createMessagingService() { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String id) { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String id, String c) { return CURRENT.service(); }
    }

    /** Serializer that returns a fixed value, or throws if {@code boom} is set. */
    static class StubSerializer implements MessageSerializer {
        volatile boolean boom = false;
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService s, Object o) { return null; }
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService s, Object o, Object d) { return null; }
        @Override public byte[] serializeToBytes(MessagingService s, Object o) { return new byte[0]; }
        @SuppressWarnings("unchecked")
        @Override public <T> T deserialize(InboundMessage m, Class<T> t) {
            if (boom) throw new RuntimeException("deserialize failure");
            return (T) "payload";
        }
        @Override public String deserializeToString(InboundMessage m) { return "payload"; }
    }

    private StubCM cm;
    private StubSerializer serializer;

    @BeforeEach
    void init() {
        CURRENT = new Env();
        var props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555");
        props.setMsgVpn("default");
        props.setClientUsername("default");
        props.setClientPassword("");
        cm = new StubCM(props);
        serializer = new StubSerializer();
    }

    private static InboundMessage inbound() {
        return (InboundMessage) Proxy.newProxyInstance(
                SolaceConsumerOnMessageTest.class.getClassLoader(),
                new Class[]{InboundMessage.class}, (p, m, a) -> null);
    }

    private SolaceConsumer<String> persistentAuto(SolaceMessageHandler<String> handler) {
        SolaceConsumer<String> c = new SolaceConsumer<>(
                "c", "q.auto", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, serializer);
        c.start();
        return c;
    }

    @Test
    void auto_ack_success_does_not_explicitly_ack() {
        AtomicInteger handled = new AtomicInteger();
        SolaceConsumer<String> c = persistentAuto((msg, in) -> handled.incrementAndGet());

        c.onMessage(inbound());

        assertThat(handled.get()).isEqualTo(1);
        // AUTO ack mode relies on the library; the wrapper must not call ack()/settle() itself.
        assertThat(CURRENT.ackCount.get()).isZero();
        assertThat(CURRENT.settleCount.get()).isZero();
    }

    @Test
    void auto_ack_failure_does_not_settle() {
        SolaceConsumer<String> c = persistentAuto((msg, in) -> { throw new RuntimeException("fail"); });

        c.onMessage(inbound());

        // Under AUTO ack the wrapper cannot nack explicitly; it must not call settle().
        assertThat(CURRENT.settleCount.get()).isZero();
    }

    @Test
    void deserialize_failure_settles_for_manual_persistent() {
        serializer.boom = true;
        SolaceConsumer<String> c = new SolaceConsumer<>(
                "c", "q.boom", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, (msg, in) -> { }, cm, serializer);
        c.start();

        c.onMessage(inbound());

        // The exception is caught and the message is negatively settled for redelivery.
        assertThat(CURRENT.settleCount.get()).isEqualTo(1);
    }

    @Test
    void not_running_consumer_ignores_messages() {
        AtomicInteger handled = new AtomicInteger();
        SolaceConsumer<String> c = persistentAuto((msg, in) -> handled.incrementAndGet());
        c.stop();

        c.onMessage(inbound());

        assertThat(handled.get()).isZero();
    }

    @Test
    void direct_mode_processes_without_ack() {
        AtomicInteger handled = new AtomicInteger();
        SolaceConsumer<String> c = new SolaceConsumer<>(
                "c", "", new String[]{"t/>"}, SolaceConsumer.MessagingMode.DIRECT, false,
                SolaceConsumer.AckMode.AUTO, String.class, (msg, in) -> handled.incrementAndGet(), cm, serializer);
        c.start();

        c.onMessage(inbound());

        assertThat(handled.get()).isEqualTo(1);
        assertThat(CURRENT.ackCount.get()).isZero();
        assertThat(CURRENT.settleCount.get()).isZero();
        assertThat(c.getDescription()).contains("DIRECT").contains("t/>");
    }

    @Test
    void termination_callbacks_mark_not_running() {
        SolaceConsumer<String> persistent = persistentAuto((msg, in) -> { });
        assertThat(persistent.isRunning()).isTrue();
        persistent.onTermination(CURRENT.persistentReceiver);
        assertThat(persistent.isRunning()).isFalse();

        CURRENT = new Env();
        SolaceConsumer<String> direct = new SolaceConsumer<>(
                "d", "", new String[]{"t/>"}, SolaceConsumer.MessagingMode.DIRECT, false,
                SolaceConsumer.AckMode.AUTO, String.class, (msg, in) -> { }, cm, serializer);
        direct.start();
        assertThat(direct.isRunning()).isTrue();
        direct.onTermination(CURRENT.directReceiver);
        assertThat(direct.isRunning()).isFalse();
    }
}
