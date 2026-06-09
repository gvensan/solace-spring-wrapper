package com.solace.wrapper.consumer;

import com.solace.messaging.MessagingService;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.metrics.SolaceMetrics;
import com.solace.wrapper.serialization.MessageSerializer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link SolaceConsumer} emits consume success/failure, retry, and latency metrics
 * through an injected {@link SolaceMetrics} facade. Uses dynamic proxies to stand in for the Solace
 * receiver so no broker is required.
 */
class SolaceConsumerMetricsTest {

    static volatile Env CURRENT;

    static class Env {
        volatile PersistentMessageReceiver persistentReceiver;

        MessagingService service() {
            return (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return true;
                            case "createPersistentMessageReceiverBuilder":
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{m.getReturnType()}, (bp, bm, ba) -> {
                                    String n = bm.getName();
                                    if ("withMissingResourcesCreationStrategy".equals(n)
                                            || "withMessageClientAcknowledgement".equals(n)
                                            || "withMessageAutoAcknowledgement".equals(n)
                                            || "withRequiredMessageClientOutcomeOperationSupport".equals(n)
                                            || "withSubscriptions".equals(n)) {
                                        return bp;
                                    }
                                    if ("build".equals(n)) {
                                        Env.this.persistentReceiver = (PersistentMessageReceiver) Proxy.newProxyInstance(
                                                getClass().getClassLoader(), new Class[]{PersistentMessageReceiver.class},
                                                (rp, rm, ra) -> null);
                                        return Env.this.persistentReceiver;
                                    }
                                    return null;
                                });
                            default:
                                if (m.getReturnType().equals(boolean.class)) return false;
                                if (m.getReturnType().equals(int.class)) return 0;
                                return null;
                        }
                    });
        }
    }

    static class NoopCM extends SolaceConnectionManager {
        NoopCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
        @Override public MessagingService createMessagingService() { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String consumerId) { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String consumerId, String clientName) { return CURRENT.service(); }
    }

    static class StubSerializer implements MessageSerializer {
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService s, Object o) { return null; }
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService s, Object o, Object d) { return null; }
        @Override public byte[] serializeToBytes(MessagingService s, Object o) { return new byte[0]; }
        @SuppressWarnings("unchecked")
        @Override public <T> T deserialize(InboundMessage message, Class<T> targetType) { return (T) "payload"; }
        @Override public String deserializeToString(InboundMessage message) { return "payload"; }
    }

    private SolaceConnectionManager cm;

    @BeforeEach
    void init() {
        CURRENT = new Env();
        var props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555");
        props.setMsgVpn("default");
        props.setClientUsername("default");
        props.setClientPassword("");
        cm = new NoopCM(props);
    }

    private static InboundMessage inbound() {
        return (InboundMessage) Proxy.newProxyInstance(
                SolaceConsumerMetricsTest.class.getClassLoader(), new Class[]{InboundMessage.class}, (p, m, a) -> {
                    if ("getPayloadAsString".equals(m.getName())) return "payload";
                    if ("getPayloadAsBytes".equals(m.getName())) return "payload".getBytes(StandardCharsets.UTF_8);
                    return null;
                });
    }

    @Test
    void records_consume_success_counter_and_latency() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMessageHandler<String> handler = (msg, in) -> { /* success */ };

        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "cm-1", "q.metrics", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer())
                .withMetrics(new SolaceMetrics(registry, true));
        cons.start();

        cons.onMessage(inbound());

        assertThat(registry.find("solace.consume.total")
                .tag("outcome", "success").tag("destination", "q.metrics").tag("consumerId", "cm-1")
                .counter()).isNotNull();
        assertThat(registry.find("solace.consume.total").tag("outcome", "success").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("solace.consume.latency").tag("outcome", "success").timer().count())
                .isEqualTo(1L);
    }

    @Test
    void records_consume_failure_and_retry_counters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AtomicInteger calls = new AtomicInteger();
        // Always fails -> exhausts retries (2 attempts => 1 retry recorded).
        SolaceMessageHandler<String> handler = (msg, in) -> {
            calls.incrementAndGet();
            throw new RuntimeException("boom");
        };

        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "cm-2", "q.fail", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer())
                .withLocalBackoff(2, 0, 1.0, 0)
                .withMetrics(new SolaceMetrics(registry, true));
        cons.start();

        cons.onMessage(inbound());

        assertThat(calls.get()).isEqualTo(2);
        assertThat(registry.find("solace.consume.total").tag("outcome", "failure").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("solace.consume.retries.total").tag("consumerId", "cm-2").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void no_metrics_when_facade_absent() {
        // Without withMetrics(), the default no-op facade must not throw on message processing.
        SolaceMessageHandler<String> handler = (msg, in) -> { /* success */ };
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "cm-3", "q.none", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer());
        cons.start();
        cons.onMessage(inbound());
        assertThat(cons.isRunning()).isTrue();
    }
}
