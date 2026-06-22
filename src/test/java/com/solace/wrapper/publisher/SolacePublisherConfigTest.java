package com.solace.wrapper.publisher;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.wrapper.config.SolaceProperties;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link SolacePublisher} construction with custom executor sizing + REJECT backpressure, and
 * the {@code createMessageWithProperties} fallback when {@code messagingService.messageBuilder()}
 * returns {@code null}. Uses a compact broker-free harness.
 */
class SolacePublisherConfigTest {

    private SolacePublisher publisher;

    /** Service whose messageBuilder() returns null to force the serializer fallback. */
    private static MessagingService nullBuilderService() {
        return (MessagingService) Proxy.newProxyInstance(SolacePublisherConfigTest.class.getClassLoader(),
                new Class[]{MessagingService.class}, (p, m, a) -> {
                    switch (m.getName()) {
                        case "connect": return p;
                        case "isConnected": return true;
                        case "messageBuilder": return null; // force fallback
                        case "createDirectMessagePublisherBuilder": return directBuilder();
                        default:
                            if (m.getReturnType().equals(boolean.class)) return false;
                            return null;
                    }
                });
    }

    private static Object directBuilder() {
        return Proxy.newProxyInstance(SolacePublisherConfigTest.class.getClassLoader(),
                new Class[]{com.solace.messaging.DirectMessagePublisherBuilder.class}, (bp, bm, ba) -> {
                    if ("build".equals(bm.getName())) {
                        return (DirectMessagePublisher) Proxy.newProxyInstance(
                                SolacePublisherConfigTest.class.getClassLoader(),
                                new Class[]{DirectMessagePublisher.class}, (rp, rm, ra) -> {
                                    if ("hashCode".equals(rm.getName())) return System.identityHashCode(rp);
                                    if ("equals".equals(rm.getName())) return rp == ra[0];
                                    return null; // start/publish/terminate no-ops
                                });
                    }
                    return bp; // onBackPressureWait/Reject fluent
                });
    }

    static class NoConnectCM extends SolaceConnectionManager {
        NoConnectCM(SolaceProperties p) { super(p); }
        @Override protected void initializePrimaryService() { }
        @Override public MessagingService createMessagingService() { return nullBuilderService(); }
        @Override public MessagingService createMessagingService(String c) { return nullBuilderService(); }
        @Override public MessagingService createPublisherService(String id, String c) { return nullBuilderService(); }
    }

    static class CountingSerializer implements MessageSerializer {
        final AtomicInteger serializeWithDest = new AtomicInteger();
        @Override public OutboundMessage serialize(MessagingService s, Object o) { return msg(); }
        @Override public OutboundMessage serialize(MessagingService s, Object o, Object d) {
            serializeWithDest.incrementAndGet();
            return msg();
        }
        @Override public byte[] serializeToBytes(MessagingService s, Object o) {
            return o != null ? o.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];
        }
        @SuppressWarnings("unchecked") @Override public <T> T deserialize(com.solace.messaging.receiver.InboundMessage m, Class<T> t) { return null; }
        @Override public String deserializeToString(com.solace.messaging.receiver.InboundMessage m) { return null; }
        private OutboundMessage msg() {
            return (OutboundMessage) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{OutboundMessage.class}, (p, m, a) -> null);
        }
    }

    private CountingSerializer serializer;

    private SolacePublisher build(SolaceProperties props) {
        serializer = new CountingSerializer();
        publisher = new SolacePublisher(new NoConnectCM(props), serializer);
        publisher.init();
        return publisher;
    }

    private static SolaceProperties props() {
        SolaceProperties p = new SolaceProperties();
        p.setHost("tcp://noop:55555"); p.setMsgVpn("default");
        p.setClientUsername("default"); p.setClientPassword("");
        return p;
    }

    @AfterEach
    void tearDown() {
        if (publisher != null) publisher.shutdown();
    }

    @Test
    void builds_with_custom_executor_sizing_and_reject_backpressure() {
        SolaceProperties p = props();
        p.setPublisherExecutorCoreSize(3);
        p.setPublisherExecutorMaxSize(9);
        p.setPublisherExecutorQueueCapacity(500);
        p.setDirectPublisherBackpressure(SolaceProperties.BackpressureStrategy.REJECT);
        p.setDirectPublisherBackpressureWaitMs(20);

        build(p);
        // A direct publish exercises the configured (REJECT) direct publisher.
        publisher.publishToTopic("t/cfg", "x");
        assertThat(publisher.getActivePublisherCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void null_message_builder_falls_back_to_serializer() {
        build(props());
        // messageBuilder() returns null -> createMessageWithProperties falls back to serialize(ms,obj,dest).
        publisher.publishToTopicWithProperties("t/fallback", "body",
                new MessageProperties().setCorrelationId("cid"));
        assertThat(serializer.serializeWithDest.get()).isGreaterThanOrEqualTo(1);
    }
}
