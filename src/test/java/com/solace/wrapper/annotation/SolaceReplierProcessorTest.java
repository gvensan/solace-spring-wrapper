package com.solace.wrapper.annotation;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.RequestReplyMessageReceiver;
import com.solace.wrapper.annotation.processor.SolaceReplierProcessor;
import com.solace.wrapper.annotation.processor.SpelExpressionResolver;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.requestreply.SolaceReplierEndpoint;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link SolaceReplierProcessor} discovers {@code @SolaceReplier} methods and registers
 * repliers with the right id, request-type inference, autoStart handling, and duplicate skipping.
 */
class SolaceReplierProcessorTest {

    private SolaceReplierProcessor processor;
    static volatile boolean CONNECTED = true;

    static class StubCM extends SolaceConnectionManager {
        StubCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
        @Override protected void initializePrimaryService() { }
        @Override public MessagingService createMessagingService() { return svc(); }
        @Override public MessagingService createConsumerService(String id) { return svc(); }
        @Override public MessagingService createConsumerService(String id, String c) { return svc(); }
        @Override public void removeConsumerService(String id) { }

        private MessagingService svc() {
            return (MessagingService) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return CONNECTED;
                            case "requestReply": return rr(m.getReturnType());
                            default:
                                if (m.getReturnType().equals(boolean.class)) return false;
                                return null;
                        }
                    });
        }
        private Object rr(Class<?> type) {
            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{type}, (p, m, a) -> {
                if ("createRequestReplyMessageReceiverBuilder".equals(m.getName())) {
                    Class<?> bt = m.getReturnType();
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{bt}, (bp, bm, ba) -> {
                        if (bm.getName().startsWith("onReplierBackPressure")) return bp;
                        if ("build".equals(bm.getName())) {
                            return Proxy.newProxyInstance(getClass().getClassLoader(),
                                    new Class[]{RequestReplyMessageReceiver.class}, (rp, rm, ra) -> null);
                        }
                        return null;
                    });
                }
                return null;
            });
        }
    }

    static class StubSerializer implements MessageSerializer {
        @Override public OutboundMessage serialize(MessagingService s, Object o) { return null; }
        @Override public OutboundMessage serialize(MessagingService s, Object o, Object d) { return null; }
        @Override public byte[] serializeToBytes(MessagingService s, Object o) { return new byte[0]; }
        @SuppressWarnings("unchecked") @Override public <T> T deserialize(InboundMessage m, Class<T> t) { return (T) "x"; }
        @Override public String deserializeToString(InboundMessage m) { return "x"; }
    }

    static class Beans {
        @SolaceReplier(topic = "rr/a")
        public String a(String req) { return "ra"; }

        @SolaceReplier(topic = "rr/b", replierIdPrefix = "svc", messageType = "java.lang.Integer", autoStart = false)
        public String b(Integer req) { return "rb"; }

        @SolaceReplier(topic = "rr/d", replierId = "fixed")
        public String d(String req) { return "rd"; }
    }

    @BeforeEach
    void setup() throws Exception {
        CONNECTED = true;
        processor = new SolaceReplierProcessor();
        var props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555"); props.setMsgVpn("default");
        props.setClientUsername("default"); props.setClientPassword("");
        inject("connectionManager", new StubCM(props));
        inject("messageSerializer", new StubSerializer());
        processor.setSpelResolver(new SpelExpressionResolver());
    }

    private void inject(String field, Object value) throws Exception {
        Field f = SolaceReplierProcessor.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(processor, value);
    }

    @Test
    void discovers_and_registers_repliers() {
        processor.postProcessAfterInitialization(new Beans(), "beans");

        assertThat(processor.getReplierCount()).isEqualTo(3);

        SolaceReplierEndpoint fixed = processor.getEndpoint("fixed");
        assertThat(fixed).isNotNull();
        assertThat(fixed.getTopic()).isEqualTo("rr/d");
        assertThat(fixed.isRunning()).isTrue(); // autoStart default true
    }

    @Test
    void infers_request_type_and_honors_message_type_override() {
        processor.postProcessAfterInitialization(new Beans(), "beans");

        SolaceReplierEndpoint b = processor.getEndpoints().stream()
                .filter(e -> e.getReplierId().startsWith("svc-")).findFirst().orElseThrow();
        assertThat(b.getRequestType()).isEqualTo(Integer.class);     // messageType override
        assertThat(b.isRunning()).isFalse();                          // autoStart=false

        SolaceReplierEndpoint a = processor.getEndpoints().stream()
                .filter(e -> "rr/a".equals(e.getTopic())).findFirst().orElseThrow();
        assertThat(a.getRequestType()).isEqualTo(String.class);      // inferred from param
    }

    @Test
    void duplicate_explicit_id_is_skipped() {
        Beans bean = new Beans();
        processor.postProcessAfterInitialization(bean, "beans");
        int afterFirst = processor.getReplierCount();
        // Re-processing (as can happen with CGLIB proxies) must not double-register the fixed id.
        processor.postProcessAfterInitialization(bean, "beans");
        assertThat(processor.getEndpoint("fixed")).isNotNull();
        // The explicit-id replier is deduped; generated-id ones may re-add, but "fixed" stays single.
        long fixedCount = processor.getEndpoints().stream().filter(e -> e.getReplierId().equals("fixed")).count();
        assertThat(fixedCount).isEqualTo(1);
        assertThat(processor.getReplierCount()).isGreaterThanOrEqualTo(afterFirst);
    }

    @Test
    void start_all_starts_non_autostarted_repliers() {
        processor.postProcessAfterInitialization(new Beans(), "beans");
        SolaceReplierEndpoint b = processor.getEndpoints().stream()
                .filter(e -> e.getReplierId().startsWith("svc-")).findFirst().orElseThrow();
        assertThat(b.isRunning()).isFalse();

        processor.startAll();
        assertThat(b.isRunning()).isTrue();
    }

    @Test
    void autostart_replier_failure_fails_fast() {
        CONNECTED = false; // start() will fail because the messaging service is not connected
        assertThat(org.assertj.core.api.Assertions.catchThrowable(
                () -> processor.postProcessAfterInitialization(new Beans(), "beans")))
                .as("a broken autoStart replier must fail context startup, not be swallowed")
                .isInstanceOf(com.solace.wrapper.exception.SolaceReplierException.class);
    }

    @Test
    void shutdown_clears_repliers() {
        processor.postProcessAfterInitialization(new Beans(), "beans");
        assertThat(processor.getReplierCount()).isEqualTo(3);
        processor.shutdown();
        assertThat(processor.getReplierCount()).isZero();
    }
}
