package com.solace.wrapper.requestreply;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.RequestReplyMessageReceiver;
import com.solace.wrapper.annotation.SolaceReplier;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SolaceReplierEndpoint}, exercising the request-reply receiver wiring and the
 * reply behaviour (return value sent as reply, void/null = no reply, exception = no reply, raw
 * InboundMessage injection) using dynamic proxies in place of the Solace SDK — no broker required.
 */
class SolaceReplierEndpointTest {

    static volatile Env CURRENT;

    /** Captures the request handler and reply invocations from the stubbed SDK chain. */
    static class Env {
        volatile Object capturedHandler;            // RequestReplyMessageReceiver.RequestMessageHandler
        final AtomicInteger replyCount = new AtomicInteger();
        final AtomicInteger terminateCount = new AtomicInteger();
        volatile boolean connected = true;

        MessagingService service() {
            return (MessagingService) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return connected;
                            case "requestReply": return requestReplyService(m.getReturnType());
                            case "messageBuilder": return messageBuilder();
                            default:
                                if (m.getReturnType().equals(boolean.class)) return false;
                                return null;
                        }
                    });
        }

        private Object requestReplyService(Class<?> type) {
            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{type}, (p, m, a) -> {
                if ("createRequestReplyMessageReceiverBuilder".equals(m.getName())) {
                    return receiverBuilder(m.getReturnType());
                }
                return null;
            });
        }

        private Object receiverBuilder(Class<?> type) {
            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{type}, (p, m, a) -> {
                String n = m.getName();
                if (n.startsWith("onReplierBackPressure")) return p; // fluent
                if ("build".equals(n)) return receiver();
                return null;
            });
        }

        private RequestReplyMessageReceiver receiver() {
            return (RequestReplyMessageReceiver) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{RequestReplyMessageReceiver.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "receiveAsync": capturedHandler = a[0]; return null;
                            case "terminate": terminateCount.incrementAndGet(); return null;
                            case "setReplyFailureListener":
                            case "start":
                            default: return null;
                        }
                    });
        }

        private Object messageBuilder() {
            Class<?> builderType = com.solace.messaging.publisher.OutboundMessageBuilder.class;
            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{builderType}, (p, m, a) -> {
                if ("build".equals(m.getName())) {
                    return (OutboundMessage) Proxy.newProxyInstance(getClass().getClassLoader(),
                            new Class[]{OutboundMessage.class}, (mp, mm, ma) -> null);
                }
                return p;
            });
        }

        /** A Replier proxy that counts reply() invocations. */
        RequestReplyMessageReceiver.Replier replier() {
            return (RequestReplyMessageReceiver.Replier) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{RequestReplyMessageReceiver.Replier.class}, (p, m, a) -> {
                        if ("reply".equals(m.getName())) replyCount.incrementAndGet();
                        return null;
                    });
        }
    }

    static class StubCM extends SolaceConnectionManager {
        StubCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
        @Override protected void initializePrimaryService() { }
        @Override public MessagingService createMessagingService() { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String id) { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String id, String c) { return CURRENT.service(); }
        @Override public void removeConsumerService(String id) { }
    }

    /** Records the object handed to serialize() and the value returned by deserialize(). */
    static class RecordingSerializer implements MessageSerializer {
        final AtomicReference<Object> lastSerialized = new AtomicReference<>();
        volatile Object deserializeResult = "request";
        @Override public OutboundMessage serialize(MessagingService s, Object o) {
            lastSerialized.set(o);
            return (OutboundMessage) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{OutboundMessage.class}, (p, m, a) -> null);
        }
        @Override public OutboundMessage serialize(MessagingService s, Object o, Object d) { return serialize(s, o); }
        @Override public byte[] serializeToBytes(MessagingService s, Object o) { return new byte[0]; }
        @SuppressWarnings("unchecked") @Override public <T> T deserialize(InboundMessage m, Class<T> t) { return (T) deserializeResult; }
        @Override public String deserializeToString(InboundMessage m) { return "request"; }
    }

    static class Beans {
        volatile Object receivedRequest;
        volatile InboundMessage receivedInbound;

        public String echo(String req) { receivedRequest = req; return "reply:" + req; }
        public void fireAndForget(String req) { receivedRequest = req; }
        public String boom(String req) { throw new RuntimeException("handler boom"); }
        public String withInbound(String req, InboundMessage raw) { receivedInbound = raw; return "ok"; }
        public String nullReply(String req) { return null; }
    }

    private StubCM cm;
    private RecordingSerializer serializer;
    private Beans beans;

    @BeforeEach
    void setup() {
        CURRENT = new Env();
        var props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555"); props.setMsgVpn("default");
        props.setClientUsername("default"); props.setClientPassword("");
        cm = new StubCM(props);
        serializer = new RecordingSerializer();
        beans = new Beans();
    }

    private SolaceReplierEndpoint endpoint(String method, Class<?>... paramTypes) throws Exception {
        Method m = Beans.class.getMethod(method, paramTypes);
        return new SolaceReplierEndpoint("r1", "rr/echo", "", "", String.class, beans, m,
                cm, serializer, SolaceReplier.Backpressure.ELASTIC, 1024);
    }

    private static InboundMessage inbound() {
        return (InboundMessage) Proxy.newProxyInstance(SolaceReplierEndpointTest.class.getClassLoader(),
                new Class[]{InboundMessage.class}, (p, m, a) -> null);
    }

    @SuppressWarnings("unchecked")
    private void deliverRequest() {
        // Invoke the RequestMessageHandler that the endpoint registered via receiveAsync.
        ((RequestReplyMessageReceiver.RequestMessageHandler) CURRENT.capturedHandler)
                .onMessage(inbound(), CURRENT.replier());
    }

    @Test
    void return_value_is_sent_as_reply() throws Exception {
        SolaceReplierEndpoint ep = endpoint("echo", String.class);
        ep.start();
        assertThat(ep.isRunning()).isTrue();
        assertThat(CURRENT.capturedHandler).isNotNull();

        serializer.deserializeResult = "hello";
        deliverRequest();

        assertThat(beans.receivedRequest).isEqualTo("hello");
        assertThat(CURRENT.replyCount.get()).isEqualTo(1);
        assertThat(serializer.lastSerialized.get()).isEqualTo("reply:hello");
    }

    @Test
    void void_method_sends_no_reply() throws Exception {
        SolaceReplierEndpoint ep = endpoint("fireAndForget", String.class);
        ep.start();
        deliverRequest();

        assertThat(beans.receivedRequest).isEqualTo("request");
        assertThat(CURRENT.replyCount.get()).isZero();
    }

    @Test
    void null_return_sends_no_reply() throws Exception {
        SolaceReplierEndpoint ep = endpoint("nullReply", String.class);
        ep.start();
        deliverRequest();
        assertThat(CURRENT.replyCount.get()).isZero();
    }

    @Test
    void handler_exception_sends_no_reply() throws Exception {
        SolaceReplierEndpoint ep = endpoint("boom", String.class);
        ep.start();
        deliverRequest(); // must not throw out of onRequest
        assertThat(CURRENT.replyCount.get()).isZero();
        assertThat(ep.isRunning()).isTrue();
    }

    @Test
    void raw_inbound_message_is_injected() throws Exception {
        SolaceReplierEndpoint ep = endpoint("withInbound", String.class, InboundMessage.class);
        ep.start();
        deliverRequest();
        assertThat(beans.receivedInbound).isNotNull();
        assertThat(CURRENT.replyCount.get()).isEqualTo(1);
    }

    @Test
    void stop_terminates_receiver_and_marks_not_running() throws Exception {
        SolaceReplierEndpoint ep = endpoint("echo", String.class);
        ep.start();
        ep.stop();
        assertThat(ep.isRunning()).isFalse();
        assertThat(CURRENT.terminateCount.get()).isEqualTo(1);
        // A request delivered after stop must be ignored.
        deliverRequest();
        assertThat(CURRENT.replyCount.get()).isZero();
    }

    @Test
    void start_is_idempotent() throws Exception {
        SolaceReplierEndpoint ep = endpoint("echo", String.class);
        ep.start();
        Object handler1 = CURRENT.capturedHandler;
        ep.start(); // no-op
        assertThat(CURRENT.capturedHandler).isSameAs(handler1);
    }

    @Test
    void start_fails_when_service_not_connected() throws Exception {
        CURRENT.connected = false;
        SolaceReplierEndpoint ep = endpoint("echo", String.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(ep::start)
                .isInstanceOf(com.solace.wrapper.exception.SolaceReplierException.class);
        assertThat(ep.isRunning()).isFalse();
    }

    @Test
    void backpressure_wait_and_reject_strategies_start() throws Exception {
        Method m = Beans.class.getMethod("echo", String.class);
        SolaceReplierEndpoint wait = new SolaceReplierEndpoint("rw", "rr/echo", "", "", String.class, beans, m,
                cm, serializer, SolaceReplier.Backpressure.WAIT, 16).withTerminationTimeout(200);
        wait.start();
        assertThat(wait.isRunning()).isTrue();

        SolaceReplierEndpoint reject = new SolaceReplierEndpoint("rj", "rr/echo", "", "", String.class, beans, m,
                cm, serializer, SolaceReplier.Backpressure.REJECT, 16);
        reject.start();
        assertThat(reject.isRunning()).isTrue();
    }

    @Test
    void shutdown_marks_shutdown_and_stops() throws Exception {
        SolaceReplierEndpoint ep = endpoint("echo", String.class);
        ep.start();
        ep.shutdown();
        assertThat(ep.isShutdown()).isTrue();
        assertThat(ep.isRunning()).isFalse();
        // start() after shutdown is a no-op.
        ep.start();
        assertThat(ep.isRunning()).isFalse();
    }

    @Test
    void records_reply_metrics_on_success_and_failure() throws Exception {
        io.micrometer.core.instrument.simple.SimpleMeterRegistry registry =
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        com.solace.wrapper.metrics.SolaceMetrics metrics =
                new com.solace.wrapper.metrics.SolaceMetrics(registry, true);

        SolaceReplierEndpoint ok = endpoint("echo", String.class);
        ok.withMetrics(metrics);
        ok.start();
        deliverRequest();
        assertThat(registry.find("solace.reply.total").tag("outcome", "success").counter().count())
                .isEqualTo(1.0);

        CURRENT = new Env();
        SolaceReplierEndpoint bad = endpoint("boom", String.class);
        bad.withMetrics(metrics);
        bad.start();
        deliverRequest();
        assertThat(registry.find("solace.reply.total").tag("outcome", "failure").counter().count())
                .isEqualTo(1.0);

        // A void/null result is recorded as a distinct no_reply outcome (not success).
        CURRENT = new Env();
        SolaceReplierEndpoint silent = endpoint("fireAndForget", String.class);
        silent.withMetrics(metrics);
        silent.start();
        deliverRequest();
        assertThat(registry.find("solace.reply.total").tag("outcome", "no_reply").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void share_name_builds_with_share_subscription() throws Exception {
        Method m = Beans.class.getMethod("echo", String.class);
        SolaceReplierEndpoint shared = new SolaceReplierEndpoint("rs", "rr/echo", "group-1", "", String.class,
                beans, m, cm, serializer, SolaceReplier.Backpressure.ELASTIC, 1024);
        shared.start();
        assertThat(shared.isRunning()).isTrue();
    }
}
