package com.solace.wrapper.requestreply;

import com.solace.messaging.MessagingService;
import com.solace.messaging.PubSubPlusClientException;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.RequestReplyMessagePublisher;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.exception.SolaceRequestException;
import com.solace.wrapper.exception.SolaceRequestTimeoutException;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SolaceRequestor} covering blocking and async paths, timeout and error
 * mapping, the configured default timeout, and publisher lifecycle — using dynamic proxies in place
 * of the Solace request-reply publisher (no broker).
 */
class SolaceRequestorTest {

    enum Mode { SUCCESS, TIMEOUT, ERROR }

    static volatile Env CURRENT;

    static class Env {
        volatile Mode mode = Mode.SUCCESS;
        volatile boolean failBuild = false;
        volatile String lastTopic;
        volatile long lastTimeout;
        final AtomicInteger startCount = new AtomicInteger();
        final AtomicInteger terminateCount = new AtomicInteger();

        MessagingService service() {
            return (MessagingService) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return true;
                            case "requestReply": return rr(m.getReturnType());
                            default:
                                if (m.getReturnType().equals(boolean.class)) return false;
                                return null;
                        }
                    });
        }

        private Object rr(Class<?> type) {
            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{type}, (p, m, a) -> {
                if ("createRequestReplyMessagePublisherBuilder".equals(m.getName())) {
                    Class<?> bt = m.getReturnType();
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{bt}, (bp, bm, ba) -> {
                        if ("build".equals(bm.getName())) {
                            if (failBuild) throw new RuntimeException("build failed");
                            return publisher();
                        }
                        return bp;
                    });
                }
                return null;
            });
        }

        private RequestReplyMessagePublisher publisher() {
            return (RequestReplyMessagePublisher) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{RequestReplyMessagePublisher.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "start": startCount.incrementAndGet(); return p;
                            case "terminate": terminateCount.incrementAndGet(); return null;
                            case "publishAwaitResponse": {
                                // (OutboundMessage, Topic, long) — record and react to mode.
                                lastTopic = a[1].toString();
                                lastTimeout = (long) a[a.length - 1];
                                if (mode == Mode.TIMEOUT) throw timeoutException();
                                if (mode == Mode.ERROR) throw new RuntimeException("broker error");
                                return reply();
                            }
                            case "publish": {
                                // (OutboundMessage, ReplyMessageHandler, Object, Topic, long)
                                lastTimeout = (long) a[a.length - 1];
                                RequestReplyMessagePublisher.ReplyMessageHandler handler =
                                        (RequestReplyMessagePublisher.ReplyMessageHandler) a[1];
                                if (mode == Mode.TIMEOUT) {
                                    handler.onMessage(null, a[2], timeoutException());
                                } else if (mode == Mode.ERROR) {
                                    handler.onMessage(null, a[2], Mockito.mock(PubSubPlusClientException.class));
                                } else {
                                    handler.onMessage(reply(), a[2], null);
                                }
                                return null;
                            }
                            default: return null;
                        }
                    });
        }

        private InboundMessage reply() {
            return (InboundMessage) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{InboundMessage.class}, (p, m, a) -> null);
        }

        private PubSubPlusClientException.TimeoutException timeoutException() {
            return Mockito.mock(PubSubPlusClientException.TimeoutException.class);
        }
    }

    static class StubCM extends SolaceConnectionManager {
        StubCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
        @Override protected void initializePrimaryService() { }
        @Override public MessagingService createMessagingService() { return CURRENT.service(); }
        @Override public MessagingService createPublisherService(String id, String clientName) { return CURRENT.service(); }
    }

    static class StubSerializer implements MessageSerializer {
        volatile Object reply = "REPLY";
        volatile boolean failDeserialize = false;
        @Override public OutboundMessage serialize(MessagingService s, Object o) {
            return (OutboundMessage) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{OutboundMessage.class}, (p, m, a) -> null);
        }
        @Override public OutboundMessage serialize(MessagingService s, Object o, Object d) { return serialize(s, o); }
        @Override public byte[] serializeToBytes(MessagingService s, Object o) { return new byte[0]; }
        @SuppressWarnings("unchecked") @Override public <T> T deserialize(InboundMessage m, Class<T> t) {
            if (failDeserialize) throw new RuntimeException("bad payload");
            return (T) reply;
        }
        @Override public String deserializeToString(InboundMessage m) { return "x"; }
    }

    private StubCM cm;
    private StubSerializer serializer;
    private SolaceRequestor requestor;

    @BeforeEach
    void setup() {
        CURRENT = new Env();
        var props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555"); props.setMsgVpn("default");
        props.setClientUsername("default"); props.setClientPassword("");
        cm = new StubCM(props);
        serializer = new StubSerializer();
        requestor = new SolaceRequestor(cm, serializer, 4321);
    }

    @Test
    void blocking_request_returns_reply() {
        String reply = requestor.request("rr/topic", "req", String.class, Duration.ofSeconds(2));
        assertThat(reply).isEqualTo("REPLY");
        assertThat(CURRENT.lastTopic).contains("rr/topic");
        assertThat(CURRENT.lastTimeout).isEqualTo(2000L);
        assertThat(CURRENT.startCount.get()).isEqualTo(1);
    }

    @Test
    void blocking_request_uses_default_timeout_overload() {
        requestor.request("rr/topic", "req", String.class);
        assertThat(CURRENT.lastTimeout).isEqualTo(4321L); // configured default
    }

    @Test
    void blocking_request_maps_timeout() {
        CURRENT.mode = Mode.TIMEOUT;
        assertThatThrownBy(() -> requestor.request("rr/t", "req", String.class, Duration.ofMillis(500)))
                .isInstanceOf(SolaceRequestTimeoutException.class)
                .hasMessageContaining("timed out");
    }

    @Test
    void blocking_request_maps_other_errors() {
        CURRENT.mode = Mode.ERROR;
        assertThatThrownBy(() -> requestor.request("rr/e", "req", String.class, Duration.ofSeconds(1)))
                .isInstanceOf(SolaceRequestException.class)
                .isNotInstanceOf(SolaceRequestTimeoutException.class);
    }

    @Test
    void non_positive_timeout_is_rejected() {
        assertThatThrownBy(() -> requestor.request("rr/x", "req", String.class, Duration.ZERO))
                .isInstanceOf(SolaceRequestException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void async_request_completes_with_reply() {
        CompletableFuture<String> f = requestor.requestAsync("rr/a", "req", String.class, Duration.ofSeconds(3));
        assertThat(f).succeedsWithin(Duration.ofSeconds(1)).isEqualTo("REPLY");
        assertThat(CURRENT.lastTimeout).isEqualTo(3000L);
    }

    @Test
    void async_request_completes_exceptionally_on_timeout() {
        CURRENT.mode = Mode.TIMEOUT;
        CompletableFuture<String> f = requestor.requestAsync("rr/a", "req", String.class);
        assertThatThrownBy(f::join)
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .hasCauseInstanceOf(SolaceRequestTimeoutException.class);
    }

    @Test
    void async_request_completes_exceptionally_on_error() {
        CURRENT.mode = Mode.ERROR;
        CompletableFuture<String> f = requestor.requestAsync("rr/a", "req", String.class);
        assertThatThrownBy(f::join).hasCauseInstanceOf(SolaceRequestException.class);
    }

    @Test
    void records_request_metrics_on_success_and_timeout() {
        io.micrometer.core.instrument.simple.SimpleMeterRegistry registry =
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        requestor.setMetrics(new com.solace.wrapper.metrics.SolaceMetrics(registry, true));

        requestor.request("rr/m", "req", String.class, Duration.ofSeconds(1));
        assertThat(registry.find("solace.request.total").tag("outcome", "success").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("solace.request.latency").timer().count()).isEqualTo(1L);

        CURRENT.mode = Mode.TIMEOUT;
        assertThatThrownBy(() -> requestor.request("rr/m", "req", String.class, Duration.ofMillis(200)))
                .isInstanceOf(SolaceRequestTimeoutException.class);
        assertThat(registry.find("solace.request.timeouts.total").counter().count()).isEqualTo(1.0);
    }

    @Test
    void publisher_create_failure_is_wrapped() {
        CURRENT.failBuild = true;
        assertThatThrownBy(() -> requestor.request("rr/x", "req", String.class, Duration.ofSeconds(1)))
                .isInstanceOf(SolaceRequestException.class)
                .hasMessageContaining("publisher");
    }

    @Test
    void async_reply_deserialize_failure_completes_exceptionally() {
        serializer.failDeserialize = true;
        CompletableFuture<String> f = requestor.requestAsync("rr/a", "req", String.class);
        assertThatThrownBy(f::join)
                .hasCauseInstanceOf(SolaceRequestException.class)
                .hasMessageContaining("deserialize");
    }

    @Test
    void publisher_is_cached_and_shutdown_terminates() {
        requestor.request("rr/1", "a", String.class, Duration.ofSeconds(1));
        requestor.request("rr/2", "b", String.class, Duration.ofSeconds(1));
        assertThat(requestor.getActivePublisherCount()).isEqualTo(1); // single primary publisher reused
        assertThat(CURRENT.startCount.get()).isEqualTo(1);

        requestor.shutdown();
        assertThat(CURRENT.terminateCount.get()).isEqualTo(1);
        assertThat(requestor.getActivePublisherCount()).isZero();
    }
}
