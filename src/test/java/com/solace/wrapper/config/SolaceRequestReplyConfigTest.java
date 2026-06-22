package com.solace.wrapper.config;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.metrics.SolaceMetrics;
import com.solace.wrapper.requestreply.SolaceRequestor;
import com.solace.wrapper.serialization.JsonMessageSerializer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers {@link SolaceRequestReplyProperties} and the {@link SolaceAutoConfiguration#solaceRequestor}
 * bean factory method.
 */
class SolaceRequestReplyConfigTest {

    static class NoConnectCM extends SolaceConnectionManager {
        NoConnectCM(SolaceProperties p) { super(p); }
        @Override protected void initializePrimaryService() { }
        @Override public MessagingService createMessagingService() { return null; }
    }

    private static SolaceProperties props() {
        SolaceProperties p = new SolaceProperties();
        p.setHost("tcp://noop:55555"); p.setMsgVpn("default");
        p.setClientUsername("default"); p.setClientPassword("");
        return p;
    }

    @Test
    void properties_default_and_round_trip() {
        SolaceRequestReplyProperties rr = new SolaceRequestReplyProperties();
        assertThat(rr.getDefaultTimeoutMs()).isEqualTo(5000L);
        rr.setDefaultTimeoutMs(12345L);
        assertThat(rr.getDefaultTimeoutMs()).isEqualTo(12345L);
    }

    @Test
    void auto_configuration_builds_requestor() {
        SolaceAutoConfiguration cfg = new SolaceAutoConfiguration();
        SolaceRequestReplyProperties rr = new SolaceRequestReplyProperties();
        rr.setDefaultTimeoutMs(7777L);

        @SuppressWarnings("unchecked")
        ObjectProvider<SolaceMetrics> metricsProvider = mock(ObjectProvider.class);
        when(metricsProvider.getIfAvailable()).thenReturn(new SolaceMetrics(new SimpleMeterRegistry(), true));

        SolaceRequestor requestor = cfg.solaceRequestor(new NoConnectCM(props()), new JsonMessageSerializer(),
                rr, metricsProvider);
        assertThat(requestor).isNotNull();
        assertThat(requestor.getActivePublisherCount()).isZero();
    }
}
