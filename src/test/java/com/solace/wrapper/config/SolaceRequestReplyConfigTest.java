package com.solace.wrapper.config;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.requestreply.SolaceRequestor;
import com.solace.wrapper.serialization.JsonMessageSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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

        SolaceRequestor requestor = cfg.solaceRequestor(new NoConnectCM(props()), new JsonMessageSerializer(), rr);
        assertThat(requestor).isNotNull();
        assertThat(requestor.getActivePublisherCount()).isZero();
    }
}
