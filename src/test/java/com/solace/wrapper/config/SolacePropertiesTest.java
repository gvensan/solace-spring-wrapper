package com.solace.wrapper.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SolaceProperties} — defaults, setter round-trips, and the side effect that
 * enabling consumer/publisher isolation forces reconnect retries on.
 */
class SolacePropertiesTest {

    @Test
    void sensible_defaults() {
        SolaceProperties p = new SolaceProperties();

        assertThat(p.getHost()).isEmpty();
        assertThat(p.getMsgVpn()).isEqualTo("default");
        assertThat(p.getClientUsername()).isEqualTo("default");
        assertThat(p.getClientPassword()).isEmpty();
        assertThat(p.getClientName()).isEqualTo("solace-spring-wrapper");
        assertThat(p.isReconnectRetries()).isTrue();
        assertThat(p.getConnectTimeoutInMillis()).isEqualTo(30000);
        assertThat(p.getKeepAliveIntervalInMillis()).isEqualTo(3000);
        assertThat(p.getDirectPublisherBackpressure())
                .isEqualTo(SolaceProperties.BackpressureStrategy.WAIT);
        assertThat(p.isReapplyDirectSubscriptions()).isTrue();
        assertThat(p.isGenerateSenderId()).isTrue();
        assertThat(p.isIsolateConsumers()).isFalse();
        assertThat(p.isIsolatePublishers()).isFalse();
        assertThat(p.getMaxConsumerConnections()).isEqualTo(50);
        assertThat(p.getMaxPublisherConnections()).isEqualTo(50);
        assertThat(p.getTerminationTimeoutMs()).isEqualTo(5000);
        assertThat(p.getPendingConfirmTimeoutMs()).isEqualTo(30000L);
    }

    @Test
    void enabling_isolate_consumers_forces_reconnect_retries() {
        SolaceProperties p = new SolaceProperties();
        p.setReconnectRetries(false);
        assertThat(p.isReconnectRetries()).isFalse();

        p.setIsolateConsumers(true);

        assertThat(p.isIsolateConsumers()).isTrue();
        assertThat(p.isReconnectRetries())
                .as("isolation requires reconnect retries for stability")
                .isTrue();
    }

    @Test
    void enabling_isolate_publishers_forces_reconnect_retries() {
        SolaceProperties p = new SolaceProperties();
        p.setReconnectRetries(false);

        p.setIsolatePublishers(true);

        assertThat(p.isIsolatePublishers()).isTrue();
        assertThat(p.isReconnectRetries()).isTrue();
    }

    @Test
    void disabling_isolation_does_not_toggle_reconnect_retries() {
        SolaceProperties p = new SolaceProperties();
        p.setReconnectRetries(false);

        p.setIsolateConsumers(false);
        p.setIsolatePublishers(false);

        assertThat(p.isReconnectRetries()).isFalse();
    }

    @Test
    void core_setters_round_trip() {
        SolaceProperties p = new SolaceProperties();
        p.setHost("tcps://broker:55443");
        p.setMsgVpn("vpn-x");
        p.setClientUsername("user");
        p.setClientPassword("pass");
        p.setClientName("my-client");
        p.setApplicationDescription("desc");
        p.setConnectionRetries(4);
        p.setReconnectionAttempts(7);
        p.setReconnectionAttemptsWaitIntervalInMillis(1234);
        p.setPublisherExecutorCoreSize(3);
        p.setPublisherExecutorMaxSize(9);
        p.setPublisherExecutorQueueCapacity(2000);
        p.setDirectPublisherBackpressure(SolaceProperties.BackpressureStrategy.REJECT);
        p.setDirectPublisherBackpressureWaitMs(25);
        p.setMaxConsumerConnections(10);
        p.setMaxPublisherConnections(20);
        p.setTerminationTimeoutMs(9000);
        p.setPendingConfirmTimeoutMs(60000L);

        assertThat(p.getHost()).isEqualTo("tcps://broker:55443");
        assertThat(p.getMsgVpn()).isEqualTo("vpn-x");
        assertThat(p.getClientUsername()).isEqualTo("user");
        assertThat(p.getClientPassword()).isEqualTo("pass");
        assertThat(p.getClientName()).isEqualTo("my-client");
        assertThat(p.getApplicationDescription()).isEqualTo("desc");
        assertThat(p.getConnectionRetries()).isEqualTo(4);
        assertThat(p.getReconnectionAttempts()).isEqualTo(7);
        assertThat(p.getReconnectionAttemptsWaitIntervalInMillis()).isEqualTo(1234);
        assertThat(p.getPublisherExecutorCoreSize()).isEqualTo(3);
        assertThat(p.getPublisherExecutorMaxSize()).isEqualTo(9);
        assertThat(p.getPublisherExecutorQueueCapacity()).isEqualTo(2000);
        assertThat(p.getDirectPublisherBackpressure())
                .isEqualTo(SolaceProperties.BackpressureStrategy.REJECT);
        assertThat(p.getDirectPublisherBackpressureWaitMs()).isEqualTo(25);
        assertThat(p.getMaxConsumerConnections()).isEqualTo(10);
        assertThat(p.getMaxPublisherConnections()).isEqualTo(20);
        assertThat(p.getTerminationTimeoutMs()).isEqualTo(9000);
        assertThat(p.getPendingConfirmTimeoutMs()).isEqualTo(60000L);
    }

    @Test
    void tls_and_oauth_setters_round_trip() {
        SolaceProperties p = new SolaceProperties();
        p.setOauth2AccessToken("token");
        p.setOauth2IssuerIdentifier("issuer");
        p.setTlsTrustStorePath("/ts.jks");
        p.setTlsTrustStorePassword("tsp");
        p.setTlsTrustStoreFormat("PKCS12");
        p.setTlsIgnoreCertificateExpiration(true);
        p.setClientCertKeystorePath("/ks.p12");
        p.setClientCertKeystorePassword("ksp");
        p.setClientCertKeystoreFormat("PKCS12");
        p.setClientCertPrivateKeyPassword("pk");

        assertThat(p.getOauth2AccessToken()).isEqualTo("token");
        assertThat(p.getOauth2IssuerIdentifier()).isEqualTo("issuer");
        assertThat(p.getTlsTrustStorePath()).isEqualTo("/ts.jks");
        assertThat(p.getTlsTrustStorePassword()).isEqualTo("tsp");
        assertThat(p.getTlsTrustStoreFormat()).isEqualTo("PKCS12");
        assertThat(p.isTlsIgnoreCertificateExpiration()).isTrue();
        assertThat(p.getClientCertKeystorePath()).isEqualTo("/ks.p12");
        assertThat(p.getClientCertKeystorePassword()).isEqualTo("ksp");
        assertThat(p.getClientCertKeystoreFormat()).isEqualTo("PKCS12");
        assertThat(p.getClientCertPrivateKeyPassword()).isEqualTo("pk");
    }

    @Test
    void optional_transport_tuning_setters_round_trip() {
        SolaceProperties p = new SolaceProperties();
        p.setConnectionRetriesPerHost(2);
        p.setKeepAliveWithoutResponseLimit(5);
        p.setTcpNoDelay(Boolean.TRUE);
        p.setGenerateSendTimestamps(Boolean.TRUE);
        p.setGenerateReceiveTimestamps(Boolean.FALSE);
        p.setReceiverDirectNoLocal(Boolean.TRUE);
        p.setReceiverPersistentNoLocal(Boolean.FALSE);
        p.setPublisherPersistentAckTimeoutInMs(2500);
        p.setPublisherPersistentAckWindowSize(50);
        p.setReadTimeoutInMillis(15000);
        p.setKeepAliveIntervalInMillis(1000);
        p.setReconnectRetriesWaitInMillis(2000);

        assertThat(p.getConnectionRetriesPerHost()).isEqualTo(2);
        assertThat(p.getKeepAliveWithoutResponseLimit()).isEqualTo(5);
        assertThat(p.getTcpNoDelay()).isTrue();
        assertThat(p.getGenerateSendTimestamps()).isTrue();
        assertThat(p.getGenerateReceiveTimestamps()).isFalse();
        assertThat(p.getReceiverDirectNoLocal()).isTrue();
        assertThat(p.getReceiverPersistentNoLocal()).isFalse();
        assertThat(p.getPublisherPersistentAckTimeoutInMs()).isEqualTo(2500);
        assertThat(p.getPublisherPersistentAckWindowSize()).isEqualTo(50);
        assertThat(p.getReadTimeoutInMillis()).isEqualTo(15000);
        assertThat(p.getKeepAliveIntervalInMillis()).isEqualTo(1000);
        assertThat(p.getReconnectRetriesWaitInMillis()).isEqualTo(2000);
    }
}
