package com.solace.wrapper.connection;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.config.SolaceProperties;
import com.solace.wrapper.exception.SolaceConnectionException;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the real {@link SolaceConnectionManager#createMessagingService} property-building paths
 * (optional transport tuning, reconnection settings, service defaults, TLS/auth strategy
 * application) without connecting to a broker. {@code MessagingService.builder().build()} only
 * constructs the client; it does not open a connection, so these run offline.
 */
class SolaceConnectionManagerBuildTest {

    /** Skips the connecting primary-service initialization so the constructor does not need a broker. */
    static class NoConnectCM extends SolaceConnectionManager {
        NoConnectCM(SolaceProperties p) { super(p); }
        @Override protected void initializePrimaryService() { /* no connect in tests */ }
    }

    private static SolaceProperties baseProps() {
        SolaceProperties p = new SolaceProperties();
        // build() resolves the host (DNS) but does not connect; use a resolvable host so the
        // client object is constructed offline.
        p.setHost("tcp://localhost:55555");
        p.setMsgVpn("vpn");
        p.setClientUsername("user");
        p.setClientPassword("pw");
        return p;
    }

    @Test
    void builds_service_with_full_optional_tuning() {
        SolaceProperties p = baseProps();
        p.setClientName("client-x");
        p.setApplicationDescription("desc");
        p.setReconnectRetries(true);
        p.setConnectionRetries(2);
        p.setConnectionRetriesPerHost(1);
        p.setReconnectionAttempts(5);
        p.setReconnectionAttemptsWaitIntervalInMillis(1000);
        p.setKeepAliveWithoutResponseLimit(4);
        p.setTcpNoDelay(Boolean.TRUE);
        p.setGenerateSendTimestamps(Boolean.TRUE);
        p.setGenerateReceiveTimestamps(Boolean.TRUE);
        p.setReceiverDirectNoLocal(Boolean.TRUE);
        p.setReceiverPersistentNoLocal(Boolean.TRUE);
        p.setPublisherPersistentAckTimeoutInMs(2000);
        p.setPublisherPersistentAckWindowSize(50);

        NoConnectCM cm = new NoConnectCM(p);
        MessagingService svc = cm.createMessagingService();
        assertThat(svc).isNotNull();
    }

    @Test
    void builds_service_with_reconnect_retries_disabled() {
        SolaceProperties p = baseProps();
        p.setReconnectRetries(false);

        NoConnectCM cm = new NoConnectCM(p);
        assertThat(cm.createMessagingService()).isNotNull();
    }

    @Test
    void builds_service_with_client_name_override() {
        NoConnectCM cm = new NoConnectCM(baseProps());
        assertThat(cm.createMessagingService("override-client")).isNotNull();
    }

    @Test
    void builds_custom_messaging_service() {
        NoConnectCM cm = new NoConnectCM(baseProps());
        Properties custom = new Properties();
        custom.setProperty("solace.messaging.client.name", "custom");
        assertThat(cm.createCustomMessagingService(custom)).isNotNull();
    }

    @Test
    void missing_host_throws_connection_exception() {
        SolaceProperties p = baseProps();
        p.setHost("   ");
        NoConnectCM cm = new NoConnectCM(p);
        assertThatThrownBy(cm::createMessagingService)
                .isInstanceOf(SolaceConnectionException.class)
                .hasMessageContaining("solace.host");
    }

    @Test
    void connection_info_and_stats_reflect_disconnected_primary() {
        NoConnectCM cm = new NoConnectCM(baseProps());

        // initializePrimaryService is a no-op here, so the primary service is null/absent.
        assertThat(cm.isConnected()).isFalse();

        SolaceConnectionManager.ConnectionInfo info = cm.getConnectionInfo();
        assertThat(info.isConnected()).isFalse();
        assertThat(info.getConsumerServiceCount()).isZero();
        assertThat(info.getHost()).isEqualTo("tcp://localhost:55555");

        assertThat(cm.getServiceInfo())
                .contains("tcp://localhost:55555")
                .contains("Connected: false");

        SolaceConnectionManager.ServiceStats stats = cm.getServiceStats();
        assertThat(stats.isPrimaryConnected()).isFalse();
        assertThat(stats.getBrokerHost()).isEqualTo("tcp://localhost:55555");
        assertThat(stats.getReconnectionAttemptCount()).isZero();
    }

    @Test
    void removing_unknown_services_is_a_noop() {
        NoConnectCM cm = new NoConnectCM(baseProps());
        // Should not throw for ids that were never registered.
        cm.removeConsumerService("nope");
        cm.removePublisherService("nope");
        assertThat(cm.getServiceStats().getTotalConsumerServices()).isZero();
    }

    @Test
    void isolation_forces_reconnect_retries_at_construction() {
        SolaceProperties p = baseProps();
        p.setIsolateConsumers(true);
        // Force the conflicting state that ensureReconnectRetriesForIsolation must correct.
        p.setReconnectRetries(false);

        NoConnectCM cm = new NoConnectCM(p);

        assertThat(cm.getProperties().isReconnectRetries())
                .as("isolation must force reconnect retries on")
                .isTrue();
    }

    @Test
    void shutdown_is_idempotent_and_blocks_further_creation() {
        NoConnectCM cm = new NoConnectCM(baseProps());
        cm.shutdown();
        cm.shutdown(); // second shutdown must not throw

        assertThatThrownBy(cm::createMessagingService)
                .isInstanceOf(SolaceConnectionException.class)
                .hasMessageContaining("shutdown");
    }
}
