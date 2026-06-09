package com.solace.wrapper.connection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the value objects exposed by {@link SolaceConnectionManager} for monitoring:
 * {@link SolaceConnectionManager.ConnectionInfo} and {@link SolaceConnectionManager.ServiceStats}.
 * These are pure data holders and need no broker.
 */
class SolaceConnectionStatsTest {

    @Test
    void connection_info_exposes_fields_and_toString() {
        SolaceConnectionManager.ConnectionInfo info = new SolaceConnectionManager.ConnectionInfo(
                "tcp://host:55555", "vpn-a", "client-a", true, 3);

        assertThat(info.getHost()).isEqualTo("tcp://host:55555");
        assertThat(info.getVpnName()).isEqualTo("vpn-a");
        assertThat(info.getClientName()).isEqualTo("client-a");
        assertThat(info.isConnected()).isTrue();
        assertThat(info.getConsumerServiceCount()).isEqualTo(3);

        assertThat(info.toString())
                .contains("tcp://host:55555")
                .contains("vpn-a")
                .contains("client-a")
                .contains("connected=true")
                .contains("consumers=3");
    }

    @Test
    void service_stats_exposes_all_fields_and_toString() {
        SolaceConnectionManager.ServiceStats stats = new SolaceConnectionManager.ServiceStats(
                true, 5, 4, "tcp://host:55555", "vpn-b",
                2, 1, 3, 100L, 200L, 300L, "interruption-cause");

        assertThat(stats.isPrimaryConnected()).isTrue();
        assertThat(stats.getTotalConsumerServices()).isEqualTo(5);
        assertThat(stats.getConnectedConsumerServices()).isEqualTo(4);
        assertThat(stats.getBrokerHost()).isEqualTo("tcp://host:55555");
        assertThat(stats.getVpnName()).isEqualTo("vpn-b");
        assertThat(stats.getReconnectionAttemptCount()).isEqualTo(2);
        assertThat(stats.getReconnectedCount()).isEqualTo(1);
        assertThat(stats.getServiceInterruptionCount()).isEqualTo(3);
        assertThat(stats.getLastReconnectionAttemptTs()).isEqualTo(100L);
        assertThat(stats.getLastReconnectedTs()).isEqualTo(200L);
        assertThat(stats.getLastServiceInterruptionTs()).isEqualTo(300L);
        assertThat(stats.getLastServiceInterruptionCause()).isEqualTo("interruption-cause");

        assertThat(stats.toString())
                .contains("primary=true")
                .contains("total=5")
                .contains("connected=4")
                .contains("vpn-b")
                .contains("reconAttempts=2")
                .contains("reconnected=1")
                .contains("interruptions=3");
    }
}
