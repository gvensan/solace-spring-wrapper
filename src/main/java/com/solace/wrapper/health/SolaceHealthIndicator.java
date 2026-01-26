package com.solace.wrapper.health;

import com.solace.wrapper.connection.SolaceConnectionManager;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

/**
 * Spring Boot HealthIndicator that reports Solace connectivity and basic stats.
 */
@Component
public class SolaceHealthIndicator implements HealthIndicator {

    private final SolaceConnectionManager connectionManager;

    public SolaceHealthIndicator(SolaceConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public Health health() {
        boolean up = connectionManager.isConnected();
        SolaceConnectionManager.ConnectionInfo info = connectionManager.getConnectionInfo();
        SolaceConnectionManager.ServiceStats stats = connectionManager.getServiceStats();
        String interruptionCause = stats.getLastServiceInterruptionCause();

        Health.Builder builder = new Health.Builder(up ? Status.UP : Status.DOWN)
                .withDetail("host", info.getHost())
                .withDetail("vpn", info.getVpnName())
                .withDetail("clientName", info.getClientName())
                .withDetail("primaryConnected", stats.isPrimaryConnected())
                .withDetail("consumerServices.total", stats.getTotalConsumerServices())
                .withDetail("consumerServices.connected", stats.getConnectedConsumerServices())
                .withDetail("reconnect.attempt.count", stats.getReconnectionAttemptCount())
                .withDetail("reconnect.success.count", stats.getReconnectedCount())
                .withDetail("service.interrupt.count", stats.getServiceInterruptionCount())
                .withDetail("last.reconnect.attempt.ts", stats.getLastReconnectionAttemptTs())
                .withDetail("last.reconnected.ts", stats.getLastReconnectedTs())
                .withDetail("last.service.interrupt.ts", stats.getLastServiceInterruptionTs())
                .withDetail("last.service.interrupt.cause", interruptionCause != null ? interruptionCause : "unknown");

        return builder.build();
    }
}
