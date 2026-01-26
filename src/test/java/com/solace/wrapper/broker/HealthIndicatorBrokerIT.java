package com.solace.wrapper.broker;

import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.health.SolaceHealthIndicator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("broker")
public class HealthIndicatorBrokerIT {

    private static final Logger log = LoggerFactory.getLogger(HealthIndicatorBrokerIT.class);

    private BrokerTestSettings settings;
    private SolaceConnectionManager connectionManager;

    @BeforeEach
    void setup() {
        settings = BrokerTestSettings.load();
        settings.assumeConfigured();
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "SETUP: Connecting to Solace broker at " + settings.host + "\n" +
                "───────────────────────────────────────────────────────────────");
        connectionManager = new SolaceConnectionManager(BrokerTestSupport.toSolaceProperties(settings));
    }

    @AfterEach
    void cleanup() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "CLEANUP: Shutting down connection manager\n" +
                "───────────────────────────────────────────────────────────────");
        try {
            if (connectionManager != null) connectionManager.shutdown();
        } catch (Exception ignore) { }
    }

    @Test
    void health_indicator_reports_up() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: health_indicator_reports_up\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify Spring Boot Actuator health indicator correctly reports UP status\n" +
                "  when the Solace connection is healthy.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. SolaceHealthIndicator - custom Spring Boot health indicator\n" +
                "  2. Integration with Spring Boot Actuator /health endpoint\n" +
                "  3. Health details including host, vpn, and connection status\n" +
                "\n" +
                "SPRING BOOT ACTUATOR INTEGRATION:\n" +
                "  - SolaceHealthIndicator implements HealthIndicator interface\n" +
                "  - Automatically exposed via /actuator/health endpoint\n" +
                "  - Enables Kubernetes liveness/readiness probes for Solace connectivity\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create SolaceHealthIndicator with active connection manager\n" +
                "  2. Call health() method to get current status\n" +
                "  3. Verify status is UP and details include connection info\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Status: UP (broker is connected and healthy)\n" +
                "  - Details include: host, vpn, primaryConnected=true\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating SolaceHealthIndicator with active connection manager");
        SolaceHealthIndicator indicator = new SolaceHealthIndicator(connectionManager);

        log.info("STEP 2: Invoking health() to get current health status");
        var health = indicator.health();

        log.info("STEP 3: Examining health status and details");
        log.info("        Status: {}", health.getStatus());
        log.info("        Details: {}", health.getDetails());

        var status = health.getStatus();
        var details = health.getDetails();
        boolean hasHost = details.containsKey("host");
        boolean hasVpn = details.containsKey("vpn");
        Object primaryConnected = details.get("primaryConnected");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Health status: " + status + " (expected: UP)\n" +
                "  Contains 'host' key: " + hasHost + " (expected: true)\n" +
                "  Contains 'vpn' key: " + hasVpn + " (expected: true)\n" +
                "  primaryConnected: " + primaryConnected + " (expected: true)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - SolaceHealthIndicator correctly detected active broker connection\n" +
                "  - Health details provide debugging info for operations teams\n" +
                "  - This indicator can be used for:\n" +
                "    * Kubernetes liveness probes (is the app healthy?)\n" +
                "    * Kubernetes readiness probes (can the app receive traffic?)\n" +
                "    * Load balancer health checks\n" +
                "    * Monitoring dashboards (Prometheus, Grafana)\n" +
                "\n" +
                "STATUS: " + (Status.UP.equals(status) && hasHost && hasVpn && Boolean.TRUE.equals(primaryConnected) ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsKey("host")
                .containsKey("vpn")
                .containsEntry("primaryConnected", true);
    }
}
