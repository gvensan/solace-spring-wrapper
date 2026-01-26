package com.solace.wrapper.health;

import com.solace.wrapper.connection.SolaceConnectionManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class SolaceHealthIndicatorTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceHealthIndicatorTest.class);

    @Test
    void health_up_reports_expected_details() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: health_up_reports_expected_details\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the Spring Boot Actuator health indicator reports UP\n" +
                "  status with comprehensive connection details when Solace is connected.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Health status = UP when isConnected() returns true\n" +
                "  2. Connection info details (host, vpn, clientName)\n" +
                "  3. Service stats (consumer counts, reconnect counts)\n" +
                "  4. Timestamp tracking (last reconnect, last interrupt)\n" +
                "\n" +
                "HEALTH INDICATOR INTEGRATION:\n" +
                "  - Exposed at /actuator/health endpoint\n" +
                "  - Used by Kubernetes readiness/liveness probes\n" +
                "  - Aggregated with other health indicators\n" +
                "  - Details useful for debugging connection issues\n" +
                "\n" +
                "DETAILS EXPLAINED:\n" +
                "  - host/vpn/clientName: Connection target identification\n" +
                "  - primaryConnected: Is main connection active?\n" +
                "  - consumerServices.*: How many consumer connections exist/connected\n" +
                "  - reconnect.*: Historical reconnection attempts/successes\n" +
                "  - service.interrupt.*: Connection interruption tracking\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Mock ConnectionManager with connected state and full stats\n" +
                "  2. Call health() on SolaceHealthIndicator\n" +
                "  3. Verify status is UP and all details are present\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Status = UP\n" +
                "  - All connection details populated correctly\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Mocking SolaceConnectionManager with connected state");
        SolaceConnectionManager cm = mock(SolaceConnectionManager.class);

        when(cm.isConnected()).thenReturn(true);
        when(cm.getConnectionInfo()).thenReturn(new SolaceConnectionManager.ConnectionInfo(
                "tcp://h:55555", "vpn1", "client-A", true, 2
        ));
        when(cm.getServiceStats()).thenReturn(new SolaceConnectionManager.ServiceStats(
                true, 3, 2, "h", "vpn1",
                1, 1, 0,
                100L, 200L, 300L,
                "none"
        ));

        log.info("STEP 2: Creating SolaceHealthIndicator and invoking health()");
        SolaceHealthIndicator ind = new SolaceHealthIndicator(cm);
        Health h = ind.health();

        log.info("STEP 3: Verifying health status is UP with all expected details");
        assertThat(h.getStatus()).isEqualTo(Status.UP);
        assertThat(h.getDetails())
                .containsEntry("host", "tcp://h:55555")
                .containsEntry("vpn", "vpn1")
                .containsEntry("clientName", "client-A")
                .containsEntry("primaryConnected", true)
                .containsEntry("consumerServices.total", 3)
                .containsEntry("consumerServices.connected", 2)
                .containsEntry("reconnect.attempt.count", 1)
                .containsEntry("reconnect.success.count", 1)
                .containsEntry("service.interrupt.count", 0)
                .containsEntry("last.reconnect.attempt.ts", 100L)
                .containsEntry("last.reconnected.ts", 200L)
                .containsEntry("last.service.interrupt.ts", 300L)
                .containsEntry("last.service.interrupt.cause", "none");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Status: " + h.getStatus() + " (expected: UP)\n" +
                "\n" +
                "  Connection Details:\n" +
                "    host: " + h.getDetails().get("host") + "\n" +
                "    vpn: " + h.getDetails().get("vpn") + "\n" +
                "    clientName: " + h.getDetails().get("clientName") + "\n" +
                "    primaryConnected: " + h.getDetails().get("primaryConnected") + "\n" +
                "\n" +
                "  Consumer Services:\n" +
                "    total: " + h.getDetails().get("consumerServices.total") + "\n" +
                "    connected: " + h.getDetails().get("consumerServices.connected") + "\n" +
                "\n" +
                "  Reconnection Stats:\n" +
                "    attempts: " + h.getDetails().get("reconnect.attempt.count") + "\n" +
                "    successes: " + h.getDetails().get("reconnect.success.count") + "\n" +
                "    interrupts: " + h.getDetails().get("service.interrupt.count") + "\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Health indicator correctly detected connected state\n" +
                "  - All connection metadata exposed for monitoring\n" +
                "  - Reconnection stats available for reliability analysis\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - /actuator/health will show Solace as UP\n" +
                "  - Kubernetes probes will pass\n" +
                "  - Ops teams can monitor connection details\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void health_down_sets_status_down() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: health_down_sets_status_down\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the Spring Boot Actuator health indicator reports DOWN\n" +
                "  status when the Solace connection is disconnected.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Health status = DOWN when isConnected() returns false\n" +
                "  2. Correct reporting of disconnected state\n" +
                "\n" +
                "HEALTH DOWN IMPLICATIONS:\n" +
                "  - /actuator/health returns HTTP 503 (Service Unavailable)\n" +
                "  - Kubernetes readiness probe fails → pod removed from service\n" +
                "  - Load balancer stops routing traffic to this instance\n" +
                "  - Alerts triggered in monitoring systems\n" +
                "\n" +
                "DISCONNECTION SCENARIOS:\n" +
                "  - Broker unreachable (network issue)\n" +
                "  - Authentication failure\n" +
                "  - Broker shutdown for maintenance\n" +
                "  - Connection not yet established (startup)\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Mock ConnectionManager with disconnected state\n" +
                "  2. Call health() on SolaceHealthIndicator\n" +
                "  3. Verify status is DOWN\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Status = DOWN\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Mocking SolaceConnectionManager with disconnected state");
        SolaceConnectionManager cm = mock(SolaceConnectionManager.class);
        when(cm.isConnected()).thenReturn(false);
        when(cm.getConnectionInfo()).thenReturn(new SolaceConnectionManager.ConnectionInfo(
                "tcp://h:55555", "vpn1", "client-A", false, 0
        ));
        when(cm.getServiceStats()).thenReturn(new SolaceConnectionManager.ServiceStats(
                false, 0, 0, "h", "vpn1",
                0, 0, 0,
                0L, 0L, 0L,
                null
        ));

        log.info("STEP 2: Creating SolaceHealthIndicator and invoking health()");
        SolaceHealthIndicator ind = new SolaceHealthIndicator(cm);
        Health h = ind.health();

        log.info("STEP 3: Verifying health status is DOWN");
        assertThat(h.getStatus()).isEqualTo(Status.DOWN);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Status: " + h.getStatus() + " (expected: DOWN)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Health indicator correctly detected disconnected state\n" +
                "  - Status DOWN will be returned to health endpoint\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - /actuator/health will return 503 with Solace DOWN\n" +
                "  - Kubernetes will mark pod as not ready\n" +
                "  - Traffic will be routed away from this instance\n" +
                "  - Reconnection attempts should be in progress\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}

