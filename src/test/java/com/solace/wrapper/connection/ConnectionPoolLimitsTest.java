package com.solace.wrapper.connection;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.config.SolaceProperties;
import com.solace.wrapper.exception.SolaceConnectionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for connection pool size limits.
 * Verifies that maxConsumerConnections and maxPublisherConnections are enforced.
 */
public class ConnectionPoolLimitsTest {

    private static final Logger log = LoggerFactory.getLogger(ConnectionPoolLimitsTest.class);

    private TestableConnectionManager cm;
    private SolaceProperties props;

    @BeforeEach
    void setUp() {
        props = new SolaceProperties();
        props.setHost("tcp://noop:55555");
        props.setMsgVpn("default");
        props.setClientUsername("default");
        props.setClientPassword("");
        cm = new TestableConnectionManager(props);
    }

    @Test
    void createConsumerService_enforces_max_connections_when_isolated() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumerService_enforces_max_connections_when_isolated\n" +
                "PURPOSE: Verify maxConsumerConnections limit is enforced when isolation is enabled\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Enabling consumer isolation with maxConsumerConnections=3");
        // Enable isolation and set a low limit
        props.setIsolateConsumers(true);
        props.setMaxConsumerConnections(3);

        log.info("STEP 2: Creating 3 consumers (should all succeed)");
        // Should succeed for first 3 consumers
        MessagingService s1 = cm.createConsumerService("consumer-1");
        MessagingService s2 = cm.createConsumerService("consumer-2");
        MessagingService s3 = cm.createConsumerService("consumer-3");

        assertThat(s1).isNotNull();
        assertThat(s2).isNotNull();
        assertThat(s3).isNotNull();
        assertThat(cm.serviceCreationCount.get()).isEqualTo(3);
        log.info("        Created 3 consumers successfully, count=" + cm.serviceCreationCount.get());

        log.info("STEP 3: Attempting to create 4th consumer - should throw SolaceConnectionException");
        // Fourth consumer should fail
        assertThatThrownBy(() -> cm.createConsumerService("consumer-4"))
            .isInstanceOf(SolaceConnectionException.class)
            .hasMessageContaining("Maximum consumer connections reached")
            .hasMessageContaining("3");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Max consumer connections limit enforced correctly (3 max)\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void createConsumerService_reuses_existing_service_does_not_count_against_limit() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumerService_reuses_existing_service_does_not_count_against_limit\n" +
                "PURPOSE: Verify that requesting same consumer ID reuses existing service (doesn't count again)\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Enabling consumer isolation with maxConsumerConnections=2");
        props.setIsolateConsumers(true);
        props.setMaxConsumerConnections(2);

        log.info("STEP 2: Creating 2 consumers");
        // Create 2 consumers
        cm.createConsumerService("consumer-1");
        cm.createConsumerService("consumer-2");
        log.info("        Created count=" + cm.serviceCreationCount.get());

        log.info("STEP 3: Requesting consumer-1 again - should reuse existing service");
        // Requesting the same consumer ID should reuse, not create new
        MessagingService reused = cm.createConsumerService("consumer-1");
        assertThat(reused).isNotNull();
        assertThat(cm.serviceCreationCount.get()).isEqualTo(2); // Still only 2 created

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Reuse works correctly - still only " + cm.serviceCreationCount.get() + " services created\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void createPublisherService_enforces_max_connections_when_isolated() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createPublisherService_enforces_max_connections_when_isolated\n" +
                "PURPOSE: Verify maxPublisherConnections limit is enforced when isolation is enabled\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Enabling publisher isolation with maxPublisherConnections=2");
        // Enable isolation and set a low limit
        props.setIsolatePublishers(true);
        props.setMaxPublisherConnections(2);

        log.info("STEP 2: Creating 2 publishers (should both succeed)");
        // Should succeed for first 2 publishers
        MessagingService p1 = cm.createPublisherService("pub-1", null);
        MessagingService p2 = cm.createPublisherService("pub-2", null);

        assertThat(p1).isNotNull();
        assertThat(p2).isNotNull();
        log.info("        Created 2 publishers successfully");

        log.info("STEP 3: Attempting to create 3rd publisher - should throw SolaceConnectionException");
        // Third publisher should fail
        assertThatThrownBy(() -> cm.createPublisherService("pub-3", null))
            .isInstanceOf(SolaceConnectionException.class)
            .hasMessageContaining("Maximum publisher connections reached")
            .hasMessageContaining("2");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Max publisher connections limit enforced correctly (2 max)\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void zero_limit_means_unlimited() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: zero_limit_means_unlimited\n" +
                "PURPOSE: Verify that maxConsumerConnections=0 means unlimited connections\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Enabling consumer isolation with maxConsumerConnections=0 (unlimited)");
        props.setIsolateConsumers(true);
        props.setMaxConsumerConnections(0); // 0 = unlimited

        log.info("STEP 2: Creating 100 consumers - all should succeed with 0 limit");
        // Should be able to create many consumers
        for (int i = 0; i < 100; i++) {
            MessagingService s = cm.createConsumerService("consumer-" + i);
            assertThat(s).isNotNull();
        }
        assertThat(cm.serviceCreationCount.get()).isEqualTo(100);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: 0 limit = unlimited, created " + cm.serviceCreationCount.get() + " consumers successfully\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void non_isolated_mode_uses_shared_service_no_limit_issues() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: non_isolated_mode_uses_shared_service_no_limit_issues\n" +
                "PURPOSE: Verify that non-isolated mode shares primary service (limit doesn't apply)\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Setting isolateConsumers=false with maxConsumerConnections=1");
        props.setIsolateConsumers(false);
        props.setMaxConsumerConnections(1); // Even with limit of 1

        log.info("STEP 2: Creating 3 consumers - all should share the same primary service");
        // Non-isolated mode shares primary service, so limit doesn't apply
        MessagingService s1 = cm.createConsumerService("consumer-1");
        MessagingService s2 = cm.createConsumerService("consumer-2");
        MessagingService s3 = cm.createConsumerService("consumer-3");

        log.info("STEP 3: Verifying all consumers share the same service instance");
        // All should get the same (primary) service
        assertThat(s1).isSameAs(s2);
        assertThat(s2).isSameAs(s3);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Non-isolated mode shares primary service, limit doesn't apply\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void default_limits_are_reasonable() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: default_limits_are_reasonable\n" +
                "PURPOSE: Verify default connection pool limits are set to sensible defaults (50)\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Creating SolaceProperties with defaults");
        // Default limits should be set to 50
        SolaceProperties defaultProps = new SolaceProperties();

        log.info("STEP 2: Verifying default limits are 50 for both consumers and publishers");
        assertThat(defaultProps.getMaxConsumerConnections()).isEqualTo(50);
        assertThat(defaultProps.getMaxPublisherConnections()).isEqualTo(50);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Default limits are reasonable (maxConsumer=50, maxPublisher=50)\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    /**
     * Test-specific connection manager that counts service creations.
     * Note: serviceCreationCount may be null during parent constructor's call to createMessagingService.
     */
    static class TestableConnectionManager extends SolaceConnectionManager {
        AtomicInteger serviceCreationCount;

        TestableConnectionManager(SolaceProperties props) {
            super(props);
            // Initialize after super() to ensure field is accessible
            if (serviceCreationCount == null) {
                serviceCreationCount = new AtomicInteger();
            }
        }

        @Override
        public MessagingService createMessagingService() {
            return createStubService();
        }

        @Override
        public MessagingService createMessagingService(String clientNameOverride) {
            return createStubService();
        }

        private MessagingService createStubService() {
            // Null check needed because parent constructor may call this before field init
            if (serviceCreationCount != null) {
                serviceCreationCount.incrementAndGet();
            }
            AtomicBoolean connected = new AtomicBoolean(true);
            return (MessagingService) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{MessagingService.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "connect": return proxy;
                        case "disconnect": connected.set(false); return null;
                        case "isConnected": return connected.get();
                        case "hashCode": return System.identityHashCode(proxy);
                        case "equals": return proxy == args[0];
                        default: return null;
                    }
                }
            );
        }
    }
}
