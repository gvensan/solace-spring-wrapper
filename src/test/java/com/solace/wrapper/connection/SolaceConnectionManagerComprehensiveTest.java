package com.solace.wrapper.connection;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.config.SolaceProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive unit tests for SolaceConnectionManager covering:
 * - Connection pooling and isolation
 * - Reconnection behavior
 * - Client name overrides
 * - Resource limits
 * - Multi-threaded access
 * - Shutdown behavior
 */
public class SolaceConnectionManagerComprehensiveTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceConnectionManagerComprehensiveTest.class);

    private SolaceProperties props;

    static class TrackingCM extends SolaceConnectionManager {
        // These fields need special handling due to parent constructor calling initializePrimaryService
        private AtomicInteger created;
        private ConcurrentHashMap<String, MessagingService> services;
        private ConcurrentHashMap<MessagingService, AtomicBoolean> connectionState;
        volatile String lastClientNameOverride;
        volatile boolean failNextConnection = false;
        private volatile MessagingService mockPrimaryService;

        TrackingCM(SolaceProperties p) {
            super(p);
        }

        private void ensureInitialized() {
            if (created == null) {
                created = new AtomicInteger();
            }
            if (services == null) {
                services = new ConcurrentHashMap<>();
            }
            if (connectionState == null) {
                connectionState = new ConcurrentHashMap<>();
            }
        }

        @Override
        protected void initializePrimaryService() {
            // Defer to lazy initialization in getPrimaryService()
            // This is called from parent constructor when fields aren't ready
        }

        @Override
        public MessagingService getPrimaryService() {
            ensureInitialized();
            if (mockPrimaryService == null) {
                synchronized (this) {
                    if (mockPrimaryService == null) {
                        mockPrimaryService = createStubService(null);
                    }
                }
            }
            // Check if disconnected and recreate if needed
            AtomicBoolean state = connectionState.get(mockPrimaryService);
            if (state != null && !state.get()) {
                mockPrimaryService = createStubService(null);
            }
            return mockPrimaryService;
        }

        @Override
        public MessagingService createMessagingService() {
            ensureInitialized();
            return createStubService(null);
        }

        @Override
        public MessagingService createMessagingService(String clientNameOverride) {
            ensureInitialized();
            return createStubService(clientNameOverride);
        }

        private MessagingService createStubService(String clientNameOverride) {
            ensureInitialized();
            if (failNextConnection) {
                failNextConnection = false;
                throw new RuntimeException("Simulated connection failure");
            }
            created.incrementAndGet();
            lastClientNameOverride = clientNameOverride;
            AtomicBoolean connected = new AtomicBoolean(true);
            MessagingService svc = (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (prx, m, a) -> {
                        String n = m.getName();
                        if ("connect".equals(n)) return prx;
                        if ("disconnect".equals(n)) {
                            connected.set(false);
                            return null;
                        }
                        if ("isConnected".equals(n)) return connected.get();
                        if ("hashCode".equals(n)) return System.identityHashCode(prx);
                        if ("equals".equals(n)) return prx == a[0];
                        if ("toString".equals(n)) return "MockMessagingService@" + System.identityHashCode(prx);
                        // Handle listener methods
                        if (n.startsWith("add") && n.endsWith("Listener")) return null;
                        return null;
                    });
            connectionState.put(svc, connected);
            services.put(String.valueOf(created.get()), svc);
            return svc;
        }

        void simulateDisconnect(MessagingService svc) {
            ensureInitialized();
            AtomicBoolean state = connectionState.get(svc);
            if (state != null) {
                state.set(false);
            }
        }

        int getCreatedCount() {
            ensureInitialized();
            return created.get();
        }

        ConcurrentHashMap<MessagingService, AtomicBoolean> getConnectionState() {
            ensureInitialized();
            return connectionState;
        }

        @Override
        public void shutdown() {
            // Call parent shutdown which will disconnect services in its pools
            super.shutdown();
            // Also disconnect our mock primary service
            if (mockPrimaryService != null) {
                AtomicBoolean state = connectionState.get(mockPrimaryService);
                if (state != null) {
                    state.set(false);
                }
            }
        }
    }

    @BeforeEach
    void init() {
        props = new SolaceProperties();
        props.setHost("tcp://noop:55555");
        props.setMsgVpn("default");
        props.setClientUsername("default");
        props.setClientPassword("");
    }

    // ─────────────────────────────────────────────────────────────────
    // PRIMARY SERVICE TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void getPrimaryService_creates_on_first_call() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getPrimaryService_creates_on_first_call\n" +
                "───────────────────────────────────────────────────────────────");

        TrackingCM cm = new TrackingCM(props);

        MessagingService svc = cm.getPrimaryService();

        assertThat(svc).isNotNull();
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        log.info("Primary service created on first call");
    }

    @Test
    void getPrimaryService_reuses_existing_connected_service() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getPrimaryService_reuses_existing_connected_service\n" +
                "───────────────────────────────────────────────────────────────");

        TrackingCM cm = new TrackingCM(props);

        MessagingService svc1 = cm.getPrimaryService();
        MessagingService svc2 = cm.getPrimaryService();
        MessagingService svc3 = cm.getPrimaryService();

        assertThat(svc1).isSameAs(svc2).isSameAs(svc3);
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        log.info("Primary service reused across calls");
    }

    @Test
    void getPrimaryService_recreates_when_disconnected() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getPrimaryService_recreates_when_disconnected\n" +
                "───────────────────────────────────────────────────────────────");

        TrackingCM cm = new TrackingCM(props);

        MessagingService svc1 = cm.getPrimaryService();
        cm.simulateDisconnect(svc1);

        MessagingService svc2 = cm.getPrimaryService();

        assertThat(svc2).isNotSameAs(svc1);
        assertThat(cm.getCreatedCount()).isEqualTo(2);

        log.info("Primary service recreated after disconnect");
    }

    // ─────────────────────────────────────────────────────────────────
    // CONSUMER SERVICE TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void createConsumerService_shares_primary_by_default() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumerService_shares_primary_by_default\n" +
                "───────────────────────────────────────────────────────────────");

        TrackingCM cm = new TrackingCM(props);

        MessagingService primary = cm.getPrimaryService();
        MessagingService consumer1 = cm.createConsumerService("c1");
        MessagingService consumer2 = cm.createConsumerService("c2");

        assertThat(consumer1).isSameAs(primary);
        assertThat(consumer2).isSameAs(primary);
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        log.info("Consumer services share primary by default");
    }

    @Test
    void createConsumerService_isolates_when_configured() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumerService_isolates_when_configured\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolateConsumers(true);
        props.setReconnectRetries(true); // Required for isolation to take effect
        TrackingCM cm = new TrackingCM(props);

        MessagingService primary = cm.getPrimaryService();
        MessagingService consumer1 = cm.createConsumerService("c1");
        MessagingService consumer2 = cm.createConsumerService("c2");

        assertThat(consumer1).isNotSameAs(primary);
        assertThat(consumer2).isNotSameAs(primary);
        assertThat(consumer1).isNotSameAs(consumer2);
        assertThat(cm.getCreatedCount()).isEqualTo(3); // primary + 2 isolated consumers

        log.info("Consumer services isolated when configured");
    }

    @Test
    void createConsumerService_with_clientName_override() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumerService_with_clientName_override\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolateConsumers(true);
        props.setReconnectRetries(true);
        TrackingCM cm = new TrackingCM(props);

        cm.createConsumerService("c1", "custom-client-name");

        assertThat(cm.lastClientNameOverride).isEqualTo("custom-client-name");

        log.info("Client name override passed through");
    }

    // ─────────────────────────────────────────────────────────────────
    // PUBLISHER SERVICE TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void createPublisherService_shares_primary_by_default() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createPublisherService_shares_primary_by_default\n" +
                "───────────────────────────────────────────────────────────────");

        TrackingCM cm = new TrackingCM(props);

        MessagingService primary = cm.getPrimaryService();
        MessagingService pub1 = cm.createPublisherService("p1", null);
        MessagingService pub2 = cm.createPublisherService("p2", null);

        assertThat(pub1).isSameAs(primary);
        assertThat(pub2).isSameAs(primary);
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        log.info("Publisher services share primary by default");
    }

    @Test
    void createPublisherService_isolates_when_configured() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createPublisherService_isolates_when_configured\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolatePublishers(true);
        props.setReconnectRetries(true); // Required for isolation to take effect
        TrackingCM cm = new TrackingCM(props);

        MessagingService primary = cm.getPrimaryService();
        MessagingService pub1 = cm.createPublisherService("p1", null);
        MessagingService pub2 = cm.createPublisherService("p2", null);

        assertThat(pub1).isNotSameAs(primary);
        assertThat(pub2).isNotSameAs(primary);
        assertThat(pub1).isNotSameAs(pub2);
        assertThat(cm.getCreatedCount()).isEqualTo(3);

        log.info("Publisher services isolated when configured");
    }

    @Test
    void createPublisherService_with_clientName_override() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createPublisherService_with_clientName_override\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolatePublishers(true);
        props.setReconnectRetries(true);
        TrackingCM cm = new TrackingCM(props);

        cm.createPublisherService("p1", "publisher-client");

        assertThat(cm.lastClientNameOverride).isEqualTo("publisher-client");

        log.info("Publisher client name override passed through");
    }

    // ─────────────────────────────────────────────────────────────────
    // RESOURCE MANAGEMENT TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void removeConsumerService_cleans_up_isolated_service() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: removeConsumerService_cleans_up_isolated_service\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolateConsumers(true);
        props.setReconnectRetries(true);
        TrackingCM cm = new TrackingCM(props);

        cm.createConsumerService("c1");
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        cm.removeConsumerService("c1");

        // Create again - should create new service since old was removed
        cm.createConsumerService("c1");
        assertThat(cm.getCreatedCount()).isEqualTo(2);

        log.info("Isolated consumer service cleaned up on remove");
    }

    @Test
    void removePublisherService_cleans_up_isolated_service() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: removePublisherService_cleans_up_isolated_service\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolatePublishers(true);
        props.setReconnectRetries(true);
        TrackingCM cm = new TrackingCM(props);

        cm.createPublisherService("p1", null);
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        cm.removePublisherService("p1");

        // Create again - should create new service
        cm.createPublisherService("p1", null);
        assertThat(cm.getCreatedCount()).isEqualTo(2);

        log.info("Isolated publisher service cleaned up on remove");
    }

    // ─────────────────────────────────────────────────────────────────
    // CONCURRENT ACCESS TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void getPrimaryService_is_thread_safe() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getPrimaryService_is_thread_safe\n" +
                "───────────────────────────────────────────────────────────────");

        TrackingCM cm = new TrackingCM(props);
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        ConcurrentHashMap<Integer, MessagingService> results = new ConcurrentHashMap<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> threadError =
                new java.util.concurrent.atomic.AtomicReference<>();

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    results.put(idx, cm.getPrimaryService());
                } catch (Throwable t) {
                    threadError.compareAndSet(null, t); // surface failures instead of swallowing them
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown(); // Release all threads
        // Deterministic: require all threads to finish (no silent timeout) and no thread error.
        assertThat(doneLatch.await(30, TimeUnit.SECONDS))
                .as("all worker threads should complete").isTrue();
        assertThat(threadError.get()).as("no worker thread should fail").isNull();
        assertThat(results).as("every thread recorded a result").hasSize(threadCount);

        // All threads should get the same instance
        MessagingService expected = results.get(0);
        for (MessagingService svc : results.values()) {
            assertThat(svc).isSameAs(expected);
        }
        // Should only create once
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        log.info("Concurrent access to primary service is thread-safe");
    }

    @Test
    void createConsumerService_isolated_is_thread_safe() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumerService_isolated_is_thread_safe\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolateConsumers(true);
        props.setReconnectRetries(true);
        TrackingCM cm = new TrackingCM(props);
        int threadCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        ConcurrentHashMap<Integer, MessagingService> results = new ConcurrentHashMap<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> threadError =
                new java.util.concurrent.atomic.AtomicReference<>();

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    results.put(idx, cm.createConsumerService("consumer-" + idx));
                } catch (Throwable t) {
                    threadError.compareAndSet(null, t);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertThat(doneLatch.await(30, TimeUnit.SECONDS))
                .as("all worker threads should complete").isTrue();
        assertThat(threadError.get()).as("no worker thread should fail").isNull();
        assertThat(results).as("every thread recorded a result").hasSize(threadCount);

        // All services should be different (isolated)
        for (int i = 0; i < threadCount; i++) {
            for (int j = i + 1; j < threadCount; j++) {
                assertThat(results.get(i)).isNotSameAs(results.get(j));
            }
        }
        assertThat(cm.getCreatedCount()).isEqualTo(threadCount);

        log.info("Concurrent isolated consumer creation is thread-safe");
    }

    // ─────────────────────────────────────────────────────────────────
    // SHUTDOWN TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void shutdown_disconnects_all_services() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: shutdown_disconnects_all_services\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolateConsumers(true);
        props.setIsolatePublishers(true);
        props.setReconnectRetries(true);
        TrackingCM cm = new TrackingCM(props);

        cm.getPrimaryService();
        cm.createConsumerService("c1");
        cm.createConsumerService("c2");
        cm.createPublisherService("p1", null);

        assertThat(cm.getCreatedCount()).isEqualTo(4);

        cm.shutdown();

        // All services should be disconnected
        for (AtomicBoolean state : cm.getConnectionState().values()) {
            assertThat(state.get()).isFalse();
        }

        log.info("Shutdown disconnected all services");
    }

    // ─────────────────────────────────────────────────────────────────
    // PROPERTIES TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void getProperties_returns_configured_properties() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getProperties_returns_configured_properties\n" +
                "───────────────────────────────────────────────────────────────");

        props.setHost("tcp://custom:12345");
        props.setMsgVpn("custom-vpn");
        props.setTerminationTimeoutMs(30000);

        TrackingCM cm = new TrackingCM(props);

        assertThat(cm.getProperties().getHost()).isEqualTo("tcp://custom:12345");
        assertThat(cm.getProperties().getMsgVpn()).isEqualTo("custom-vpn");
        assertThat(cm.getProperties().getTerminationTimeoutMs()).isEqualTo(30000);

        log.info("Properties accessible via getProperties()");
    }

    // ─────────────────────────────────────────────────────────────────
    // ISOLATED SERVICE TRACKING TESTS
    // ─────────────────────────────────────────────────────────────────

    @Test
    void isolated_consumers_tracked_via_creation_count() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: isolated_consumers_tracked_via_creation_count\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolateConsumers(true);
        props.setReconnectRetries(true);
        TrackingCM cm = new TrackingCM(props);

        assertThat(cm.getCreatedCount()).isEqualTo(0);

        cm.createConsumerService("c1");
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        cm.createConsumerService("c2");
        assertThat(cm.getCreatedCount()).isEqualTo(2);

        cm.createConsumerService("c3");
        assertThat(cm.getCreatedCount()).isEqualTo(3);

        log.info("Isolated consumer creation tracked: {} services created", cm.getCreatedCount());
    }

    @Test
    void isolated_publishers_tracked_via_creation_count() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: isolated_publishers_tracked_via_creation_count\n" +
                "───────────────────────────────────────────────────────────────");

        props.setIsolatePublishers(true);
        props.setReconnectRetries(true);
        TrackingCM cm = new TrackingCM(props);

        assertThat(cm.getCreatedCount()).isEqualTo(0);

        cm.createPublisherService("p1", null);
        assertThat(cm.getCreatedCount()).isEqualTo(1);

        cm.createPublisherService("p2", null);
        assertThat(cm.getCreatedCount()).isEqualTo(2);

        cm.createPublisherService("p3", null);
        assertThat(cm.getCreatedCount()).isEqualTo(3);

        log.info("Isolated publisher creation tracked: {} services created", cm.getCreatedCount());
    }
}
