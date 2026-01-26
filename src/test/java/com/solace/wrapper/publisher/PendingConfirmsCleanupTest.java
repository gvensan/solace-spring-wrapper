package com.solace.wrapper.publisher;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.config.SolaceProperties;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the pendingConfirms cleanup mechanism in SolacePublisher.
 * Verifies that stale pending confirmations are cleaned up and completed exceptionally.
 */
public class PendingConfirmsCleanupTest {

    private static final Logger log = LoggerFactory.getLogger(PendingConfirmsCleanupTest.class);

    private SolacePublisher publisher;
    private SolaceProperties props;

    @BeforeEach
    void setUp() {
        props = new SolaceProperties();
        props.setHost("tcp://noop:55555");
        props.setMsgVpn("default");
        props.setClientUsername("default");
        props.setClientPassword("");
        // Set a very short timeout for testing
        props.setPendingConfirmTimeoutMs(100);

        NoopConnectionManager cm = new NoopConnectionManager(props);
        NoopSerializer serializer = new NoopSerializer();
        publisher = new SolacePublisher(cm, serializer);
    }

    @Test
    void pendingConfirmTimeoutMs_is_configurable() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: pendingConfirmTimeoutMs_is_configurable\n" +
                "PURPOSE: Verify pendingConfirmTimeoutMs property is configurable with sensible default\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Verifying configured timeout (100ms for testing)");
        assertThat(props.getPendingConfirmTimeoutMs()).isEqualTo(100);

        log.info("STEP 2: Verifying default timeout is 30000ms (30 seconds)");
        SolaceProperties defaultProps = new SolaceProperties();
        assertThat(defaultProps.getPendingConfirmTimeoutMs()).isEqualTo(30000); // default 30 seconds

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: pendingConfirmTimeoutMs is configurable (test=100ms, default=30000ms)\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void cleanupStalePendingConfirms_removes_old_entries() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: cleanupStalePendingConfirms_removes_old_entries\n" +
                "PURPOSE: Verify stale pending confirms are cleaned up and completed exceptionally after timeout\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Accessing private pendingConfirms map via reflection");
        // Access the private pendingConfirms map
        Field pendingConfirmsField = SolacePublisher.class.getDeclaredField("pendingConfirms");
        pendingConfirmsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> pendingConfirms = (Map<String, Object>) pendingConfirmsField.get(publisher);

        log.info("STEP 2: Creating a test PendingConfirm and adding to map");
        // Create a test PendingConfirm
        Class<?> pendingConfirmClass = Class.forName("com.solace.wrapper.publisher.SolacePublisher$PendingConfirm");
        var constructor = pendingConfirmClass.getDeclaredConstructor(CompletableFuture.class);
        constructor.setAccessible(true);

        CompletableFuture<Void> staleFuture = new CompletableFuture<>();
        Object stalePendingConfirm = constructor.newInstance(staleFuture);

        // Put the entry in the map
        pendingConfirms.put("stale-id", stalePendingConfirm);

        log.info("STEP 3: Waiting 150ms for timeout to expire (configured timeout=100ms)");
        // Wait for the timeout to expire (100ms configured + buffer)
        Thread.sleep(150);

        log.info("STEP 4: Invoking cleanupStalePendingConfirms()");
        // Invoke the cleanup method
        Method cleanupMethod = SolacePublisher.class.getDeclaredMethod("cleanupStalePendingConfirms");
        cleanupMethod.setAccessible(true);
        cleanupMethod.invoke(publisher);

        log.info("STEP 5: Verifying stale entry was removed and future completed exceptionally");
        // Verify the stale entry was removed (the entry was created 150ms ago, timeout is 100ms)
        assertThat(pendingConfirms).doesNotContainKey("stale-id");

        // Verify the future was completed exceptionally with TimeoutException
        assertThat(staleFuture).isCompletedExceptionally();
        assertThatThrownBy(staleFuture::join)
            .hasCauseInstanceOf(TimeoutException.class)
            .hasMessageContaining("timed out");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Stale pending confirm cleaned up with TimeoutException\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void shutdown_completes_pending_confirms_exceptionally() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: shutdown_completes_pending_confirms_exceptionally\n" +
                "PURPOSE: Verify shutdown() completes all pending confirms exceptionally and clears the map\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Accessing private pendingConfirms map and adding a pending confirm");
        // Access the private pendingConfirms map
        Field pendingConfirmsField = SolacePublisher.class.getDeclaredField("pendingConfirms");
        pendingConfirmsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> pendingConfirms = (Map<String, Object>) pendingConfirmsField.get(publisher);

        // Create a pending confirm
        Class<?> pendingConfirmClass = Class.forName("com.solace.wrapper.publisher.SolacePublisher$PendingConfirm");
        var constructor = pendingConfirmClass.getDeclaredConstructor(CompletableFuture.class);
        constructor.setAccessible(true);

        CompletableFuture<Void> pendingFuture = new CompletableFuture<>();
        Object pendingConfirm = constructor.newInstance(pendingFuture);
        pendingConfirms.put("pending-id", pendingConfirm);

        assertThat(pendingConfirms).hasSize(1);
        log.info("        pendingConfirms size before shutdown: " + pendingConfirms.size());

        log.info("STEP 2: Calling publisher.shutdown()");
        // Shutdown the publisher
        publisher.shutdown();

        log.info("STEP 3: Verifying pending future completed exceptionally with 'shutting down' message");
        // Verify all pending confirms are completed exceptionally
        assertThat(pendingFuture).isCompletedExceptionally();
        assertThatThrownBy(pendingFuture::join)
            .hasMessageContaining("shutting down");

        log.info("STEP 4: Verifying pendingConfirms map is cleared");
        // Verify the map is cleared
        assertThat(pendingConfirms).isEmpty();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Shutdown completed pending confirms exceptionally and cleared map\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void pendingConfirm_wrapper_stores_creation_timestamp() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: pendingConfirm_wrapper_stores_creation_timestamp\n" +
                "PURPOSE: Verify PendingConfirm inner class stores creation timestamp for timeout tracking\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Creating PendingConfirm via reflection and capturing timestamps");
        Class<?> pendingConfirmClass = Class.forName("com.solace.wrapper.publisher.SolacePublisher$PendingConfirm");
        var constructor = pendingConfirmClass.getDeclaredConstructor(CompletableFuture.class);
        constructor.setAccessible(true);

        long before = System.currentTimeMillis();
        CompletableFuture<Void> future = new CompletableFuture<>();
        Object pendingConfirm = constructor.newInstance(future);
        long after = System.currentTimeMillis();

        log.info("STEP 2: Verifying createdAt timestamp is within expected range");
        // Access the createdAt field
        Field createdAtField = pendingConfirmClass.getDeclaredField("createdAt");
        createdAtField.setAccessible(true);
        long createdAt = createdAtField.getLong(pendingConfirm);

        assertThat(createdAt).isBetween(before, after);
        log.info("        createdAt=" + createdAt + " (between " + before + " and " + after + ")");

        log.info("STEP 3: Verifying stored future is the same instance");
        // Access the future field
        Field futureField = pendingConfirmClass.getDeclaredField("future");
        futureField.setAccessible(true);
        @SuppressWarnings("unchecked")
        CompletableFuture<Void> storedFuture = (CompletableFuture<Void>) futureField.get(pendingConfirm);

        assertThat(storedFuture).isSameAs(future);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: PendingConfirm correctly stores creation timestamp and future\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    /**
     * No-op connection manager for testing.
     */
    static class NoopConnectionManager extends SolaceConnectionManager {
        NoopConnectionManager(SolaceProperties props) {
            super(props);
        }

        @Override
        public MessagingService createMessagingService() {
            return createDummyService();
        }

        private MessagingService createDummyService() {
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
                        case "createDirectMessagePublisherBuilder":
                        case "createPersistentMessagePublisherBuilder":
                            return createDummyBuilder(method.getReturnType());
                        default: return null;
                    }
                }
            );
        }

        private Object createDummyBuilder(Class<?> builderType) {
            return Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{builderType},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("build".equals(name)) {
                        return createDummyPublisher(method.getReturnType());
                    }
                    return proxy; // fluent methods return self
                }
            );
        }

        private Object createDummyPublisher(Class<?> publisherType) {
            return Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{publisherType},
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "start":
                        case "terminate":
                            return null;
                        case "setMessagePublishReceiptListener":
                            throw new UnsupportedOperationException("no receipts");
                        default:
                            return null;
                    }
                }
            );
        }
    }

    /**
     * No-op serializer for testing.
     */
    static class NoopSerializer implements MessageSerializer {
        @Override
        public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService svc, Object obj) {
            return null;
        }

        @Override
        public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService svc, Object obj, Object dest) {
            return null;
        }

        @Override
        public byte[] serializeToBytes(MessagingService svc, Object obj) {
            return new byte[0];
        }

        @Override
        public <T> T deserialize(com.solace.messaging.receiver.InboundMessage msg, Class<T> type) {
            return null;
        }

        @Override
        public String deserializeToString(com.solace.messaging.receiver.InboundMessage msg) {
            return null;
        }
    }
}
