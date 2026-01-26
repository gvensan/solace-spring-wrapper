package com.solace.wrapper.consumer;

import com.solace.messaging.MessagingService;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SolaceConsumerManagerTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceConsumerManagerTest.class);

    static class Env {
        volatile PersistentMessageReceiver persistentReceiver;
        volatile DirectMessageReceiver directReceiver;
        volatile int persistentStarted = 0;
        volatile int directStarted = 0;

        MessagingService service() {
            return (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return true;
                            case "createPersistentMessageReceiverBuilder":
                                // dynamic builder using return type
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{m.getReturnType()}, (bp, bm, ba) -> {
                                    String n = bm.getName();
                                    if ("withMissingResourcesCreationStrategy".equals(n) ||
                                        "withMessageClientAcknowledgement".equals(n) ||
                                        "withMessageAutoAcknowledgement".equals(n) ||
                                        "withSubscriptions".equals(n)) return bp; // fluent
                                    if ("build".equals(n)) {
                                        persistentReceiver = (PersistentMessageReceiver) Proxy.newProxyInstance(
                                                getClass().getClassLoader(), new Class[]{PersistentMessageReceiver.class}, (rp, rm, ra) -> {
                                                    switch (rm.getName()) {
                                                        case "start": persistentStarted++; return null;
                                                        case "receiveAsync": return null;
                                                        case "terminate": return null;
                                                        case "setReceiveFailureListener": return null;
                                                        default: return null;
                                                    }
                                                });
                                        return persistentReceiver;
                                    }
                                    return null;
                                });
                            case "createDirectMessageReceiverBuilder":
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{m.getReturnType()}, (bp, bm, ba) -> {
                                    String n = bm.getName();
                                    if ("withSubscriptions".equals(n)) return bp;
                                    if ("build".equals(n)) {
                                        directReceiver = (DirectMessageReceiver) Proxy.newProxyInstance(
                                                getClass().getClassLoader(), new Class[]{DirectMessageReceiver.class}, (rp, rm, ra) -> {
                                                    switch (rm.getName()) {
                                                        case "start": directStarted++; return null;
                                                        case "receiveAsync": return null;
                                                        case "terminate": return null;
                                                        default: return null;
                                                    }
                                                });
                                        return directReceiver;
                                    }
                                    return null;
                                });
                        }
                        if (m.getReturnType().equals(boolean.class)) return false;
                        if (m.getReturnType().equals(int.class)) return 0;
                        return null;
                    });
        }
    }

    static class NoopCM extends SolaceConnectionManager {
        static volatile Env CURRENT;
        NoopCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
        @Override public MessagingService createMessagingService() { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String consumerId) { return CURRENT.service(); }
    }

    static class StubSerializer implements MessageSerializer {
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService messagingService, Object object) { return null; }
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService messagingService, Object object, Object destination) { return null; }
        @Override public byte[] serializeToBytes(MessagingService messagingService, Object object) {
            if (object instanceof byte[]) return (byte[]) object;
            return object != null ? object.toString().getBytes(StandardCharsets.UTF_8) : new byte[0];
        }
        @Override public <T> T deserialize(com.solace.messaging.receiver.InboundMessage message, Class<T> targetType) { return null; }
        @Override public String deserializeToString(com.solace.messaging.receiver.InboundMessage message) { return null; }
    }

    private Env env;
    private SolaceConsumerManager mgr;

    @BeforeEach
    void setup() {
        env = new Env();
        NoopCM.CURRENT = env;
        var props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555"); props.setMsgVpn("default"); props.setClientUsername("default"); props.setClientPassword("");
        SolaceConnectionManager cm = new NoopCM(props);
        mgr = new SolaceConsumerManager(cm, new StubSerializer());
    }

    @Test
    void create_consumer_generates_id_starts_and_status_reflects() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: create_consumer_generates_id_starts_and_status_reflects\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that SolaceConsumerManager correctly generates consumer IDs,\n" +
                "  auto-starts consumers, and maintains accurate status tracking.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Auto-generated consumer ID with queue name prefix\n" +
                "  2. Automatic consumer startup on creation (autoStart=true default)\n" +
                "  3. hasConsumer() and isConsumerRunning() status checks\n" +
                "  4. Consumer count tracking (total and active)\n" +
                "  5. ConsumerStatus metadata (queueName, messageType)\n" +
                "  6. Lookup by queue and message type\n" +
                "\n" +
                "CONSUMER MANAGER RESPONSIBILITIES:\n" +
                "  - Registry of all consumers in the application\n" +
                "  - Lifecycle management (start, stop, restart)\n" +
                "  - Status reporting for monitoring and health checks\n" +
                "  - Lookup capabilities for operational tools\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create consumer with auto-generated ID\n" +
                "  2. Verify ID format and consumer is running\n" +
                "  3. Verify counts and status metadata\n" +
                "  4. Verify lookup by queue and message type\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - ID starts with 'q.mgr1-consumer-'\n" +
                "  - hasConsumer=true, isRunning=true\n" +
                "  - totalCount=1, activeCount=1\n" +
                "  - Status contains correct queueName and messageType\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating consumer for queue 'q.mgr1' (auto-generated ID)");
        String id = mgr.createConsumer("q.mgr1", String.class, (msg, in) -> {});

        log.info("STEP 2: Verifying consumer ID was generated with expected prefix");
        assertThat(id).startsWith("q.mgr1-consumer-");

        log.info("STEP 3: Verifying consumer exists and is running");
        assertThat(mgr.hasConsumer(id)).isTrue();
        assertThat(mgr.isConsumerRunning(id)).isTrue();

        log.info("STEP 4: Verifying consumer counts");
        assertThat(mgr.getTotalConsumerCount()).isEqualTo(1);
        assertThat(mgr.getActiveConsumerCount()).isEqualTo(1);

        log.info("STEP 5: Verifying ConsumerStatus contains correct metadata");
        SolaceConsumerManager.ConsumerStatus st = mgr.getConsumerStatus(id);
        assertThat(st).isNotNull();
        assertThat(st.getQueueName()).isEqualTo("q.mgr1");
        assertThat(st.getMessageType()).isEqualTo("String");

        log.info("STEP 6: Verifying lookup by queue and message type");
        assertThat(mgr.getConsumersByQueue("q.mgr1")).containsExactly(id);
        assertThat(mgr.getConsumersByMessageType(String.class)).containsExactly(id);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Consumer ID: '" + id + "' (expected: starts with 'q.mgr1-consumer-')\n" +
                "  hasConsumer: " + mgr.hasConsumer(id) + " (expected: true)\n" +
                "  isRunning: " + mgr.isConsumerRunning(id) + " (expected: true)\n" +
                "  totalCount: " + mgr.getTotalConsumerCount() + " (expected: 1)\n" +
                "  activeCount: " + mgr.getActiveConsumerCount() + " (expected: 1)\n" +
                "  queueName: '" + st.getQueueName() + "' (expected: 'q.mgr1')\n" +
                "  messageType: '" + st.getMessageType() + "' (expected: 'String')\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Consumer ID generated with queue name prefix + unique suffix\n" +
                "  - Consumer auto-started on creation (default behavior)\n" +
                "  - Status tracking accurate for monitoring\n" +
                "  - Lookup methods enable operational tooling\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void create_with_id_and_raw_and_restart_and_stop_start_shutdown() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: create_with_id_and_raw_and_restart_and_stop_start_shutdown\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Comprehensive test of consumer lifecycle management including explicit\n" +
                "  IDs, duplicate rejection, raw API, restart, bulk operations, and shutdown.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Explicit consumer ID assignment\n" +
                "  2. Duplicate ID rejection (throws SolaceConsumerException)\n" +
                "  3. Raw API consumer creation (createConsumerRaw)\n" +
                "  4. Individual consumer restart (restartConsumer)\n" +
                "  5. Bulk stop (stopAllConsumers)\n" +
                "  6. Bulk start (startAllConsumers)\n" +
                "  7. Manager shutdown (clears all consumers)\n" +
                "\n" +
                "LIFECYCLE MANAGEMENT USE CASES:\n" +
                "  - Explicit IDs: Predictable naming for monitoring/logging\n" +
                "  - Restart: Recovery from transient errors\n" +
                "  - Stop/Start All: Maintenance windows, connection cycling\n" +
                "  - Shutdown: Graceful application termination\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create consumer with explicit ID 'cid-1'\n" +
                "  2. Attempt duplicate - should throw\n" +
                "  3. Create second consumer with raw API\n" +
                "  4. Restart first consumer\n" +
                "  5. Stop all consumers (activeCount=0)\n" +
                "  6. Start all consumers (activeCount=2)\n" +
                "  7. Shutdown manager (totalCount=0)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - All lifecycle operations complete successfully\n" +
                "  - Counts reflect current state at each step\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating consumer with explicit ID 'cid-1'");
        String id = mgr.createConsumer("cid-1", "q.mgr2", String.class, (msg, in) -> {});
        assertThat(id).isEqualTo("cid-1");

        log.info("STEP 2: Attempting to create duplicate consumer with same ID - should throw");
        // Duplicate should throw
        assertThatThrownBy(() -> mgr.createConsumer("cid-1", "q.mgr2", String.class, (msg, in) -> {})).isInstanceOf(com.solace.wrapper.exception.SolaceConsumerException.class);
        log.info("        Duplicate ID rejection works correctly");

        log.info("STEP 3: Creating consumer using raw API with explicit ID 'cid-2'");
        // Raw
        String id2 = mgr.createConsumerRaw("cid-2", "q.mgr3", Integer.class, (SolaceMessageHandler<?>) (m, in) -> {});
        assertThat(mgr.hasConsumer(id2)).isTrue();

        log.info("STEP 4: Testing restartConsumer() on 'cid-1'");
        // restart
        mgr.restartConsumer(id);
        assertThat(mgr.isConsumerRunning(id)).isTrue();

        log.info("STEP 5: Testing stopAllConsumers() - all consumers should stop");
        // stop all, then start all
        mgr.stopAllConsumers();
        assertThat(mgr.getActiveConsumerCount()).isEqualTo(0);
        log.info("        activeConsumerCount=" + mgr.getActiveConsumerCount() + " (all stopped)");

        log.info("STEP 6: Testing startAllConsumers() - all consumers should restart");
        mgr.startAllConsumers();
        assertThat(mgr.getActiveConsumerCount()).isEqualTo(2);
        log.info("        activeConsumerCount=" + mgr.getActiveConsumerCount() + " (all restarted)");

        log.info("STEP 7: Verifying getConsumersByQueue lookup");
        // all statuses
        List<String> ids = mgr.getConsumersByQueue("q.mgr2");
        assertThat(ids).contains("cid-1");

        log.info("STEP 8: Testing shutdown() - should clear all consumers");
        // shutdown clears
        mgr.shutdown();
        assertThat(mgr.getTotalConsumerCount()).isEqualTo(0);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Explicit ID creation: 'cid-1' created successfully\n" +
                "  Duplicate rejection: SolaceConsumerException thrown (as expected)\n" +
                "  Raw API creation: 'cid-2' created successfully\n" +
                "  Restart: Consumer 'cid-1' restarted, isRunning=true\n" +
                "  Stop all: activeCount=0 (all stopped)\n" +
                "  Start all: activeCount=2 (all restarted)\n" +
                "  Shutdown: totalCount=0 (all cleared)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Explicit IDs work for predictable consumer naming\n" +
                "  - Duplicate IDs are properly rejected with exception\n" +
                "  - Lifecycle operations (restart, stop, start) function correctly\n" +
                "  - Shutdown cleanly removes all consumers\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications can use explicit IDs for monitoring integration\n" +
                "  - Bulk operations enable maintenance scenarios\n" +
                "  - Clean shutdown ensures graceful application termination\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}
