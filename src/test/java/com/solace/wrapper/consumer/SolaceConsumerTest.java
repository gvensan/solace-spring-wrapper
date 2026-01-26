package com.solace.wrapper.consumer;

import com.solace.messaging.MessagingService;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.serialization.MessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class SolaceConsumerTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceConsumerTest.class);

    static volatile Env CURRENT;

    static class Env {
        volatile PersistentMessageReceiver persistentReceiver;
        volatile DirectMessageReceiver directReceiver;
        final AtomicInteger ackCount = new AtomicInteger();
        final AtomicInteger settleCount = new AtomicInteger();
        final AtomicInteger directStart = new AtomicInteger();
        final AtomicInteger persistentStart = new AtomicInteger();

        MessagingService service() {
            return (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (p, m, a) -> {
                        switch (m.getName()) {
                            case "connect": return p;
                            case "isConnected": return true;
                            case "createPersistentMessageReceiverBuilder":
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{m.getReturnType()}, (bp, bm, ba) -> {
                                    String n = bm.getName();
                                    if ("withMissingResourcesCreationStrategy".equals(n) ||
                                        "withMessageClientAcknowledgement".equals(n) ||
                                        "withMessageAutoAcknowledgement".equals(n) ||
                                        "withSubscriptions".equals(n)) {
                                        return bp; // fluent
                                    }
                                    if ("build".equals(n)) {
                                        Env.this.persistentReceiver = (PersistentMessageReceiver) Proxy.newProxyInstance(
                                                getClass().getClassLoader(), new Class[]{PersistentMessageReceiver.class}, (rp, rm, ra) -> {
                                                    switch (rm.getName()) {
                                                        case "start": persistentStart.incrementAndGet(); return null;
                                                        case "ack": ackCount.incrementAndGet(); return null;
                                                        case "settle": settleCount.incrementAndGet(); return null;
                                                        case "terminate": return null;
                                                        case "receiveAsync": return null;
                                                        case "setReceiveFailureListener": return null;
                                                        default: return null;
                                                    }
                                                });
                                        return Env.this.persistentReceiver;
                                    }
                                    return null;
                                });
                            case "createDirectMessageReceiverBuilder":
                                // Minimal builder with withSubscriptions + build
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{m.getReturnType()}, (bp, bm, ba) -> {
                                    String n = bm.getName();
                                    if ("withSubscriptions".equals(n)) return bp;
                                    if ("build".equals(n)) {
                                        Env.this.directReceiver = (DirectMessageReceiver) Proxy.newProxyInstance(
                                                getClass().getClassLoader(), new Class[]{DirectMessageReceiver.class}, (rp, rm, ra) -> {
                                                    switch (rm.getName()) {
                                                        case "start": directStart.incrementAndGet(); return null;
                                                        case "receiveAsync": return null;
                                                        case "terminate": return null;
                                                        default: return null;
                                                    }
                                                });
                                        return Env.this.directReceiver;
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
        NoopCM(com.solace.wrapper.config.SolaceProperties p, Env env) { super(p); }
        @Override public MessagingService createMessagingService() { return CURRENT.service(); }
        @Override public MessagingService createConsumerService(String consumerId) { return CURRENT.service(); }
    }

    static class StubSerializer implements MessageSerializer {
        final Object deserialized;
        StubSerializer(Object d) { this.deserialized = d; }
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService messagingService, Object object) { return null; }
        @Override public com.solace.messaging.publisher.OutboundMessage serialize(MessagingService messagingService, Object object, Object destination) { return null; }
        @Override public byte[] serializeToBytes(MessagingService messagingService, Object object) {
            if (object instanceof byte[]) return (byte[]) object;
            return object != null ? object.toString().getBytes(StandardCharsets.UTF_8) : new byte[0];
        }
        @SuppressWarnings("unchecked")
        @Override public <T> T deserialize(InboundMessage message, Class<T> targetType) { return (T) deserialized; }
        @Override public String deserializeToString(InboundMessage message) { return null; }
    }

    private Env env;
    private SolaceConnectionManager cm;

    @BeforeEach
    void init() {
        env = new Env();
        CURRENT = env;
        var props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555"); props.setMsgVpn("default"); props.setClientUsername("default"); props.setClientPassword("");
        cm = new NoopCM(props, env);
    }

    private static InboundMessage inbound(String s) {
        return (InboundMessage) Proxy.newProxyInstance(
                SolaceConsumerTest.class.getClassLoader(), new Class[]{InboundMessage.class}, (p, m, a) -> {
                    if ("getPayloadAsString".equals(m.getName())) return s;
                    if ("getPayloadAsBytes".equals(m.getName())) return s.getBytes(StandardCharsets.UTF_8);
                    return null;
                });
    }

    @Test
    void persistent_manual_ack_success_acknowledges() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_manual_ack_success_acknowledges\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that when a message handler completes successfully in MANUAL\n" +
                "  ack mode, the framework sends an ACK (not NACK) to the broker.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. MANUAL ack mode with successful handler\n" +
                "  2. Framework-managed ACK after handler completion\n" +
                "  3. No NACK/settle when handler succeeds\n" +
                "\n" +
                "MANUAL ACK FLOW:\n" +
                "  1. Message received from queue\n" +
                "  2. Handler invoked with deserialized message\n" +
                "  3. Handler completes successfully (no exception)\n" +
                "  4. Framework calls receiver.ack() → message removed from queue\n" +
                "\n" +
                "WHY MANUAL ACK MATTERS:\n" +
                "  - Message stays in queue until explicitly ACKed\n" +
                "  - If consumer crashes before ACK, message is redelivered\n" +
                "  - Provides exactly-once-like processing semantics\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create consumer with MANUAL ack mode\n" +
                "  2. Simulate message receipt with successful handler\n" +
                "  3. Verify ack() was called, settle() was NOT called\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Handler invoked once\n" +
                "  - ackCount = 1 (ACK sent)\n" +
                "  - settleCount = 0 (no NACK)\n" +
                "───────────────────────────────────────────────────────────────\n");

        AtomicInteger handled = new AtomicInteger();
        SolaceMessageHandler<String> handler = (msg, in) -> handled.incrementAndGet();

        log.info("STEP 1: Creating persistent consumer with MANUAL ack mode on queue 'q1'");
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c1", "q1", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer("x")
        );
        cons.start();

        log.info("STEP 2: Simulating message receipt - handler will succeed");
        cons.onMessage(inbound("m1"));

        log.info("STEP 3: Verifying handler was called and ACK was sent (not NACK/settle)");
        assertThat(handled.get()).isEqualTo(1);
        assertThat(env.ackCount.get()).isEqualTo(1);
        assertThat(env.settleCount.get()).isEqualTo(0);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Handler invocations: " + handled.get() + " (expected: 1)\n" +
                "  ackCount: " + env.ackCount.get() + " (expected: 1)\n" +
                "  settleCount: " + env.settleCount.get() + " (expected: 0)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Handler processed message successfully\n" +
                "  - Framework sent ACK to broker after handler completion\n" +
                "  - No NACK was sent (settleCount=0)\n" +
                "  - Message is now removed from queue\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Successful processing results in message acknowledgment\n" +
                "  - Queue depth decreases by 1\n" +
                "  - Message will not be redelivered\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void persistent_manual_ack_failure_nacks() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_manual_ack_failure_nacks\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that when a message handler fails (throws exception) in MANUAL\n" +
                "  ack mode and retries are exhausted, the framework sends NACK (settle).\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. MANUAL ack mode with failing handler\n" +
                "  2. Local retry exhaustion\n" +
                "  3. NACK (settle) after all retries fail\n" +
                "\n" +
                "MANUAL ACK FAILURE FLOW:\n" +
                "  1. Message received from queue\n" +
                "  2. Handler throws exception\n" +
                "  3. Local retry (if configured) - also fails\n" +
                "  4. Retries exhausted → framework calls settle() (NACK)\n" +
                "  5. Message goes to DMQ or redelivers based on broker config\n" +
                "\n" +
                "WHY NACK MATTERS:\n" +
                "  - Prevents infinite reprocessing of poison messages\n" +
                "  - Allows dead-letter queue routing\n" +
                "  - Application can handle failed messages separately\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create handler that always throws\n" +
                "  2. Create consumer with maxAttempts=1 (no local retry)\n" +
                "  3. Simulate message receipt - handler fails immediately\n" +
                "  4. Verify settle() (NACK) was called, ack() was NOT\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - ackCount = 0 (no ACK)\n" +
                "  - settleCount = 1 (NACK sent)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating handler that always throws RuntimeException");
        SolaceMessageHandler<String> handler = (msg, in) -> { throw new RuntimeException("boom"); };

        log.info("STEP 2: Creating persistent consumer with MANUAL ack mode and maxAttempts=1");
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c2", "q2", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer("x")
        ).withLocalBackoff(1, 0, 1.0, 0);
        cons.start();

        log.info("STEP 3: Simulating message receipt - handler will fail and exhaust retries");
        cons.onMessage(inbound("m2"));

        log.info("STEP 4: Verifying NO ack was sent, but SETTLE (NACK) was called");
        assertThat(env.ackCount.get()).isEqualTo(0);
        assertThat(env.settleCount.get()).isEqualTo(1);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  ackCount: " + env.ackCount.get() + " (expected: 0)\n" +
                "  settleCount: " + env.settleCount.get() + " (expected: 1)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Handler threw RuntimeException('boom')\n" +
                "  - maxAttempts=1 means no local retry (immediate give up)\n" +
                "  - Framework sent NACK (settle) instead of ACK\n" +
                "  - Message will be redelivered or sent to DMQ\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Poison messages don't block queue processing\n" +
                "  - Broker handles redelivery/DMQ based on configuration\n" +
                "  - Application can monitor settle count for failures\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void direct_mode_success_no_ack() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: direct_mode_success_no_ack\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that DIRECT messaging mode (topic subscription without queue)\n" +
                "  does not use ACK/NACK since there's no persistence or guaranteed delivery.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. DIRECT messaging mode (topic-only, no queue)\n" +
                "  2. Best-effort delivery semantics\n" +
                "  3. No ACK/NACK for direct messages\n" +
                "\n" +
                "DIRECT vs PERSISTENT:\n" +
                "  - DIRECT: Fire-and-forget, no persistence, lowest latency\n" +
                "    • No queue = no message storage\n" +
                "    • No ACK needed = messages are consumed immediately or lost\n" +
                "  - PERSISTENT: Queue-based, guaranteed delivery, requires ACK\n" +
                "\n" +
                "WHY NO ACK FOR DIRECT:\n" +
                "  - Direct messages have no queue to ack against\n" +
                "  - Message lifetime is instant (deliver or discard)\n" +
                "  - Acknowledgment is meaningless without persistence\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create DIRECT mode consumer (topic subscription only)\n" +
                "  2. Simulate message receipt\n" +
                "  3. Verify handler invoked but NO ACK/NACK sent\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Handler invoked once\n" +
                "  - ackCount = 0, settleCount = 0 (no acknowledgment for direct)\n" +
                "───────────────────────────────────────────────────────────────\n");

        AtomicInteger handled = new AtomicInteger();
        SolaceMessageHandler<String> handler = (msg, in) -> handled.incrementAndGet();

        log.info("STEP 1: Creating DIRECT mode consumer (no queue, topic subscription only)");
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c3", "", new String[]{"a/>"}, SolaceConsumer.MessagingMode.DIRECT, false,
                SolaceConsumer.AckMode.AUTO, String.class, handler, cm, new StubSerializer("x")
        );
        cons.start();

        log.info("STEP 2: Simulating message receipt");
        cons.onMessage(inbound("m3"));

        log.info("STEP 3: Verifying handler was called but NO ACK/NACK was sent (direct mode)");
        assertThat(handled.get()).isEqualTo(1);
        assertThat(env.ackCount.get()).isEqualTo(0);
        assertThat(env.settleCount.get()).isEqualTo(0);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Handler invocations: " + handled.get() + " (expected: 1)\n" +
                "  ackCount: " + env.ackCount.get() + " (expected: 0)\n" +
                "  settleCount: " + env.settleCount.get() + " (expected: 0)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Direct mode consumer processed message successfully\n" +
                "  - No ACK/NACK sent (not applicable to direct messaging)\n" +
                "  - Message processing is fire-and-forget\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Direct mode is stateless from consumer perspective\n" +
                "  - No redelivery possible (message was consumed or discarded)\n" +
                "  - Use for real-time streaming where occasional loss is acceptable\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void local_backoff_retries_then_ack() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: local_backoff_retries_then_ack\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the local backoff retry mechanism allows a handler\n" +
                "  to fail and retry locally before sending ACK on eventual success.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Local retry with configurable maxAttempts\n" +
                "  2. Handler invoked multiple times for same message\n" +
                "  3. ACK sent only after successful attempt\n" +
                "\n" +
                "LOCAL RETRY vs BROKER REDELIVERY:\n" +
                "  - LOCAL RETRY: Same message retried in memory (faster)\n" +
                "    • No round-trip to broker\n" +
                "    • Configurable backoff delay between attempts\n" +
                "    • Suitable for transient errors (network blip, lock contention)\n" +
                "  - BROKER REDELIVERY: NACK causes broker to redeliver (slower)\n" +
                "    • Message goes back to queue\n" +
                "    • Other consumers might process it\n" +
                "    • Suitable for persistent failures\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create handler that fails on first call, succeeds on second\n" +
                "  2. Configure consumer with localMaxAttempts=2\n" +
                "  3. Simulate message receipt - first attempt fails, retry succeeds\n" +
                "  4. Verify handler invoked twice, ACK sent after success\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Handler invocations = 2 (fail + retry)\n" +
                "  - ackCount = 1 (ACK after successful retry)\n" +
                "  - settleCount = 0 (no NACK since retry succeeded)\n" +
                "───────────────────────────────────────────────────────────────\n");

        AtomicInteger handled = new AtomicInteger();
        log.info("STEP 1: Creating handler that fails on first attempt, succeeds on second");
        SolaceMessageHandler<String> handler = (msg, in) -> {
            if (handled.getAndIncrement() == 0) throw new RuntimeException("fail once");
        };

        log.info("STEP 2: Creating consumer with localMaxAttempts=2 (allows 1 retry)");
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c4", "q4", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer("x")
        ).withLocalBackoff(2, 0, 1.0, 0);
        cons.start();

        log.info("STEP 3: Simulating message receipt - first attempt fails, retry succeeds");
        cons.onMessage(inbound("m4"));

        log.info("STEP 4: Verifying handler was invoked twice (fail+retry) and ACK sent after success");
        assertThat(handled.get()).isEqualTo(2);
        assertThat(env.ackCount.get()).isEqualTo(1);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Handler invocations: " + handled.get() + " (expected: 2)\n" +
                "  ackCount: " + env.ackCount.get() + " (expected: 1)\n" +
                "  settleCount: " + env.settleCount.get() + " (expected: 0)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - First attempt: handler threw RuntimeException('fail once')\n" +
                "  - Local retry triggered (maxAttempts=2 allows 1 retry)\n" +
                "  - Second attempt: handler succeeded (no exception)\n" +
                "  - ACK sent after successful retry\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Transient errors handled locally without broker round-trip\n" +
                "  - Reduces redelivery count on broker\n" +
                "  - Configurable via @SolaceConsumer(localMaxAttempts=N)\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void termination_marks_not_running() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: termination_marks_not_running\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that when the underlying receiver terminates (broker disconnect,\n" +
                "  shutdown), the consumer correctly updates its running state.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Receiver termination callback handling\n" +
                "  2. Consumer running state management\n" +
                "  3. isRunning() accuracy after termination\n" +
                "\n" +
                "TERMINATION SCENARIOS:\n" +
                "  - Explicit shutdown() call on consumer\n" +
                "  - Broker connection lost\n" +
                "  - Application context shutdown\n" +
                "  - Network failure causing receiver termination\n" +
                "\n" +
                "WHY STATE TRACKING MATTERS:\n" +
                "  - Health checks need accurate running state\n" +
                "  - Restart logic depends on isRunning() being false\n" +
                "  - Monitoring dashboards show consumer status\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create and start consumer (isRunning=true)\n" +
                "  2. Simulate receiver termination callback\n" +
                "  3. Verify isRunning() returns false\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Before termination: isRunning = true\n" +
                "  - After termination: isRunning = false\n" +
                "───────────────────────────────────────────────────────────────\n");

        SolaceMessageHandler<String> handler = (msg, in) -> {};

        log.info("STEP 1: Creating and starting persistent consumer");
        SolaceConsumer<String> cons = new SolaceConsumer<>(
                "c5", "q5", new String[0], SolaceConsumer.MessagingMode.PERSISTENT, true,
                SolaceConsumer.AckMode.MANUAL, String.class, handler, cm, new StubSerializer("x")
        );
        cons.start();

        log.info("STEP 2: Verifying consumer is running after start()");
        assertThat(cons.isRunning()).isTrue();

        log.info("STEP 3: Simulating receiver termination callback");
        cons.onTermination(env.persistentReceiver);

        log.info("STEP 4: Verifying consumer is marked as NOT running after termination");
        assertThat(cons.isRunning()).isFalse();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  isRunning() after start: true (expected: true)\n" +
                "  isRunning() after termination: " + cons.isRunning() + " (expected: false)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Consumer started and entered running state\n" +
                "  - Termination callback was received\n" +
                "  - Consumer correctly transitioned to not-running state\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Health indicators will report consumer as stopped\n" +
                "  - Restart logic can detect need to restart\n" +
                "  - Monitoring can alert on unexpected terminations\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}
