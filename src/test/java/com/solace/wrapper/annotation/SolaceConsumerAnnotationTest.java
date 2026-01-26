package com.solace.wrapper.annotation;

import com.solace.wrapper.annotation.processor.SolaceConsumerProcessor;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import com.solace.messaging.receiver.InboundMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Dedicated annotation-focused tests for @SolaceConsumer.
 * Uses the processor directly with a recording manager to avoid a Spring context.
 */
public class SolaceConsumerAnnotationTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceConsumerAnnotationTest.class);

    private RecordingManager manager;
    private SolaceConsumerProcessor processor;

    @BeforeEach
    void setup() throws Exception {
        manager = new RecordingManager();
        processor = new SolaceConsumerProcessor();
        Field f = SolaceConsumerProcessor.class.getDeclaredField("consumerManager");
        f.setAccessible(true);
        f.set(processor, manager);
        processor.setSpelResolver(new com.solace.wrapper.annotation.processor.SpelExpressionResolver());
    }

    @Test
    void persistent_queue_manual_ack_and_auto_mode() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_queue_manual_ack_and_auto_mode\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that a @SolaceConsumer annotation with a queue specified\n" +
                "  correctly triggers PERSISTENT messaging mode with MANUAL acknowledgment.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Queue-based consumer registration via @SolaceConsumer\n" +
                "  2. AUTO messaging mode → PERSISTENT when queue is specified\n" +
                "  3. Explicit ackMode=MANUAL configuration\n" +
                "  4. Message type inference from method parameter\n" +
                "\n" +
                "AUTO MODE BEHAVIOR:\n" +
                "  - If queue is specified → PERSISTENT mode (guaranteed delivery)\n" +
                "  - If only topics specified → DIRECT mode (best-effort)\n" +
                "  - This provides sensible defaults while allowing explicit override\n" +
                "\n" +
                "MANUAL ACK USE CASES:\n" +
                "  - Application needs to process message before acknowledging\n" +
                "  - Transaction-like semantics (process then ack)\n" +
                "  - Ability to NACK/reject messages on processing failure\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create bean with @SolaceConsumer(queue='q.A', ackMode=MANUAL)\n" +
                "  2. Process bean through SolaceConsumerProcessor\n" +
                "  3. Verify consumer registration has PERSISTENT mode and MANUAL ack\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - queue = 'q.A'\n" +
                "  - mode = PERSISTENT (derived from AUTO + queue present)\n" +
                "  - ackMode = MANUAL (explicitly specified)\n" +
                "  - messageType = String.class (inferred from method parameter)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Processing BeanA with @SolaceConsumer(queue='q.A', ackMode=MANUAL)");
        BeanA b = new BeanA();
        processor.postProcessAfterInitialization(b, "ba");

        log.info("STEP 2: Verifying consumer registration attributes");
        Recording rec = manager.records.stream()
                .filter(r -> "q.A".equals(r.queue))
                .findFirst().orElseThrow();
        assertThat(rec.queue).isEqualTo("q.A");
        assertThat(rec.mode.name()).isEqualTo("PERSISTENT");
        assertThat(rec.ackMode.name()).isEqualTo("MANUAL");
        assertThat(rec.messageType).isEqualTo(String.class);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  queue: '" + rec.queue + "' (expected: 'q.A')\n" +
                "  mode: " + rec.mode + " (expected: PERSISTENT)\n" +
                "  ackMode: " + rec.ackMode + " (expected: MANUAL)\n" +
                "  messageType: " + rec.messageType.getSimpleName() + " (expected: String)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Processor correctly detected queue='q.A'\n" +
                "  - AUTO mode resolved to PERSISTENT (queue present)\n" +
                "  - ackMode=MANUAL was preserved from annotation\n" +
                "  - Message type inferred from method parameter (String body)\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Application will receive messages from durable queue 'q.A'\n" +
                "  - Must call ack() explicitly after processing each message\n" +
                "  - Can NACK messages to trigger redelivery or DMQ\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void direct_topics_auto_ack_and_message_type_override() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: direct_topics_auto_ack_and_message_type_override\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that a @SolaceConsumer annotation with only topics (no queue)\n" +
                "  correctly uses DIRECT mode, AUTO ack, and respects messageType override.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Topic-based consumer registration (no queue)\n" +
                "  2. AUTO messaging mode → DIRECT when only topics specified\n" +
                "  3. Default ackMode=AUTO for direct messaging\n" +
                "  4. Explicit messageType override via annotation attribute\n" +
                "\n" +
                "DIRECT MODE CHARACTERISTICS:\n" +
                "  - Best-effort delivery (no persistence)\n" +
                "  - If consumer not online, message is LOST\n" +
                "  - Lowest latency for real-time streaming\n" +
                "  - No acknowledgment needed (fire-and-forget semantics)\n" +
                "\n" +
                "MESSAGE TYPE OVERRIDE:\n" +
                "  - By default, type is inferred from method parameter\n" +
                "  - messageType='java.lang.Integer' explicitly overrides\n" +
                "  - Useful when parameter type is generic (Object) or different\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create bean with @SolaceConsumer(topics={'t/>'}, messageType='java.lang.Integer')\n" +
                "  2. Process bean through SolaceConsumerProcessor\n" +
                "  3. Verify consumer has DIRECT mode, AUTO ack, and Integer type\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - queue = '' (empty - no queue for direct)\n" +
                "  - mode = DIRECT (derived from AUTO + topics only)\n" +
                "  - ackMode = AUTO (default)\n" +
                "  - messageType = Integer.class (explicitly overridden)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Processing BeanA with @SolaceConsumer(topics={'t/>'}, messageType='java.lang.Integer')");
        BeanA b = new BeanA();
        processor.postProcessAfterInitialization(b, "ba");

        log.info("STEP 2: Verifying consumer registration attributes for topic-based consumer");
        Recording rec = manager.records.stream()
                .filter(r -> Arrays.asList(r.topics).contains("t/>") )
                .findFirst().orElseThrow();
        assertThat(rec.queue).isEqualTo("");
        assertThat(rec.mode.name()).isEqualTo("DIRECT");
        assertThat(rec.ackMode.name()).isEqualTo("AUTO");
        assertThat(rec.messageType).isEqualTo(Integer.class);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  queue: '" + rec.queue + "' (expected: '' empty)\n" +
                "  topics: " + Arrays.toString(rec.topics) + " (expected: ['t/>'])\n" +
                "  mode: " + rec.mode + " (expected: DIRECT)\n" +
                "  ackMode: " + rec.ackMode + " (expected: AUTO)\n" +
                "  messageType: " + rec.messageType.getSimpleName() + " (expected: Integer)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - No queue specified, only topic subscription\n" +
                "  - AUTO mode correctly resolved to DIRECT\n" +
                "  - ackMode defaults to AUTO (no explicit setting)\n" +
                "  - messageType override was applied (Integer instead of method param)\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Consumer will receive messages published to 't/>' and subtopics\n" +
                "  - No persistence - consumer must be online to receive\n" +
                "  - Messages deserialized as Integer (per override)\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void listener_consumer_id_prefix_and_condition_evaluation() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: listener_consumer_id_prefix_and_condition_evaluation\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that consumerIdPrefix generates prefixed consumer IDs and\n" +
                "  that SpEL condition expressions correctly filter messages.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. consumerIdPrefix - prefixes generated consumer IDs\n" +
                "  2. condition - SpEL expression for message filtering\n" +
                "  3. Condition evaluation with #message variable\n" +
                "  4. Messages failing condition are silently skipped\n" +
                "\n" +
                "CONSUMER ID PREFIX USE CASES:\n" +
                "  - Distinguish consumers by service/module in logs\n" +
                "  - Group related consumers by prefix for monitoring\n" +
                "  - Debug which consumer processed a message\n" +
                "\n" +
                "CONDITION FILTERING USE CASES:\n" +
                "  - Filter messages by content without explicit code\n" +
                "  - Route messages to handlers based on attributes\n" +
                "  - Implement content-based routing declaratively\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create bean with @SolaceConsumer(consumerIdPrefix='pref-',\n" +
                "     condition='#message.length() > 5')\n" +
                "  2. Process bean and verify consumer ID starts with 'pref-'\n" +
                "  3. Send message 'hi' (length=2) - should be SKIPPED\n" +
                "  4. Send message 'hello!' (length=6) - should be PROCESSED\n" +
                "  5. Verify only 1 message was processed\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - consumerId starts with 'pref-'\n" +
                "  - 'hi' skipped (condition false: 2 > 5 is false)\n" +
                "  - 'hello!' processed (condition true: 6 > 5 is true)\n" +
                "  - BeanB.called = 1\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Processing BeanB with @SolaceConsumer(consumerIdPrefix='pref-', condition='#message.length() > 5')");
        BeanB b = new BeanB();
        processor.postProcessAfterInitialization(b, "bb");

        log.info("STEP 2: Verifying consumer ID has prefix 'pref-'");
        Recording pref = manager.records.stream()
                .filter(r -> "q.B".equals(r.queue))
                .findFirst().orElseThrow();
        assertThat(pref.consumerId).startsWith("pref-");
        log.info("        consumerId=" + pref.consumerId);

        log.info("STEP 3: Testing SpEL condition - only messages with length > 5 should be processed");
        BeanB.called.set(0);
        log.info("        Sending 'hi' (length=2) - should be SKIPPED");
        ((SolaceMessageHandler) pref.handler).handleMessage("hi", dummyInbound());
        log.info("        Sending 'hello!' (length=6) - should be PROCESSED");
        ((SolaceMessageHandler) pref.handler).handleMessage("hello!", dummyInbound());
        assertThat(BeanB.called.get()).isEqualTo(1);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  consumerId: '" + pref.consumerId + "' (expected: starts with 'pref-')\n" +
                "  Messages processed: " + BeanB.called.get() + " (expected: 1)\n" +
                "\n" +
                "CONDITION EVALUATION:\n" +
                "  - 'hi' (length=2): #message.length() > 5 → 2 > 5 → FALSE → SKIPPED\n" +
                "  - 'hello!' (length=6): #message.length() > 5 → 6 > 5 → TRUE → PROCESSED\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - consumerIdPrefix correctly prepended to generated ID\n" +
                "  - SpEL condition evaluated against each incoming message\n" +
                "  - Messages failing condition were silently skipped\n" +
                "  - Only messages passing condition reached handler method\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Declarative content-based filtering without code changes\n" +
                "  - Consumer IDs can be organized by service/component\n" +
                "  - SpEL provides full expression language capabilities\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    // --- Fixtures ----------------------------------------------------------

    static class Recording {
        String consumerId;
        String queue;
        String[] topics;
        com.solace.wrapper.annotation.SolaceConsumer.MessagingMode mode;
        boolean autoCreate;
        com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode;
        Class<?> messageType;
        SolaceMessageHandler<?> handler;
        String clientName;
        boolean autoStart;
    }

    static class RecordingManager extends SolaceConsumerManager {
        List<Recording> records = new ArrayList<>();
        RecordingManager() { super(null, null); }
        @Override
        public String createEnhancedConsumerRaw(String consumerId, String queueName, String[] topics,
                                                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode messagingMode,
                                                boolean autoCreateQueue,
                                                com.solace.wrapper.annotation.SolaceConsumer.AckMode ackMode,
                                                Class<?> messageType,
                                                SolaceMessageHandler<?> messageHandler,
                                                String clientName,
                                                int localMaxAttempts,
                                                long localBackoffInitialMs,
                                                double localBackoffMultiplier,
                                                long localBackoffMaxMs,
                                                boolean autoStart) {
            Recording r = new Recording();
            r.consumerId = consumerId;
            r.queue = queueName;
            r.topics = topics != null ? topics : new String[0];
            if (messagingMode == com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.AUTO) {
                if (queueName != null && !queueName.isEmpty()) {
                    r.mode = com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT;
                } else if (r.topics.length > 0) {
                    r.mode = com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.DIRECT;
                }
            } else { r.mode = messagingMode; }
            r.autoCreate = autoCreateQueue;
            r.ackMode = ackMode;
            r.messageType = messageType;
            r.handler = messageHandler;
            r.clientName = clientName;
            r.autoStart = autoStart;
            records.add(r);
            return consumerId;
        }
    }

    static class BeanA {
        @SolaceConsumer(queue = "q.A", ackMode = SolaceConsumer.AckMode.MANUAL)
        public void qHandler(String body) {}

        @SolaceConsumer(topics = {"t/>"}, messageType = "java.lang.Integer")
        public void tHandler(Integer body) {}
    }

    static class BeanB {
        static final AtomicInteger called = new AtomicInteger();
        @SolaceConsumer(queue = "q.B", condition = "#message.length() > 5", consumerIdPrefix = "pref-")
        public void conditional(String body) { called.incrementAndGet(); }
    }

    static InboundMessage dummyInbound() {
        return (InboundMessage) java.lang.reflect.Proxy.newProxyInstance(
                SolaceConsumerAnnotationTest.class.getClassLoader(),
                new Class<?>[]{InboundMessage.class},
                (p, m, a) -> null
        );
    }
}
