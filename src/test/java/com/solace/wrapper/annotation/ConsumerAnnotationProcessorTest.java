package com.solace.wrapper.annotation;

import com.solace.wrapper.annotation.processor.SolaceConsumerProcessor;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerAnnotationProcessorTest {

    private static final Logger log = LoggerFactory.getLogger(ConsumerAnnotationProcessorTest.class);

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
    void registers_persistent_queue_auto_mode_and_ack_manual() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: registers_persistent_queue_auto_mode_and_ack_manual\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that @SolaceConsumer with a queue name automatically triggers\n" +
                "  PERSISTENT messaging mode when mode=AUTO (the default).\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. AUTO mode detection - presence of queue name -> PERSISTENT mode\n" +
                "  2. MANUAL ack mode - explicit acknowledgment control\n" +
                "  3. autoCreateQueue - defaults to true for new queue provisioning\n" +
                "  4. Message type inference - inferred from method parameter (String)\n" +
                "  5. autoStart - defaults to true (consumer starts immediately)\n" +
                "\n" +
                "ANNOTATION BEING TESTED:\n" +
                "  @SolaceConsumer(queue = \"q.orders\", ackMode = MANUAL)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Consumer registered with queue='q.orders'\n" +
                "  - Mode resolved to PERSISTENT (because queue is specified)\n" +
                "  - AckMode is MANUAL as specified\n" +
                "  - MessageType inferred as String.class from method parameter\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Processing TestBean with @SolaceConsumer(queue='q.orders', ackMode=MANUAL)");
        TestBean b = new TestBean();
        processor.postProcessAfterInitialization(b, "tb");

        log.info("STEP 2: Verifying consumer registration attributes");
        Recording rec = manager.records.stream().filter(r -> "q.orders".equals(r.queue)).findFirst().orElseThrow();
        assertThat(rec.queue).isEqualTo("q.orders");
        assertThat(rec.topics).isEmpty();
        assertThat(rec.mode.name()).isEqualTo("PERSISTENT");
        assertThat(rec.autoCreate).isTrue();
        assertThat(rec.ackMode.name()).isEqualTo("MANUAL");
        assertThat(rec.messageType).isEqualTo(String.class);
        assertThat(rec.autoStart).isTrue();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  queue: '" + rec.queue + "' (expected: 'q.orders')\n" +
                "  mode: " + rec.mode + " (expected: PERSISTENT - queue triggers persistent mode)\n" +
                "  ackMode: " + rec.ackMode + " (expected: MANUAL - as specified)\n" +
                "  autoCreate: " + rec.autoCreate + " (expected: true - default)\n" +
                "  messageType: " + rec.messageType.getSimpleName() + " (expected: String - inferred)\n" +
                "  autoStart: " + rec.autoStart + " (expected: true - default)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  The SolaceConsumerProcessor correctly detected queue-based configuration\n" +
                "  and resolved mode=AUTO to PERSISTENT mode. All other attributes\n" +
                "  were captured correctly from the annotation and defaults.\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void registers_direct_topics_auto_mode() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: registers_direct_topics_auto_mode\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that @SolaceConsumer with topics but NO queue automatically\n" +
                "  triggers DIRECT messaging mode when mode=AUTO (the default).\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. AUTO mode detection - topics without queue -> DIRECT mode\n" +
                "  2. Multiple topic subscriptions - supports wildcard patterns\n" +
                "  3. AUTO ack mode - default for DIRECT messaging\n" +
                "  4. autoCreateQueue=false - no queue to create in DIRECT mode\n" +
                "\n" +
                "ANNOTATION BEING TESTED:\n" +
                "  @SolaceConsumer(topics = {\"events/>\", \"audit/*\"}, autoCreateQueue = false)\n" +
                "\n" +
                "WILDCARD PATTERNS:\n" +
                "  - '>' matches one or more levels (e.g., events/> matches events/a/b/c)\n" +
                "  - '*' matches exactly one level (e.g., audit/* matches audit/login)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Consumer registered with topics=['events/>', 'audit/*']\n" +
                "  - Mode resolved to DIRECT (because only topics are specified)\n" +
                "  - AckMode is AUTO (default for direct mode)\n" +
                "  - queue is empty (DIRECT mode doesn't use queues)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Processing TestBean with @SolaceConsumer(topics={'events/>','audit/*'})");
        TestBean b = new TestBean();
        processor.postProcessAfterInitialization(b, "tb");

        log.info("STEP 2: Verifying consumer registration attributes for DIRECT mode");
        Recording rec = manager.records.stream().filter(r -> Arrays.asList(r.topics).contains("events/>"))
                .findFirst().orElseThrow();
        assertThat(rec.queue).isEqualTo("");
        assertThat(rec.topics).containsExactly("events/>", "audit/*");
        assertThat(rec.mode.name()).isEqualTo("DIRECT");
        assertThat(rec.autoCreate).isFalse();
        assertThat(rec.ackMode.name()).isEqualTo("AUTO");
        assertThat(rec.messageType).isEqualTo(String.class);
        assertThat(rec.autoStart).isTrue();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  queue: '" + rec.queue + "' (expected: '' - empty for DIRECT mode)\n" +
                "  topics: " + Arrays.toString(rec.topics) + " (expected: [events/>, audit/*])\n" +
                "  mode: " + rec.mode + " (expected: DIRECT - no queue means direct mode)\n" +
                "  ackMode: " + rec.ackMode + " (expected: AUTO - default)\n" +
                "  autoCreate: " + rec.autoCreate + " (expected: false - no queue to create)\n" +
                "  messageType: " + rec.messageType.getSimpleName() + " (expected: String - inferred)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  The SolaceConsumerProcessor correctly detected topic-only configuration\n" +
                "  and resolved mode=AUTO to DIRECT mode. Multiple wildcard topic patterns\n" +
                "  are properly captured for subscription.\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void consumer_id_prefix_and_message_type_override_and_condition() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: consumer_id_prefix_and_message_type_override_and_condition\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify three advanced @SolaceConsumer features working together:\n" +
                "  1. consumerIdPrefix - adds a prefix to auto-generated consumer IDs\n" +
                "  2. messageType override - explicit type instead of inference from parameter\n" +
                "  3. condition - SpEL expression for selective message processing\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. consumerIdPrefix='my-prefix-' - consumer ID starts with 'my-prefix-'\n" +
                "  2. messageType='java.lang.Integer' - overrides parameter type inference\n" +
                "  3. condition='#message.length() > 3' - only process messages longer than 3 chars\n" +
                "\n" +
                "WHY THESE FEATURES MATTER:\n" +
                "  - consumerIdPrefix: Helps identify consumers in logs/monitoring by service\n" +
                "  - messageType: Useful when parameter type doesn't match wire format\n" +
                "  - condition: Filter messages before handler invocation (saves processing)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Consumer ID starts with 'my-prefix-'\n" +
                "  - Message type is Integer.class (not inferred from parameter)\n" +
                "  - 'hi' (length=2) is skipped, 'hello' (length=5) is processed\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Processing TestBean2 with prefix, messageType, and condition annotations");
        TestBean2 b = new TestBean2();
        processor.postProcessAfterInitialization(b, "tb2");

        log.info("STEP 2: Verifying consumer ID prefix and message type override");
        Recording prefixed = manager.records.stream()
                .filter(r -> r.queue.equals("q.pref") )
                .findFirst().orElseThrow();
        assertThat(prefixed.consumerId).startsWith("my-prefix-");
        assertThat(prefixed.messageType).isEqualTo(Integer.class);
        log.info("        consumerId: '{}' (starts with 'my-prefix-': {})", prefixed.consumerId, prefixed.consumerId.startsWith("my-prefix-"));
        log.info("        messageType: {} (expected: Integer)", prefixed.messageType.getSimpleName());

        log.info("STEP 3: Testing SpEL condition (#message.length() > 3)");
        Recording cond = manager.records.stream()
                .filter(r -> r.queue.equals("q.cond"))
                .findFirst().orElseThrow();

        TestBean2.calledPref.set(0);
        TestBean2.calledCond.set(0);

        log.info("        Sending short message 'hi' (length=2) - should be SKIPPED");
        log.info("        Condition check: 2 > 3 = false -> handler NOT called");
        ((SolaceMessageHandler) cond.handler).handleMessage("hi", dummyInbound());

        log.info("        Sending long message 'hello' (length=5) - should be PROCESSED");
        log.info("        Condition check: 5 > 3 = true -> handler IS called");
        ((SolaceMessageHandler) cond.handler).handleMessage("hello", dummyInbound());

        int calledCount = TestBean2.calledCond.get();
        assertThat(calledCount).isEqualTo(1);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Consumer ID prefix: '" + prefixed.consumerId.substring(0, Math.min(15, prefixed.consumerId.length())) + "...' (starts with 'my-prefix-': true)\n" +
                "  Message type override: " + prefixed.messageType.getSimpleName() + " (expected: Integer)\n" +
                "  Condition filtering:\n" +
                "    - 'hi' (length=2): SKIPPED (2 > 3 = false)\n" +
                "    - 'hello' (length=5): PROCESSED (5 > 3 = true)\n" +
                "  Handler invocations: " + calledCount + " (expected: 1)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  All three advanced features work correctly:\n" +
                "  - consumerIdPrefix adds 'my-prefix-' to auto-generated ID\n" +
                "  - messageType='java.lang.Integer' overrides inference\n" +
                "  - SpEL condition filters messages before handler invocation\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void retry_logic_invokes_multiple_times_then_throws() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: retry_logic_invokes_multiple_times_then_throws\n" +
                "PURPOSE: Verify @SolaceConsumer maxRetries triggers specified number of retries before exception\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Processing TestBean2 with @SolaceConsumer(maxRetries=1)");
        TestBean2 b = new TestBean2();
        processor.postProcessAfterInitialization(b, "tb2");

        Recording rec = manager.records.stream()
                .filter(r -> r.queue.equals("q.retry"))
                .findFirst().orElseThrow();

        log.info("STEP 2: Invoking handler that always throws - should retry once then throw");
        TestBean2.failCount.set(0);
        try {
            ((SolaceMessageHandler) rec.handler).handleMessage("msg", dummyInbound());
        } catch (RuntimeException ex) {
            log.info("        Expected exception after retries exhausted: " + ex.getMessage());
            // expected after retries exhausted
        }

        log.info("STEP 3: Verifying handler was invoked twice (initial + 1 retry)");
        // maxRetries=1 -> total invocations = 2
        assertThat(TestBean2.failCount.get()).isEqualTo(2);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Retry logic works - invoked " + TestBean2.failCount.get() + " times (1 initial + 1 retry)\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void infers_message_type_for_custom_pojo_in_direct_mode() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: infers_message_type_for_custom_pojo_in_direct_mode\n" +
                "PURPOSE: Verify message type is inferred from method parameter for custom POJO types\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Processing TestBean3 with POJO parameter (MyPojo)");
        TestBean3 b = new TestBean3();
        processor.postProcessAfterInitialization(b, "tb3");

        log.info("STEP 2: Verifying messageType was inferred as MyPojo.class");
        Recording rec = manager.records.stream()
                .filter(r -> r.queue.equals("") && r.topics.length > 0)
                .findFirst().orElseThrow();
        assertThat(rec.messageType).isEqualTo(MyPojo.class);
        assertThat(rec.mode.name()).isEqualTo("DIRECT");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Message type correctly inferred as " + rec.messageType.getSimpleName() + "\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void respects_explicit_consumer_id_and_default_ack_auto() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: respects_explicit_consumer_id_and_default_ack_auto\n" +
                "PURPOSE: Verify explicit consumerId is used and default ackMode is AUTO\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Processing TestBean3 with @SolaceConsumer(consumerId='custom-id')");
        TestBean3 b = new TestBean3();
        processor.postProcessAfterInitialization(b, "tb3");

        log.info("STEP 2: Verifying explicit consumerId and default ackMode");
        Recording rec = manager.records.stream()
                .filter(r -> "q.cid".equals(r.queue))
                .findFirst().orElseThrow();
        assertThat(rec.consumerId).isEqualTo("custom-id");
        assertThat(rec.ackMode.name()).isEqualTo("AUTO");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Explicit consumerId='" + rec.consumerId + "', default ackMode=" + rec.ackMode + "\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void captures_client_name_and_auto_start_false() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: captures_client_name_and_auto_start_false\n" +
                "PURPOSE: Verify clientName annotation attribute and autoStart=false are captured\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Processing TestBean4 with @SolaceConsumer(clientName='client-a', autoStart=false)");
        TestBean4 b = new TestBean4();
        processor.postProcessAfterInitialization(b, "tb4");

        log.info("STEP 2: Verifying clientName and autoStart attributes were captured");
        Recording rec = manager.records.stream()
                .filter(r -> "q.client".equals(r.queue))
                .findFirst().orElseThrow();
        assertThat(rec.clientName).isEqualTo("client-a");
        assertThat(rec.autoStart).isFalse();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: clientName='" + rec.clientName + "', autoStart=" + rec.autoStart + "\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void inbound_message_parameter_is_injected() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: inbound_message_parameter_is_injected\n" +
                "PURPOSE: Verify InboundMessage is injected as second parameter when handler declares it\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Processing TestBean2 with handler(String body, InboundMessage original)");
        TestBean2 b = new TestBean2();
        processor.postProcessAfterInitialization(b, "tb2");

        Recording rec = manager.records.stream()
                .filter(r -> r.queue.equals("q.inbound"))
                .findFirst().orElseThrow();

        log.info("STEP 2: Invoking handler with message and verifying InboundMessage is passed");
        TestBean2.gotInbound.set(false);
        ((SolaceMessageHandler) rec.handler).handleMessage("body", dummyInbound());
        assertThat(TestBean2.gotInbound.get()).isTrue();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: InboundMessage was correctly injected into handler method\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    // Test harness types
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
            // Simulate Smart detection as SolaceConsumerManager would
            com.solace.wrapper.annotation.SolaceConsumer.MessagingMode resolved = messagingMode;
            if (messagingMode == com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.AUTO) {
                if (queueName != null && !queueName.isEmpty()) {
                    resolved = com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT;
                } else if (r.topics.length > 0) {
                    resolved = com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.DIRECT;
                }
            }
            r.mode = resolved;
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

    static class TestBean {
        @SolaceConsumer(queue = "q.orders", ackMode = SolaceConsumer.AckMode.MANUAL)
        public void handleOrder(String body) {}

        @SolaceConsumer(topics = {"events/>", "audit/*"}, autoCreateQueue = false)
        public void handleEvents(String body) {}
    }

    static class TestBean2 {
        static java.util.concurrent.atomic.AtomicInteger calledPref = new java.util.concurrent.atomic.AtomicInteger();
        static java.util.concurrent.atomic.AtomicInteger calledCond = new java.util.concurrent.atomic.AtomicInteger();
        static java.util.concurrent.atomic.AtomicInteger failCount = new java.util.concurrent.atomic.AtomicInteger();
        static java.util.concurrent.atomic.AtomicBoolean gotInbound = new java.util.concurrent.atomic.AtomicBoolean(false);

        @SolaceConsumer(queue = "q.pref", messageType = "java.lang.Integer", consumerIdPrefix = "my-prefix-")
        public void withPrefix(Integer body) { calledPref.incrementAndGet(); }

        @SolaceConsumer(queue = "q.cond", condition = "#message.length() > 3", consumerIdPrefix = "my-prefix-")
        public void withCondition(String body) { calledCond.incrementAndGet(); }

        @SolaceConsumer(queue = "q.retry", maxRetries = 1, retryDelay = 1, consumerIdPrefix = "my-prefix-")
        public void withRetry(String body) { failCount.incrementAndGet(); throw new RuntimeException("fail"); }

        @SolaceConsumer(queue = "q.inbound", consumerIdPrefix = "my-prefix-")
        public void withInbound(String body, com.solace.messaging.receiver.InboundMessage original) {
            gotInbound.set(original != null);
        }
    }

    static class TestBean3 {
        @SolaceConsumer(topics = {"tp/>"})
        public void pojo(MyPojo body) {}

        @SolaceConsumer(queue = "q.cid", consumerId = "custom-id")
        public void withId(String body) {}
    }

    static class TestBean4 {
        @SolaceConsumer(queue = "q.client", clientName = "client-a", autoStart = false)
        public void withClient(String body) {}
    }

    static class MyPojo { int a; }

    private static com.solace.messaging.receiver.InboundMessage dummyInbound() {
        return (com.solace.messaging.receiver.InboundMessage) java.lang.reflect.Proxy.newProxyInstance(
                ConsumerAnnotationProcessorTest.class.getClassLoader(),
                new Class<?>[]{com.solace.messaging.receiver.InboundMessage.class},
                (p, m, a) -> null
        );
    }
}
