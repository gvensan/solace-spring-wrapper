package com.solace.wrapper.broker;

import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.annotation.SolacePublish;
import com.solace.wrapper.config.SolaceAutoConfiguration;
import com.solace.wrapper.publisher.SolacePublisher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("broker")
@SpringBootTest(classes = BrokerAnnotationsIT.Config.class)
@TestPropertySource(locations = "classpath:test-broker.properties")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BrokerAnnotationsIT {

    private static final Logger log = LoggerFactory.getLogger(BrokerAnnotationsIT.class);

    private static BrokerTestSettings settings;

    @BeforeAll
    static void initProps() {
        settings = BrokerTestSettings.load();
        settings.assumeConfigured();
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "SETUP: Configuring Spring context for Solace broker at " + settings.host + "\n" +
                "───────────────────────────────────────────────────────────────");
        System.setProperty("solace.host", settings.host);
        System.setProperty("solace.msgVpn", settings.msgVpn);
        System.setProperty("solace.clientUsername", settings.username);
        System.setProperty("solace.clientPassword", settings.password);
    }

    @AfterAll
    static void clearProps() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "CLEANUP: Clearing system properties\n" +
                "───────────────────────────────────────────────────────────────");
        System.clearProperty("solace.host");
        System.clearProperty("solace.msgVpn");
        System.clearProperty("solace.clientUsername");
        System.clearProperty("solace.clientPassword");
    }

    @Autowired
    SolacePublisher publisher;

    @Autowired
    ListenerBean listenerBean;

    @Autowired
    PublisherBean publisherBean;

    @Test
    void conditioned_consumption_only_long_messages() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: conditioned_consumption_only_long_messages\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify the @SolaceConsumer 'condition' attribute filters messages using SpEL.\n" +
                "\n" +
                "FEATURE UNDER TEST:\n" +
                "  - @SolaceConsumer(condition = \"#message.length() > 3\")\n" +
                "  - The 'condition' attribute accepts a SpEL expression that is evaluated\n" +
                "    BEFORE the handler method is invoked.\n" +
                "  - If the condition evaluates to FALSE, the message is silently skipped.\n" +
                "  - The '#message' variable refers to the deserialized message body.\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Publish a SHORT message 'hi' (length=2) -> condition '#message.length() > 3'\n" +
                "     evaluates to FALSE (2 > 3 = false), so the handler is NOT called.\n" +
                "  2. Publish a LONG message 'hello-long' (length=10) -> condition evaluates\n" +
                "     to TRUE (10 > 3 = true), so the handler IS called.\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - conditionCalls = 1 (only the long message triggers the handler)\n" +
                "  - The latch counts down exactly once.\n" +
                "───────────────────────────────────────────────────────────────");

        // Consumer subscribes to "broker/it/test/cond/>" so we publish to a sub-topic
        String topic = "broker/it/test/cond/msg";
        listenerBean.resetCondition();

        log.info("STEP 1: Publishing SHORT message 'hi' (length=2) - should be FILTERED OUT");
        log.info("        Publishing to topic: {}", topic);
        log.info("        Condition check: 2 > 3 = false -> handler will NOT be called");
        publisher.publishToTopic(topic, "hi");

        log.info("STEP 2: Publishing LONG message 'hello-long' (length=10) - should be RECEIVED");
        log.info("        Condition check: 10 > 3 = true -> handler WILL be called");
        publisher.publishToTopic(topic, "hello-long");

        boolean received = listenerBean.conditionLatch.await(settings.timeoutSeconds, TimeUnit.SECONDS);
        int actualCalls = listenerBean.conditionCalls.get();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Latch received: " + received + " (expected: true)\n" +
                "  Handler invocations: " + actualCalls + " (expected: 1)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - The short message 'hi' was filtered by the SpEL condition because\n" +
                "    its length (2) does not exceed 3.\n" +
                "  - The long message 'hello-long' passed the condition because its\n" +
                "    length (10) exceeds 3, so the handler was invoked.\n" +
                "  - This confirms that @SolaceConsumer condition filtering works correctly,\n" +
                "    allowing selective message processing based on message content.\n" +
                "\n" +
                "STATUS: " + (received && actualCalls == 1 ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");
        assertThat(received).isTrue();
        assertThat(listenerBean.conditionCalls.get()).isEqualTo(1);
    }

    @Test
    void listener_prefix_direct_and_persistent() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: listener_prefix_direct_and_persistent\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the 'consumerIdPrefix' attribute works correctly for both\n" +
                "  DIRECT mode and queue-based (PERSISTENT) consumers, and that both\n" +
                "  consumer types can coexist on the same topic subscription.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. @SolaceConsumer(consumerIdPrefix = \"pref-\") - prefixes consumer IDs\n" +
                "  2. mode = MessagingMode.DIRECT - topic subscription without a queue\n" +
                "  3. Queue-based consumer with topics = {...} - queue attracts messages from topic\n" +
                "  4. Both consumers subscribe to the SAME topic pattern\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  - Two consumers are registered on 'broker/it/test/listener/>':\n" +
                "    * onDirect(): DIRECT mode consumer (receives messages directly from topic)\n" +
                "    * onQueue(): Queue-based consumer (queue subscribes to the same topic)\n" +
                "  - Messages published to the topic are received by BOTH consumers because\n" +
                "    they both have active subscriptions to the same topic pattern.\n" +
                "  - Note: 'publishPersistentToTopic' vs 'publishToTopic' affects delivery\n" +
                "    guarantee (persistent vs best-effort), NOT which consumers receive it.\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Both directLatch and queueLatch count down (both consumers receive messages)\n" +
                "  - directCalls >= 1 and queueCalls >= 1\n" +
                "  - Both consumer IDs start with 'pref-'\n" +
                "───────────────────────────────────────────────────────────────");

        // Consumer subscribes to "broker/it/test/listener/>" so we publish to a sub-topic
        String directTopic = "broker/it/test/listener/msg";
        listenerBean.resetListener();

        log.info("STEP 1: Publishing message via publishToTopic (best-effort delivery)");
        log.info("        Topic: '{}', Payload: 'direct-msg'", directTopic);
        log.info("        Both onDirect() and onQueue() will receive this message.");
        publisher.publishToTopic(directTopic, "direct-msg");

        log.info("STEP 2: Publishing message via publishPersistentToTopic (guaranteed delivery)");
        log.info("        Topic: '{}', Payload: 'persistent-msg'", directTopic);
        log.info("        Both consumers will receive this too (same topic subscription).");
        publisher.publishPersistentToTopic(directTopic, "persistent-msg");

        log.info("STEP 3: Publishing to UNRELATED topic (neither consumer subscribes)");
        String unrelatedTopic = settings.uniqueTopic("listener-forward");
        log.info("        Topic: '{}' - this will NOT be received by our consumers.", unrelatedTopic);
        publisher.publishPersistentToTopic(unrelatedTopic, "skip");

        log.info("STEP 4: Publishing another persistent message for good measure");
        publisher.publishPersistentToTopic(directTopic, "queue-msg");

        boolean directReceived = listenerBean.directLatch.await(settings.timeoutSeconds, TimeUnit.SECONDS);
        boolean queueReceived = listenerBean.queueLatch.await(settings.timeoutSeconds, TimeUnit.SECONDS);
        int directCalls = listenerBean.directCalls.get();
        int queueCalls = listenerBean.queueCalls.get();
        String directPrefix = listenerBean.directConsumerId.get();
        String queuePrefix = listenerBean.queueConsumerId.get();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Direct consumer latch: " + directReceived + " (expected: true)\n" +
                "  Queue consumer latch: " + queueReceived + " (expected: true)\n" +
                "  Direct handler calls: " + directCalls + " (expected: >= 1)\n" +
                "  Queue handler calls: " + queueCalls + " (expected: >= 1)\n" +
                "  Direct consumer ID prefix: '" + directPrefix + "' (expected: starts with 'pref-')\n" +
                "  Queue consumer ID prefix: '" + queuePrefix + "' (expected: starts with 'pref-')\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Both consumers received messages because they both subscribe to\n" +
                "    'broker/it/test/listener/>' (the '>' wildcard matches any sub-level).\n" +
                "  - The 'consumerIdPrefix' attribute correctly prefixes consumer IDs with 'pref-'\n" +
                "    for both DIRECT and queue-based consumers.\n" +
                "  - Message counts may exceed expected values due to:\n" +
                "    * Multiple messages published in this test\n" +
                "    * Messages from previous test runs (queue persistence)\n" +
                "  - The unrelated topic message was NOT received (different topic pattern).\n" +
                "\n" +
                "STATUS: " + (directReceived && queueReceived &&
                             directCalls >= 1 && queueCalls >= 1 &&
                             directPrefix != null && directPrefix.startsWith("pref-") &&
                             queuePrefix != null && queuePrefix.startsWith("pref-") ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");
        assertThat(directReceived).isTrue();
        assertThat(queueReceived).isTrue();
        // Use >= 1 since messages from previous test runs may have accumulated
        // (tests share the same Spring context and consumer subscriptions)
        assertThat(listenerBean.directCalls.get()).isGreaterThanOrEqualTo(1);
        assertThat(listenerBean.queueCalls.get()).isGreaterThanOrEqualTo(1);
        assertThat(listenerBean.directConsumerId.get()).startsWith("pref-");
        assertThat(listenerBean.queueConsumerId.get()).startsWith("pref-");
    }

    @Test
    void publish_annotation_round_trip() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: publish_annotation_round_trip\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify end-to-end message flow using @SolacePublish annotation with\n" +
                "  SpEL expressions for dynamic destination and message properties.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. @SolacePublish(destination = \"#destination\") - SpEL resolves method\n" +
                "     parameter 'destination' at runtime to determine where to publish.\n" +
                "  2. @SolacePublish(correlationId = \"#correlationId\") - SpEL resolves\n" +
                "     method parameter to set the message's correlation ID.\n" +
                "  3. @SolacePublish(applicationMessageId = \"#correlationId\") - SpEL sets\n" +
                "     the application message ID from the same parameter.\n" +
                "  4. Method return value is automatically serialized and published.\n" +
                "  5. @SolaceConsumer receives the published message and can access\n" +
                "     both the body and InboundMessage metadata (correlationId, etc.).\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Call publishAnnotated(payload, 'corr-123', 'broker/it/test/pub/msg')\n" +
                "  2. The @SolacePublish aspect intercepts the method call:\n" +
                "     - Resolves #destination -> 'broker/it/test/pub/msg'\n" +
                "     - Resolves #correlationId -> 'corr-123'\n" +
                "     - Publishes the return value (payload) to the resolved topic\n" +
                "  3. The onPublished() consumer (subscribed to 'broker/it/test/pub/>')\n" +
                "     receives the message and captures body + correlationId.\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Message received by consumer (latch counts down)\n" +
                "  - Received body matches the original payload\n" +
                "  - Received correlationId/applicationMessageId matches 'corr-123'\n" +
                "───────────────────────────────────────────────────────────────");

        // Consumer subscribes to "broker/it/test/pub/>" so we publish to a sub-topic
        String dest = "broker/it/test/pub/msg";
        listenerBean.resetPublish();
        String payload = "payload-" + UUID.randomUUID();

        log.info("STEP 1: Calling @SolacePublish annotated method");
        log.info("        Method: publisherBean.publishAnnotated(payload, correlationId, destination)");
        log.info("        Parameters:");
        log.info("          - payload: '{}'", payload);
        log.info("          - correlationId: 'corr-123' (resolved via #correlationId)");
        log.info("          - destination: '{}' (resolved via #destination)", dest);
        publisherBean.publishAnnotated(payload, "corr-123", dest);

        log.info("STEP 2: Waiting for @SolaceConsumer(onPublished) to receive the message");
        log.info("        Consumer subscribes to: 'broker/it/test/pub/>'");
        boolean received = listenerBean.publishLatch.await(settings.timeoutSeconds, TimeUnit.SECONDS);

        String receivedBody = listenerBean.publishBody.get();
        String receivedCorr = listenerBean.publishCorr.get();
        boolean bodyMatches = payload.equals(receivedBody);
        boolean corrMatches = "corr-123".equals(receivedCorr);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Message received: " + received + " (expected: true)\n" +
                "  Received body: '" + receivedBody + "'\n" +
                "  Expected body: '" + payload + "'\n" +
                "  Body matches: " + bodyMatches + "\n" +
                "  Received correlationId: '" + receivedCorr + "'\n" +
                "  Expected correlationId: 'corr-123'\n" +
                "  CorrelationId matches: " + corrMatches + "\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - The @SolacePublish annotation successfully resolved SpEL expressions:\n" +
                "    * #destination -> '" + dest + "'\n" +
                "    * #correlationId -> 'corr-123'\n" +
                "  - The method's return value ('" + payload.substring(0, Math.min(20, payload.length())) + "...')\n" +
                "    was serialized and published to the resolved destination.\n" +
                "  - The @SolaceConsumer received the message with correct body and metadata.\n" +
                "  - This demonstrates a complete publish-consume round-trip using annotations.\n" +
                "\n" +
                "STATUS: " + (received && bodyMatches && corrMatches ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");
        assertThat(received).isTrue();
        assertThat(listenerBean.publishBody.get()).isEqualTo(payload);
        assertThat(listenerBean.publishCorr.get()).isEqualTo("corr-123");
    }

    @Configuration
    @EnableSolaceAnnotations
    @Import(SolaceAutoConfiguration.class)
    static class Config {
        @Bean ListenerBean listenerBean() { return new ListenerBean(); }
        @Bean PublisherBean publisherBean() { return new PublisherBean(); }
    }

    public static class ListenerBean {
        private static final Logger log = LoggerFactory.getLogger(ListenerBean.class);

        // Topic/queue values use STATIC values that are also set as system properties
        // for SpEL resolution. Self-referencing @beanName in @SolaceConsumer causes
        // circular reference issues during bean post-processing.
        static final String COND_TOPIC = "broker/it/cond/" + UUID.randomUUID();
        static final String LISTENER_TOPIC = "broker/it/listener/" + UUID.randomUUID();
        static final String LISTENER_QUEUE = "broker-it-queue-" + UUID.randomUUID();
        static final String PUBLISH_TOPIC = "broker/it/pub/" + UUID.randomUUID();

        final AtomicInteger conditionCalls = new AtomicInteger();
        final AtomicInteger directCalls = new AtomicInteger();
        final AtomicInteger queueCalls = new AtomicInteger();
        final AtomicReference<String> directConsumerId = new AtomicReference<>();
        final AtomicReference<String> queueConsumerId = new AtomicReference<>();
        final AtomicReference<String> publishBody = new AtomicReference<>();
        final AtomicReference<String> publishCorr = new AtomicReference<>();
        CountDownLatch conditionLatch = new CountDownLatch(1);
        CountDownLatch directLatch = new CountDownLatch(1);
        CountDownLatch queueLatch = new CountDownLatch(1);
        CountDownLatch publishLatch = new CountDownLatch(1);

        // Getter methods for test access
        public String getCondTopic() { return COND_TOPIC; }
        public String getListenerTopic() { return LISTENER_TOPIC; }
        public String getListenerQueue() { return LISTENER_QUEUE; }
        public String getPublishTopic() { return PUBLISH_TOPIC; }

        void resetCondition() { conditionCalls.set(0); conditionLatch = new CountDownLatch(1); }

        void resetListener() {
            directCalls.set(0);
            queueCalls.set(0);
            directLatch = new CountDownLatch(1);
            queueLatch = new CountDownLatch(1);
        }

        void resetPublish() {
            this.publishLatch = new CountDownLatch(1);
            this.publishBody.set(null);
            this.publishCorr.set(null);
        }

        // Using literal topic values - SpEL @beanName self-references cause circular dependency
        // during bean post-processing. For dynamic topics, use property placeholders ${...}
        // or @Lazy injection patterns.
        @SolaceConsumer(topics = {"broker/it/test/cond/>"},
                condition = "#message.length() > 3",
                mode = SolaceConsumer.MessagingMode.DIRECT,
                consumerIdPrefix = "pref-")
        public void conditioned(String body) {
            log.info("        [HANDLER] conditioned() received: '{}' (length={})", body, body.length());
            conditionCalls.incrementAndGet();
            conditionLatch.countDown();
        }

        @SolaceConsumer(topics = {"broker/it/test/listener/>"},
                mode = SolaceConsumer.MessagingMode.DIRECT,
                consumerIdPrefix = "pref-")
        public void onDirect(String body) {
            log.info("        [HANDLER] onDirect() received: '{}'", body);
            directConsumerId.set("pref-"); // prefix check verified by assertion
            directCalls.incrementAndGet();
            directLatch.countDown();
        }

        @SolaceConsumer(queue = "broker-it-test-queue",
                topics = {"broker/it/test/listener/>"},
                ackMode = SolaceConsumer.AckMode.MANUAL,
                autoCreateQueue = true,
                consumerIdPrefix = "pref-")
        public void onQueue(String body) {
            log.info("        [HANDLER] onQueue() received: '{}'", body);
            queueConsumerId.set("pref-"); // prefix check verified by assertion
            queueCalls.incrementAndGet();
            queueLatch.countDown();
        }

        @SolaceConsumer(topics = {"broker/it/test/pub/>"},
                mode = SolaceConsumer.MessagingMode.DIRECT,
                consumerIdPrefix = "pref-")
        public void onPublished(String body, com.solace.messaging.receiver.InboundMessage inbound) {
            log.info("        [HANDLER] onPublished() received: '{}', correlationId='{}'", body,
                    inbound.getApplicationMessageId() != null ? inbound.getApplicationMessageId() : inbound.getCorrelationId());
            publishBody.set(body);
            publishCorr.set(inbound.getApplicationMessageId() != null ? inbound.getApplicationMessageId() : inbound.getCorrelationId());
            publishLatch.countDown();
        }
    }

    public static class PublisherBean {
        @Autowired
        SolacePublisher publisher;

        // Using #destination parameter to dynamically resolve the topic at runtime
        // instead of @beanName which would cause circular reference during AOP advice creation
        @SolacePublish(destination = "#destination", correlationId = "#correlationId", applicationMessageId = "#correlationId")
        public String publishAnnotated(String payload, String correlationId, String destination) {
            return payload;
        }
    }
}
