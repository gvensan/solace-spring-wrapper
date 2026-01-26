package com.solace.wrapper.broker;

import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.serialization.JsonMessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("broker")
public class DirectMessagingBrokerIT {

    private static final Logger log = LoggerFactory.getLogger(DirectMessagingBrokerIT.class);

    private BrokerTestSettings settings;
    private SolaceConnectionManager connectionManager;
    private SolaceConsumerManager consumerManager;
    private SolacePublisher publisher;

    @BeforeEach
    void setup() {
        settings = BrokerTestSettings.load();
        settings.assumeConfigured();
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "SETUP: Connecting to Solace broker at " + settings.host + "\n" +
                "───────────────────────────────────────────────────────────────");
        var props = BrokerTestSupport.toSolaceProperties(settings);
        connectionManager = new SolaceConnectionManager(props);
        var serializer = new JsonMessageSerializer();
        consumerManager = new SolaceConsumerManager(connectionManager, serializer);
        publisher = new SolacePublisher(connectionManager, serializer);
    }

    @AfterEach
    void cleanup() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "CLEANUP: Shutting down consumers, publisher, and connection manager\n" +
                "───────────────────────────────────────────────────────────────");
        try {
            if (consumerManager != null) {
                consumerManager.shutdown();
            }
        } catch (Exception ignore) { }
        try {
            if (publisher != null) {
                publisher.shutdown();
            }
        } catch (Exception ignore) { }
        try {
            if (connectionManager != null) {
                connectionManager.shutdown();
            }
        } catch (Exception ignore) { }
    }

    @Test
    void direct_consumer_receives_published_message() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: direct_consumer_receives_published_message\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify direct (non-persistent) pub/sub messaging works correctly.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. DIRECT messaging mode - topic subscription without a queue\n" +
                "  2. publishToTopic() - best-effort delivery (fire-and-forget)\n" +
                "  3. AUTO ack mode - no explicit acknowledgment needed\n" +
                "\n" +
                "DIRECT vs PERSISTENT MESSAGING:\n" +
                "  - DIRECT: No queue, no persistence. If consumer is not online when\n" +
                "    message is published, message is LOST. Fastest, lowest latency.\n" +
                "  - PERSISTENT: Queue stores messages. If consumer is offline, messages\n" +
                "    are retained until consumer reconnects. Guaranteed delivery.\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create a DIRECT mode consumer subscribed to a topic\n" +
                "  2. Publish a non-persistent message to the same topic\n" +
                "  3. Consumer receives message immediately (real-time)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Message is received by consumer within timeout\n" +
                "  - Received payload matches published payload\n" +
                "  - This verifies basic pub/sub pattern for real-time event streaming\n" +
                "\n" +
                "USE CASES:\n" +
                "  - Real-time notifications (user online status, typing indicators)\n" +
                "  - Live data feeds (stock prices, sensor readings)\n" +
                "  - Event broadcasting where missing occasional messages is acceptable\n" +
                "───────────────────────────────────────────────────────────────\n");

        String topic = settings.uniqueTopic("direct");
        String payload = "hello-" + UUID.randomUUID();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> received = new AtomicReference<>();

        log.info("STEP 1: Creating DIRECT mode consumer subscribing to topic '{}'", topic);
        log.info("        Mode: DIRECT (no queue), AckMode: AUTO");
        log.info("        Note: Direct mode = no persistence, best-effort delivery");
        consumerManager.createEnhancedConsumerRaw(
                "direct-" + UUID.randomUUID(),
                "",
                new String[]{topic},
                SolaceConsumer.MessagingMode.DIRECT,
                false,
                SolaceConsumer.AckMode.AUTO,
                String.class,
                (SolaceMessageHandler<Object>) (message, originalMessage) -> {
                    log.info("STEP 3: Message received by direct consumer: '{}'", message);
                    received.set((String) message);
                    latch.countDown();
                }
        );

        log.info("STEP 2: Publishing message to topic '{}' (direct/non-persistent)", topic);
        log.info("        Payload: '{}'", payload);
        publisher.publishToTopic(topic, payload);

        boolean ok = latch.await(settings.timeoutSeconds, TimeUnit.SECONDS);
        String receivedPayload = received.get();
        boolean payloadMatches = payload.equals(receivedPayload);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Message received within timeout: " + ok + " (expected: true)\n" +
                "  Received payload: '" + receivedPayload + "'\n" +
                "  Expected payload: '" + payload + "'\n" +
                "  Payload matches: " + payloadMatches + "\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Direct mode consumer subscribed to topic '" + topic + "'\n" +
                "  - Message was published (non-persistent) to the same topic\n" +
                "  - Consumer received message in real-time without queue intermediary\n" +
                "  - This confirms DIRECT pub/sub works for real-time messaging\n" +
                "\n" +
                "STATUS: " + (ok && payloadMatches ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");
        assertThat(ok).isTrue();
        assertThat(received.get()).isEqualTo(payload);
    }
}
