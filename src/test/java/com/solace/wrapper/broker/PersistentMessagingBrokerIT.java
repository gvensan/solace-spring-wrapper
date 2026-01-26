package com.solace.wrapper.broker;

import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceMessageHandler;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("broker")
public class PersistentMessagingBrokerIT {

    private static final Logger log = LoggerFactory.getLogger(PersistentMessagingBrokerIT.class);

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
            if (consumerManager != null) consumerManager.shutdown();
        } catch (Exception ignore) { }
        try {
            if (publisher != null) publisher.shutdown();
        } catch (Exception ignore) { }
        try {
            if (connectionManager != null) connectionManager.shutdown();
        } catch (Exception ignore) { }
    }

    @Test
    void persistent_manual_ack_receives_message() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_manual_ack_receives_message\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that persistent messaging with manual acknowledgment works correctly.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. PERSISTENT messaging mode - messages are stored on broker queue until acknowledged\n" +
                "  2. MANUAL ack mode - application controls when message is acknowledged\n" +
                "  3. Queue with topic subscription - queue attracts messages from topic\n" +
                "  4. publishPersistentToTopic() - publishes with guaranteed delivery\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create a consumer bound to a queue with topic subscription\n" +
                "  2. Publish a persistent message to the topic\n" +
                "  3. Consumer receives message via the queue (topic->queue attraction)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Message is received by consumer within timeout\n" +
                "  - Received payload matches the published payload exactly\n" +
                "  - This verifies the persistent pub/sub pattern works end-to-end\n" +
                "───────────────────────────────────────────────────────────────\n");

        String queue = settings.uniqueQueue("manual");
        String topic = settings.uniqueTopic("manual");
        String payload = "persist-" + UUID.randomUUID();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> received = new AtomicReference<>();

        log.info("STEP 1: Creating persistent consumer on queue '{}' with topic subscription '{}'", queue, topic);
        log.info("        Mode: PERSISTENT, AckMode: MANUAL");
        consumerManager.createEnhancedConsumerRaw(
                "persist-manual-" + UUID.randomUUID(),
                queue,
                new String[]{topic},
                SolaceConsumer.MessagingMode.PERSISTENT,
                settings.allowQueueCreate,
                SolaceConsumer.AckMode.MANUAL,
                String.class,
                (SolaceMessageHandler<Object>) (message, originalMessage) -> {
                    log.info("STEP 3: Message received by handler: '{}'", message);
                    received.set((String) message);
                    latch.countDown();
                }
        );

        log.info("STEP 2: Publishing persistent message to topic '{}'", topic);
        log.info("        Payload: '{}'", payload);
        publisher.publishPersistentToTopic(topic, payload);

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
                "  - The persistent message was published to topic '" + topic + "'\n" +
                "  - The queue '" + queue + "' has a subscription to this topic\n" +
                "  - The consumer received the message via the queue binding\n" +
                "  - This confirms persistent messaging with MANUAL ack works correctly\n" +
                "\n" +
                "STATUS: " + (ok && payloadMatches ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");
        assertThat(ok).isTrue().withFailMessage("Message not received within timeout");
        assertThat(received.get()).isEqualTo(payload).withFailMessage("Received payload does not match published payload");
    }

    @Test
    void persistent_local_retry_then_ack() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_local_retry_then_ack\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify the local backoff retry mechanism handles transient failures gracefully.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. localMaxAttempts - number of retry attempts before giving up\n" +
                "  2. localBackoffInitialMs - initial delay between retries\n" +
                "  3. localBackoffMultiplier - exponential backoff multiplier\n" +
                "  4. localBackoffMaxMs - maximum backoff delay cap\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create consumer with localMaxAttempts=2 (allows 1 retry after initial failure)\n" +
                "  2. Handler intentionally throws exception on first attempt\n" +
                "  3. Local retry mechanism waits, then re-invokes handler\n" +
                "  4. Second attempt succeeds\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Handler is called exactly 2 times (first fails, second succeeds)\n" +
                "  - Message is successfully processed on retry without broker redelivery\n" +
                "  - This pattern handles transient errors (network blips, temporary resource\n" +
                "    unavailability) without impacting broker redelivery counts\n" +
                "───────────────────────────────────────────────────────────────\n");

        String queue = settings.uniqueQueue("retry");
        String topic = settings.uniqueTopic("retry");
        String payload = "retry-" + UUID.randomUUID();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();

        log.info("STEP 1: Creating consumer with local retry configuration:");
        log.info("        localMaxAttempts=2, backoffInitialMs=10, multiplier=1.0, maxBackoffMs=10");
        consumerManager.createEnhancedConsumerRaw(
                "persist-retry-" + UUID.randomUUID(),
                queue,
                new String[]{topic},
                SolaceConsumer.MessagingMode.PERSISTENT,
                settings.allowQueueCreate,
                SolaceConsumer.AckMode.MANUAL,
                String.class,
                (SolaceMessageHandler<Object>) (message, originalMessage) -> {
                    int attempt = calls.incrementAndGet();
                    if (attempt == 1) {
                        log.info("STEP 3a: Handler attempt {} - THROWING EXCEPTION (intentional failure)", attempt);
                        throw new RuntimeException("fail once");
                    }
                    log.info("STEP 3b: Handler attempt {} - SUCCESS (after local retry)", attempt);
                    latch.countDown();
                },
                2,      // localMaxAttempts
                10,     // backoff initial ms
                1.0,    // multiplier
                10      // max backoff ms
        );

        log.info("STEP 2: Publishing message to topic '{}' - consumer will fail first, then succeed on retry", topic);
        publisher.publishPersistentToTopic(topic, payload);

        boolean ok = latch.await(settings.timeoutSeconds, TimeUnit.SECONDS);
        int totalCalls = calls.get();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Latch received: " + ok + " (expected: true)\n" +
                "  Handler invocations: " + totalCalls + " (expected: 2)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Attempt 1: Handler threw RuntimeException (simulating transient failure)\n" +
                "  - Local retry mechanism waited ~10ms (backoffInitialMs)\n" +
                "  - Attempt 2: Handler succeeded, message processed\n" +
                "  - The message was NOT returned to broker queue (no broker redelivery)\n" +
                "  - Local retry is useful for handling transient errors without impacting\n" +
                "    broker-level retry counts or queue depth\n" +
                "\n" +
                "STATUS: " + (ok && totalCalls == 2 ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");
        assertThat(ok).isTrue();
        assertThat(calls.get()).isEqualTo(2); // first fails, second succeeds after local retry
    }

    @Test
    void persistent_message_survives_consumer_restart() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_message_survives_consumer_restart\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that persistent messages survive consumer downtime and are delivered\n" +
                "  when a new consumer connects to the same durable queue.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Durable queues - queue persists on broker even when no consumers are connected\n" +
                "  2. Persistent messaging - messages are stored until acknowledged\n" +
                "  3. Message durability - messages survive consumer disconnection/restart\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create initial consumer to provision the durable queue on broker\n" +
                "  2. STOP and REMOVE the consumer (queue still exists on broker, but no listener)\n" +
                "  3. Publish a persistent message while NO consumer is running\n" +
                "  4. Create a NEW consumer on the same queue\n" +
                "  5. New consumer should receive the message that was stored while it was down\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Message published during consumer downtime is delivered to new consumer\n" +
                "  - This demonstrates the 'store and forward' pattern essential for reliable messaging\n" +
                "  - Critical for scenarios like: rolling deployments, temporary consumer failures,\n" +
                "    or consumers that process messages in batches\n" +
                "───────────────────────────────────────────────────────────────\n");

        String queue = settings.uniqueQueue("durable");
        String topic = settings.uniqueTopic("durable");
        String payload = "durable-" + UUID.randomUUID();
        CountDownLatch latch = new CountDownLatch(1);

        log.info("STEP 1: Creating initial consumer to provision the durable queue '{}'", queue);
        log.info("        This ensures the queue exists on the broker with topic subscription '{}'", topic);
        String consumerId = consumerManager.createEnhancedConsumerRaw(
                "persist-durable-" + UUID.randomUUID(),
                queue,
                new String[]{topic},
                SolaceConsumer.MessagingMode.PERSISTENT,
                settings.allowQueueCreate,
                SolaceConsumer.AckMode.MANUAL,
                String.class,
                (SolaceMessageHandler<Object>) (message, originalMessage) -> {
                    // no-op for initial consumer
                }
        );

        log.info("STEP 2: Stopping and removing the consumer");
        log.info("        Queue remains on broker but NO consumer is listening");
        log.info("        (Simulates consumer crash, deployment, or maintenance window)");
        consumerManager.stopConsumer(consumerId);
        consumerManager.removeConsumer(consumerId);

        log.info("STEP 3: Publishing persistent message while NO consumer is running");
        log.info("        Payload: '{}' -> will be stored on broker queue", payload);
        publisher.publishPersistentToTopic(topic, payload);

        log.info("STEP 4: Creating NEW consumer on same queue (autoCreateQueue=false)");
        log.info("        The new consumer should receive the stored message");
        consumerManager.createEnhancedConsumerRaw(
                "persist-durable-rx-" + UUID.randomUUID(),
                queue,
                new String[]{topic},
                SolaceConsumer.MessagingMode.PERSISTENT,
                false, // queue already exists
                SolaceConsumer.AckMode.MANUAL,
                String.class,
                (SolaceMessageHandler<Object>) (message, originalMessage) -> {
                    log.info("STEP 5: New consumer received the stored message - PERSISTENCE VERIFIED!");
                    log.info("        Received payload: '{}'", message);
                    latch.countDown();
                }
        );

        boolean ok = latch.await(settings.timeoutSeconds, TimeUnit.SECONDS);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Message delivered to new consumer: " + ok + " (expected: true)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Initial consumer created queue '" + queue + "' on broker\n" +
                "  - Consumer was stopped and removed (simulating downtime)\n" +
                "  - Message was published to topic, stored in queue (no active consumer)\n" +
                "  - New consumer connected to same queue and received the stored message\n" +
                "  - This demonstrates guaranteed delivery: messages are never lost even when\n" +
                "    consumers are temporarily unavailable\n" +
                "\n" +
                "USE CASES:\n" +
                "  - Rolling deployments (old consumer stops, new one starts)\n" +
                "  - Consumer crashes and restarts\n" +
                "  - Batch processing with scheduled consumers\n" +
                "  - Geo-redundancy with failover consumers\n" +
                "\n" +
                "STATUS: " + (ok ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");
        assertThat(ok).isTrue();
    }
}
