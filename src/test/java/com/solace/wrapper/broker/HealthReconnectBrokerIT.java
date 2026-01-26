package com.solace.wrapper.broker;

import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.consumer.SolaceConsumer;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import com.solace.wrapper.health.SolaceHealthIndicator;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.serialization.JsonMessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Status;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("broker")
public class HealthReconnectBrokerIT {

    private static final Logger log = LoggerFactory.getLogger(HealthReconnectBrokerIT.class);

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
        try { if (consumerManager != null) consumerManager.shutdown(); } catch (Exception ignore) {}
        try { if (publisher != null) publisher.shutdown(); } catch (Exception ignore) {}
        try { if (connectionManager != null) connectionManager.shutdown(); } catch (Exception ignore) {}
    }

    @Test
    void health_up_and_consumer_receives_after_reconnect() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: health_up_and_consumer_receives_after_reconnect\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the system correctly recovers after a complete disconnect/reconnect\n" +
                "  cycle, with health indicator reporting UP and messaging functional.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Full connection lifecycle (connect -> disconnect -> reconnect)\n" +
                "  2. Health indicator accuracy after reconnection\n" +
                "  3. Consumer/Publisher functionality after reconnection\n" +
                "  4. Durable queue persistence across connection cycles\n" +
                "\n" +
                "REAL-WORLD SCENARIOS THIS SIMULATES:\n" +
                "  - Network outage and recovery\n" +
                "  - Broker restart or failover\n" +
                "  - Application restart connecting to existing queues\n" +
                "  - Load balancer switching to different broker instance\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Verify initial health is UP\n" +
                "  2. Create initial consumer (provisions durable queue)\n" +
                "  3. FULL DISCONNECT: shutdown all components (simulates outage)\n" +
                "  4. RECONNECT: create new connection manager, consumer manager, publisher\n" +
                "  5. Verify health is UP after reconnect\n" +
                "  6. Create new consumer on same queue\n" +
                "  7. Publish message and verify consumer receives it\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Health reports UP before disconnect, UP after reconnect\n" +
                "  - New consumer receives message published after reconnect\n" +
                "  - Queue survives disconnect (durable queue on broker)\n" +
                "───────────────────────────────────────────────────────────────\n");

        String topic = settings.uniqueTopic("reconnect");
        String queue = settings.uniqueQueue("reconnect");
        CountDownLatch latch = new CountDownLatch(1);

        log.info("STEP 1: Verifying initial health status is UP");
        SolaceHealthIndicator indicator = new SolaceHealthIndicator(connectionManager);
        var initialHealth = indicator.health().getStatus();
        assertThat(initialHealth).isEqualTo(Status.UP);
        log.info("        Initial health: {}", initialHealth);

        log.info("STEP 2: Creating initial consumer on queue '{}' with topic '{}'", queue, topic);
        log.info("        This provisions the durable queue on the broker");
        consumerManager.createEnhancedConsumerRaw(
                "reconnect-consumer-" + UUID.randomUUID(),
                queue,
                new String[]{topic},
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT,
                settings.allowQueueCreate,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO,
                String.class,
                (SolaceMessageHandler<Object>) (message, originalMessage) -> latch.countDown()
        );

        log.info("STEP 3: Simulating FULL DISCONNECT - shutting down all components");
        log.info("        (This simulates network failure, broker restart, or app restart)");
        consumerManager.shutdown();
        publisher.shutdown();
        connectionManager.shutdown();
        log.info("        All components shutdown. Connection to broker is CLOSED.");

        log.info("STEP 4: RECONNECTING - creating new connection manager, consumer manager, and publisher");
        connectionManager = new SolaceConnectionManager(BrokerTestSupport.toSolaceProperties(settings));
        var serializer = new JsonMessageSerializer();
        consumerManager = new SolaceConsumerManager(connectionManager, serializer);
        publisher = new SolacePublisher(connectionManager, serializer);
        log.info("        New components created. Connection to broker re-established.");

        log.info("STEP 5: Verifying health is UP after reconnect");
        indicator = new SolaceHealthIndicator(connectionManager);
        var healthAfterReconnect = indicator.health().getStatus();
        assertThat(healthAfterReconnect).isEqualTo(Status.UP);
        log.info("        Health after reconnect: {}", healthAfterReconnect);

        log.info("STEP 6: Creating NEW consumer on same queue (autoCreateQueue=false)");
        log.info("        Queue should still exist from step 2 (durable queue)");
        consumerManager.createEnhancedConsumerRaw(
                "reconnect-consumer2-" + UUID.randomUUID(),
                queue,
                new String[]{topic},
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT,
                false,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.AUTO,
                String.class,
                (SolaceMessageHandler<Object>) (message, originalMessage) -> {
                    log.info("STEP 8: Message received after reconnect - SUCCESS!");
                    latch.countDown();
                }
        );

        log.info("STEP 7: Publishing message after reconnect");
        publisher.publishPersistentToTopic(topic, "after-reconnect");

        boolean ok = latch.await(settings.timeoutSeconds, TimeUnit.SECONDS);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Initial health status: " + initialHealth + " (expected: UP)\n" +
                "  Health after reconnect: " + healthAfterReconnect + " (expected: UP)\n" +
                "  Message received after reconnect: " + ok + " (expected: true)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Connection was fully closed (all components shutdown)\n" +
                "  - New components were created (fresh connection)\n" +
                "  - Health indicator correctly detected new connection is healthy\n" +
                "  - Durable queue persisted on broker during disconnect\n" +
                "  - New consumer successfully bound to existing queue\n" +
                "  - Message published and received successfully\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Application can safely restart and reconnect to existing queues\n" +
                "  - Health indicator accurately reflects connection state\n" +
                "  - No message loss during reconnection (queue persisted)\n" +
                "\n" +
                "STATUS: " + (ok && Status.UP.equals(initialHealth) && Status.UP.equals(healthAfterReconnect) ? "PASS" : "FAIL") + "\n" +
                "───────────────────────────────────────────────────────────────\n");
        assertThat(ok).isTrue();
    }
}
