package com.solace.wrapper.integration;

import com.solace.wrapper.config.SolaceAutoConfiguration;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceManualAckMessageHandler;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import com.solace.wrapper.health.SolaceHealthIndicator;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.testutil.TestBroker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = SolaceAutoConfiguration.class)
@TestPropertySource(locations = "classpath:test-broker.properties")
public class LocalBrokerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(LocalBrokerIntegrationTest.class);

    @BeforeAll
    static void brokerCheck() { TestBroker.assumeAvailable(); }

    @Autowired
    SolacePublisher pub;
    @Autowired
    SolaceConsumerManager consumers;
    @Autowired(required = false)
    SolaceHealthIndicator health;

    @Test
    void direct_publish_and_receive() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: direct_publish_and_receive\n" +
                "PURPOSE: Verify DIRECT messaging mode - publish to topic and receive via subscription\n" +
                "───────────────────────────────────────────────────────────────");

        String ns = "it/" + UUID.randomUUID();
        String topic = ns + "/one";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> received = new AtomicReference<>();

        log.info("STEP 1: Creating DIRECT consumer with topic subscription: " + ns + "/>");
        String consumerId = consumers.createEnhancedConsumerRaw(
                "it-direct-" + UUID.randomUUID(),
                "", new String[]{ns+"/>"},
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.DIRECT,
                false,
                String.class,
                (SolaceMessageHandler<String>) (msg, inbound) -> { received.set(msg); latch.countDown(); }
        );

        try {
            log.info("STEP 2: Publishing message to topic: " + topic);
            pub.publishToTopic(topic, "hello");

            log.info("STEP 3: Waiting for message receipt");
            boolean ok = latch.await(5, TimeUnit.SECONDS);
            assertThat(ok).isTrue();
            assertThat(received.get()).isNotNull();

            log.info("\n───────────────────────────────────────────────────────────────\n" +
                    "RESULT: DIRECT publish and receive successful\n" +
                    "        received: " + received.get() + "\n" +
                    "───────────────────────────────────────────────────────────────\n");
        } finally {
            consumers.removeConsumer(consumerId);
        }
    }

    @Test
    void persistent_publish_confirm_and_consume_manual_ack() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_publish_confirm_and_consume_manual_ack\n" +
                "PURPOSE: Verify PERSISTENT messaging with publisher confirmation and MANUAL ack consumer\n" +
                "───────────────────────────────────────────────────────────────");

        String ns = "itp/" + UUID.randomUUID();
        String topic = ns + "/one";
        String queue = "q.it." + UUID.randomUUID();
        CountDownLatch latch = new CountDownLatch(1);

        log.info("STEP 1: Creating PERSISTENT consumer with MANUAL ack on queue: " + queue);
        String consumerId = consumers.createEnhancedConsumerRaw(
                "it-persist-" + UUID.randomUUID(),
                queue,
                new String[]{ns+"/>"},
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.PERSISTENT,
                true,
                com.solace.wrapper.annotation.SolaceConsumer.AckMode.MANUAL,
                String.class,
                (SolaceManualAckMessageHandler<String>) (msg, inbound, ack) -> {
                    ack.ack();
                    latch.countDown();
                }
        );

        try {
            log.info("STEP 2: Publishing persistent message with await confirmation to topic: " + topic);
            pub.publishPersistentToTopicAwait(topic, "p-hello", java.time.Duration.ofSeconds(10));

            log.info("STEP 3: Waiting for message receipt and manual ACK");
            boolean ok = latch.await(10, TimeUnit.SECONDS);
            assertThat(ok).isTrue();

            log.info("\n───────────────────────────────────────────────────────────────\n" +
                    "RESULT: PERSISTENT publish with confirmation and MANUAL ack successful\n" +
                    "───────────────────────────────────────────────────────────────\n");
        } finally {
            consumers.removeConsumer(consumerId);
        }
    }

    @Test
    void health_indicator_exposes_details() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: health_indicator_exposes_details\n" +
                "PURPOSE: Verify SolaceHealthIndicator exposes connection details for actuator\n" +
                "───────────────────────────────────────────────────────────────");

        if (health == null) {
            log.info("SKIP: Spring Actuator not available - health indicator is optional");
            return;
        }

        log.info("STEP 1: Retrieving health indicator status");
        Health h = health.health();

        log.info("STEP 2: Verifying health details contain expected keys");
        assertThat(h.getDetails()).containsKeys("host", "vpn", "clientName",
                "primaryConnected", "consumerServices.total");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Health indicator exposes required details\n" +
                "        details: " + h.getDetails() + "\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}
