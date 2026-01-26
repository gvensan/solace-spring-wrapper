package com.solace.wrapper.integration;

import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.config.SolaceAutoConfiguration;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceManualAckMessageHandler;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.testutil.TestBroker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = SolaceAutoConfiguration.class)
@TestPropertySource(locations = "classpath:test-broker.properties")
public class PersistentAckModeIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(PersistentAckModeIntegrationTest.class);

    @BeforeAll
    static void brokerCheck() { TestBroker.assumeAvailable(); }

    @Autowired
    SolaceConsumerManager consumers;
    @Autowired
    SolacePublisher publisher;

    @Test
    void manual_ack_triggers_redelivery_on_failure() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: manual_ack_triggers_redelivery_on_failure\n" +
                "PURPOSE: Verify MANUAL ack mode with ack.fail() triggers broker redelivery\n" +
                "───────────────────────────────────────────────────────────────");

        String ns = "ackm/" + UUID.randomUUID();
        String q = "q.ack." + UUID.randomUUID();
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        log.info("STEP 1: Creating PERSISTENT consumer with MANUAL ack that fails first 2 attempts");
        String consumerId = consumers.createEnhancedConsumerRaw(
                "ack-manual-"+UUID.randomUUID(),
                q, new String[]{ns+"/>"},
                SolaceConsumer.MessagingMode.PERSISTENT,
                true,
                SolaceConsumer.AckMode.MANUAL,
                String.class,
                (SolaceManualAckMessageHandler<String>) (msg, in, ack) -> {
                    if (attempts.incrementAndGet() < 3) {
                        ack.fail();
                        return;
                    }
                    ack.ack();
                    done.countDown();
                }
        );

        try {
            log.info("STEP 2: Publishing persistent message - should be redelivered on failure");
            publisher.publishPersistentToTopic(ns+"/m", "retry");

            log.info("STEP 3: Waiting for successful processing after redeliveries");
            boolean ok = done.await(30, TimeUnit.SECONDS);
            assertThat(ok).isTrue();
            assertThat(attempts.get()).isGreaterThanOrEqualTo(3);

            log.info("\n───────────────────────────────────────────────────────────────\n" +
                    "RESULT: MANUAL ack redelivery works - attempts=" + attempts.get() + " (failed twice, succeeded on 3rd)\n" +
                    "───────────────────────────────────────────────────────────────\n");
        } finally {
            consumers.removeConsumer(consumerId);
        }
    }

    @Test
    void auto_ack_no_redelivery_on_failure() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: auto_ack_no_redelivery_on_failure\n" +
                "PURPOSE: Verify AUTO ack mode does NOT redeliver on handler exception (message is lost)\n" +
                "───────────────────────────────────────────────────────────────");

        String ns = "acka/" + UUID.randomUUID();
        String q = "q.acka." + UUID.randomUUID();
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        log.info("STEP 1: Creating PERSISTENT consumer with AUTO ack that always throws exception");
        String consumerId = consumers.createEnhancedConsumerRaw(
                "ack-auto-"+UUID.randomUUID(),
                q, new String[]{ns+"/>"},
                SolaceConsumer.MessagingMode.PERSISTENT,
                true,
                SolaceConsumer.AckMode.AUTO,
                String.class,
                (SolaceMessageHandler<String>) (msg, in) -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException("will-not-redeliver");
                }
        );

        try {
            log.info("STEP 2: Publishing persistent message");
            publisher.publishPersistentToTopic(ns+"/m", "once");

            log.info("STEP 3: Waiting to verify no redelivery occurs (AUTO ack already confirmed)");
            TimeUnit.SECONDS.sleep(5);
            assertThat(attempts.get()).isEqualTo(1);
            done.countDown();

            log.info("\n───────────────────────────────────────────────────────────────\n" +
                    "RESULT: AUTO ack mode - no redelivery on failure, attempts=" + attempts.get() + "\n" +
                    "───────────────────────────────────────────────────────────────\n");
        } finally {
            consumers.removeConsumer(consumerId);
        }
    }
}
