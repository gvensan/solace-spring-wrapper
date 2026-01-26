package com.solace.wrapper.integration;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.config.SolaceAutoConfiguration;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.consumer.SolaceConsumerManager;
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
public class DirectReapplyIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(DirectReapplyIntegrationTest.class);

    @BeforeAll
    static void brokerCheck() { TestBroker.assumeAvailable(); }

    @Autowired
    SolaceConnectionManager cm;
    @Autowired
    SolaceConsumerManager consumers;
    @Autowired
    SolacePublisher publisher;

    @Test
    void direct_subscriptions_reapply_after_service_disconnect_reconnect() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: direct_subscriptions_reapply_after_service_disconnect_reconnect\n" +
                "PURPOSE: Verify DIRECT subscriptions are restored after disconnect/reconnect cycle\n" +
                "───────────────────────────────────────────────────────────────");

        String ns = "reapply/" + UUID.randomUUID();
        String topic = ns + "/one";

        AtomicInteger count = new AtomicInteger();
        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);

        log.info("STEP 1: Creating DIRECT consumer with topic subscription: " + ns + "/>");
        String consumerId = consumers.createEnhancedConsumerRaw(
                "reapply-cons-"+UUID.randomUUID(),
                "", new String[]{ns+"/>"},
                SolaceConsumer.MessagingMode.DIRECT,
                false,
                String.class,
                (SolaceMessageHandler<String>) (msg, in) -> {
                    int n = count.incrementAndGet();
                    if (n == 1) l1.countDown();
                    if (n == 2) l2.countDown();
                }
        );

        try {
            log.info("STEP 2: Publishing first message and verifying receipt");
            publisher.publishToTopic(topic, "m1");
            assertThat(l1.await(5, TimeUnit.SECONDS)).isTrue();
            log.info("        First message received successfully");

            log.info("STEP 3: Forcing disconnect and reconnect of underlying service");
            MessagingService svc = cm.getPrimaryService();
            svc.disconnect();
            TimeUnit.SECONDS.sleep(1);
            cm.reconnect();
            consumers.restartConsumer(consumerId);
            log.info("        Reconnected and consumer restarted");

            log.info("STEP 4: Publishing second message and verifying receipt after reapply");
            publisher.publishToTopic(topic, "m2");
            assertThat(l2.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(count.get()).isEqualTo(2);

            log.info("\n───────────────────────────────────────────────────────────────\n" +
                    "RESULT: Subscription reapply successful - received " + count.get() + " messages across disconnect\n" +
                    "───────────────────────────────────────────────────────────────\n");
        } finally {
            consumers.removeConsumer(consumerId);
        }
    }
}
