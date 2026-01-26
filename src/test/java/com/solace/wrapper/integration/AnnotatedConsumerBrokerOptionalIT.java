package com.solace.wrapper.integration;

import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.config.SolaceAutoConfiguration;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.testutil.TestBroker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = AnnotatedConsumerBrokerOptionalIT.Config.class)
@TestPropertySource(locations = "classpath:test-broker.properties")
public class AnnotatedConsumerBrokerOptionalIT {

    private static final Logger log = LoggerFactory.getLogger(AnnotatedConsumerBrokerOptionalIT.class);

    @BeforeAll
    static void brokerCheck() { TestBroker.assumeAvailable(); }

    @Autowired
    SolacePublisher pub;
    @Autowired
    ListenerBean bean;

    @Test
    void annotated_direct_consumer_receives() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: annotated_direct_consumer_receives\n" +
                "PURPOSE: Verify @SolaceConsumer annotation on bean method receives DIRECT messages\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Installing latch on ListenerBean to track message receipt");
        CountDownLatch latch = bean.installLatch(1);

        log.info("STEP 2: Publishing message to topic: " + ListenerBean.TOPIC_BASE + "/m1");
        pub.publishToTopic(ListenerBean.TOPIC_BASE + "/m1", "hi");

        log.info("STEP 3: Waiting for annotated consumer to receive message");
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(bean.count.get()).isEqualTo(1);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: @SolaceConsumer annotated method received message successfully\n" +
                "        count=" + bean.count.get() + "\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Configuration
    @EnableSolaceAnnotations
    @Import(SolaceAutoConfiguration.class)
    static class Config {
        @Bean ListenerBean listenerBean() { return new ListenerBean(); }
    }

    @Service
    public static class ListenerBean {
        static final String TOPIC_BASE = "ac/test";
        final AtomicInteger count = new AtomicInteger();
        volatile CountDownLatch latch;

        CountDownLatch installLatch(int n) { return (this.latch = new CountDownLatch(n)); }

        @SolaceConsumer(topics = {TOPIC_BASE + "/>"}, mode = SolaceConsumer.MessagingMode.DIRECT)
        public void onMessage(String body) {
            count.incrementAndGet();
            if (latch != null) latch.countDown();
        }
    }
}
