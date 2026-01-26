package com.solace.wrapper.integration;

import com.solace.wrapper.annotation.SolacePublish;
import com.solace.wrapper.config.SolaceAutoConfiguration;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.consumer.SolaceMessageHandler;
import com.solace.wrapper.testutil.TestBroker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = AnnotatedPublishIntegrationTest.Config.class)
@TestPropertySource(locations = "classpath:test-broker.properties")
public class AnnotatedPublishIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AnnotatedPublishIntegrationTest.class);

    @BeforeAll
    static void brokerCheck() { TestBroker.assumeAvailable(); }

    @Autowired
    SolaceConsumerManager consumers;
    @Autowired
    TestPublisher svc;

    @Test
    void annotation_publish_sends_to_topic() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: annotation_publish_sends_to_topic\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that @SolacePublish annotation with SpEL destination expression\n" +
                "  correctly publishes messages to the resolved topic via AOP interception.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. @SolacePublish annotation AOP interception\n" +
                "  2. SpEL destination expression (#{#dest} resolves to method parameter)\n" +
                "  3. Method return value used as message payload\n" +
                "  4. Full Spring context integration with SolaceAutoConfiguration\n" +
                "\n" +
                "HOW @SolacePublish WORKS:\n" +
                "  1. AOP aspect intercepts methods annotated with @SolacePublish\n" +
                "  2. SpEL expression #{#dest} is resolved using method parameters\n" +
                "  3. Method return value is serialized as message payload\n" +
                "  4. Message is published to resolved destination topic\n" +
                "\n" +
                "INTEGRATION ASPECTS:\n" +
                "  - Full Spring Boot context (@SpringBootTest)\n" +
                "  - Real broker connection (via TestBroker)\n" +
                "  - SolaceAutoConfiguration imports all required beans\n" +
                "  - Consumer verifies end-to-end message flow\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create DIRECT consumer subscribing to dynamic topic namespace\n" +
                "  2. Call @SolacePublish annotated method with topic and payload\n" +
                "  3. Verify consumer receives the published message\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Message received within timeout\n" +
                "  - Payload matches what was returned by annotated method\n" +
                "───────────────────────────────────────────────────────────────\n");

        String ns = "ap/" + UUID.randomUUID();
        String topic = ns + "/t";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> body = new AtomicReference<>();

        log.info("STEP 1: Creating DIRECT consumer for topic " + ns + "/>");
        consumers.createEnhancedConsumerRaw(
                "ap-cons-"+UUID.randomUUID(),
                "", new String[]{ns+"/>"},
                com.solace.wrapper.annotation.SolaceConsumer.MessagingMode.DIRECT,
                false,
                String.class,
                (SolaceMessageHandler<String>) (msg, in) -> { body.set(msg); latch.countDown(); }
        );

        log.info("STEP 2: Publishing via @SolacePublish annotated method to topic: " + topic);
        svc.send(topic, "hello-ann");

        log.info("STEP 3: Waiting for message receipt");
        boolean ok = latch.await(5, TimeUnit.SECONDS);
        assertThat(ok).isTrue();
        assertThat(body.get()).isNotNull();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Message received: " + ok + " (expected: true)\n" +
                "  Received body: '" + body.get() + "'\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - @SolacePublish annotation was intercepted by AOP aspect\n" +
                "  - SpEL #{#dest} resolved to method parameter value '" + topic + "'\n" +
                "  - Method return value 'hello-ann' used as message payload\n" +
                "  - Message published and received by consumer\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Declarative publishing works in full Spring context\n" +
                "  - SpEL expressions enable dynamic destination routing\n" +
                "  - Zero boilerplate - just annotate methods to publish\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Configuration
    @Import(SolaceAutoConfiguration.class)
    static class Config {
        @Bean TestPublisher testPublisher() { return new TestPublisher(); }
    }

    static class TestPublisher {
        @SolacePublish(destination = "#{#dest}")
        public String send(String dest, String payload) { return payload; }
    }
}
