package com.solace.wrapper.integration;

import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.annotation.SolaceReplier;
import com.solace.wrapper.config.SolaceAutoConfiguration;
import com.solace.wrapper.exception.SolaceRequestTimeoutException;
import com.solace.wrapper.requestreply.SolaceRequestor;
import com.solace.wrapper.testutil.TestBroker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end request-reply round trip against a real broker (gated by {@link TestBroker}):
 * a {@code @SolaceReplier} answers a {@link SolaceRequestor} request, both blocking and async,
 * plus a timeout when no replier is listening.
 */
@SpringBootTest(classes = RequestReplyBrokerOptionalIT.Config.class)
@TestPropertySource(locations = "classpath:test-broker.properties")
public class RequestReplyBrokerOptionalIT {

    private static final Logger log = LoggerFactory.getLogger(RequestReplyBrokerOptionalIT.class);

    static final String ECHO_TOPIC = "rr/it/echo";

    @BeforeAll
    static void brokerCheck() { TestBroker.assumeAvailable(); }

    @Autowired
    SolaceRequestor requestor;

    @Test
    void blocking_round_trip_returns_reply() {
        log.info("TEST: blocking_round_trip_returns_reply — request -> @SolaceReplier -> reply");
        EchoReply reply = requestor.request(ECHO_TOPIC, new EchoRequest("hello"), EchoReply.class,
                Duration.ofSeconds(5));
        assertThat(reply).isNotNull();
        assertThat(reply.message()).isEqualTo("echo:hello");
    }

    @Test
    void async_round_trip_returns_reply() {
        log.info("TEST: async_round_trip_returns_reply");
        CompletableFuture<EchoReply> future =
                requestor.requestAsync(ECHO_TOPIC, new EchoRequest("async"), EchoReply.class, Duration.ofSeconds(5));
        assertThat(future).succeedsWithin(Duration.ofSeconds(6));
        assertThat(future.join().message()).isEqualTo("echo:async");
    }

    @Test
    void request_without_replier_times_out() {
        log.info("TEST: request_without_replier_times_out");
        String deadTopic = "rr/it/noreplier/" + UUID.randomUUID();
        assertThatThrownBy(() ->
                requestor.request(deadTopic, new EchoRequest("x"), EchoReply.class, Duration.ofMillis(800)))
                .isInstanceOf(SolaceRequestTimeoutException.class);
    }

    @Configuration
    @EnableSolaceAnnotations
    @Import(SolaceAutoConfiguration.class)
    static class Config {
        @Bean EchoReplier echoReplier() { return new EchoReplier(); }
    }

    @Service
    public static class EchoReplier {
        @SolaceReplier(topic = ECHO_TOPIC)
        public EchoReply echo(EchoRequest request) {
            return new EchoReply("echo:" + request.text());
        }
    }

    public record EchoRequest(String text) { }
    public record EchoReply(String message) { }
}
