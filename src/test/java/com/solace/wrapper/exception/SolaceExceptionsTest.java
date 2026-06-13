package com.solace.wrapper.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers both constructors of each Solace exception type (message-only and message+cause).
 */
class SolaceExceptionsTest {

    @Test
    void connection_exception_constructors() {
        SolaceConnectionException msgOnly = new SolaceConnectionException("conn");
        assertThat(msgOnly).hasMessage("conn").isInstanceOf(RuntimeException.class);
        assertThat(msgOnly.getCause()).isNull();

        Throwable cause = new IllegalStateException("root");
        SolaceConnectionException withCause = new SolaceConnectionException("conn2", cause);
        assertThat(withCause).hasMessage("conn2").hasCause(cause);
    }

    @Test
    void consumer_exception_constructors() {
        assertThat(new SolaceConsumerException("c")).hasMessage("c").hasNoCause();
        Throwable cause = new RuntimeException("r");
        assertThat(new SolaceConsumerException("c2", cause)).hasMessage("c2").hasCause(cause);
    }

    @Test
    void publish_exception_constructors() {
        assertThat(new SolacePublishException("p")).hasMessage("p").hasNoCause();
        Throwable cause = new RuntimeException("r");
        assertThat(new SolacePublishException("p2", cause)).hasMessage("p2").hasCause(cause);
    }

    @Test
    void replier_exception_constructors() {
        assertThat(new SolaceReplierException("rp")).hasMessage("rp").hasNoCause();
        Throwable cause = new RuntimeException("r");
        assertThat(new SolaceReplierException("rp2", cause)).hasMessage("rp2").hasCause(cause);
    }

    @Test
    void request_exception_constructors() {
        assertThat(new SolaceRequestException("rq")).hasMessage("rq").hasNoCause();
        Throwable cause = new RuntimeException("r");
        assertThat(new SolaceRequestException("rq2", cause)).hasMessage("rq2").hasCause(cause);
    }

    @Test
    void request_timeout_exception_constructors_and_hierarchy() {
        SolaceRequestTimeoutException t1 = new SolaceRequestTimeoutException("to");
        assertThat(t1).hasMessage("to").hasNoCause().isInstanceOf(SolaceRequestException.class);
        Throwable cause = new RuntimeException("r");
        assertThat(new SolaceRequestTimeoutException("to2", cause)).hasMessage("to2").hasCause(cause);
    }
}
