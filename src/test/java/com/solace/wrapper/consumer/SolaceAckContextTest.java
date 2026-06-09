package com.solace.wrapper.consumer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SolaceAckContext} covering the single-completion guarantee and the
 * roll-back behavior when the underlying ack/fail action throws.
 */
class SolaceAckContextTest {

    @Test
    void ack_runs_action_once_and_records_status() {
        AtomicInteger acks = new AtomicInteger();
        AtomicInteger fails = new AtomicInteger();
        SolaceAckContext ctx = new SolaceAckContext(acks::incrementAndGet, fails::incrementAndGet);

        assertThat(ctx.getStatus()).isEqualTo(SolaceAckContext.Status.NONE);
        assertThat(ctx.isCompleted()).isFalse();

        assertThat(ctx.ack()).isTrue();

        assertThat(ctx.getStatus()).isEqualTo(SolaceAckContext.Status.ACKED);
        assertThat(ctx.isCompleted()).isTrue();
        assertThat(acks.get()).isEqualTo(1);
        assertThat(fails.get()).isZero();
    }

    @Test
    void fail_runs_fail_action_once() {
        AtomicInteger acks = new AtomicInteger();
        AtomicInteger fails = new AtomicInteger();
        SolaceAckContext ctx = new SolaceAckContext(acks::incrementAndGet, fails::incrementAndGet);

        assertThat(ctx.fail()).isTrue();

        assertThat(ctx.getStatus()).isEqualTo(SolaceAckContext.Status.FAILED);
        assertThat(fails.get()).isEqualTo(1);
        assertThat(acks.get()).isZero();
    }

    @Test
    void second_completion_is_ignored() {
        AtomicInteger acks = new AtomicInteger();
        AtomicInteger fails = new AtomicInteger();
        SolaceAckContext ctx = new SolaceAckContext(acks::incrementAndGet, fails::incrementAndGet);

        assertThat(ctx.ack()).isTrue();
        // Subsequent ack/fail must be no-ops returning false and must not run the action again.
        assertThat(ctx.ack()).isFalse();
        assertThat(ctx.fail()).isFalse();

        assertThat(ctx.getStatus()).isEqualTo(SolaceAckContext.Status.ACKED);
        assertThat(acks.get()).isEqualTo(1);
        assertThat(fails.get()).isZero();
    }

    @Test
    void action_exception_rolls_back_status_and_propagates() {
        SolaceAckContext ctx = new SolaceAckContext(
                () -> { throw new IllegalStateException("ack boom"); },
                () -> { /* no-op */ });

        assertThatThrownBy(ctx::ack)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ack boom");

        // Status must roll back so the caller (or framework) can still settle the message.
        assertThat(ctx.getStatus()).isEqualTo(SolaceAckContext.Status.NONE);
        assertThat(ctx.isCompleted()).isFalse();

        // After a failed ack, a subsequent fail() should still succeed.
        AtomicInteger fails = new AtomicInteger();
        SolaceAckContext ctx2 = new SolaceAckContext(
                () -> { throw new IllegalStateException("ack boom"); },
                fails::incrementAndGet);
        assertThatThrownBy(ctx2::ack).isInstanceOf(IllegalStateException.class);
        assertThat(ctx2.fail()).isTrue();
        assertThat(fails.get()).isEqualTo(1);
    }
}
