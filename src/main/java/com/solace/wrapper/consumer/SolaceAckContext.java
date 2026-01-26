package com.solace.wrapper.consumer;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Manual acknowledgment context for persistent consumers.
 */
public final class SolaceAckContext {

    public enum Status { NONE, ACKED, FAILED }

    private final AtomicReference<Status> status = new AtomicReference<>(Status.NONE);
    private final Runnable ackAction;
    private final Runnable failAction;

    public SolaceAckContext(Runnable ackAction, Runnable failAction) {
        this.ackAction = ackAction;
        this.failAction = failAction;
    }

    public boolean ack() {
        return complete(Status.ACKED, ackAction);
    }

    public boolean fail() {
        return complete(Status.FAILED, failAction);
    }

    public Status getStatus() {
        return status.get();
    }

    public boolean isCompleted() {
        return status.get() != Status.NONE;
    }

    private boolean complete(Status next, Runnable action) {
        if (!status.compareAndSet(Status.NONE, next)) {
            return false;
        }
        try {
            action.run();
            return true;
        } catch (RuntimeException ex) {
            status.compareAndSet(next, Status.NONE);
            throw ex;
        }
    }
}
