package com.example.myapp.model;

/**
 * Event emitted when an order is cancelled. Publishing is conditional — the order service only
 * emits it when a cancellation reason is supplied (see the {@code condition} publish attribute).
 */
public class OrderCancelled {
    public String orderId;
    public String reason;
    public String cancelledAt;

    public OrderCancelled() {}

    @Override
    public String toString() {
        return String.format("OrderCancelled{orderId='%s', reason='%s'}", orderId, reason);
    }
}
