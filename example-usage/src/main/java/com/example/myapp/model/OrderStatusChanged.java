package com.example.myapp.model;

/**
 * Lifecycle status transition for an order, published to {@code orders/status/{orderId}}.
 * These are high-frequency, low-value "ticks" — a good fit for the {@code elidingEligible},
 * {@code messageExpiration} and {@code timeToLive} publish attributes.
 */
public class OrderStatusChanged {
    public String orderId;
    public String previousStatus;
    public String newStatus;
    public long sequence;
    public String changedAt;

    public OrderStatusChanged() {}

    @Override
    public String toString() {
        return String.format("OrderStatusChanged{orderId='%s', %s -> %s}", orderId, previousStatus, newStatus);
    }
}
