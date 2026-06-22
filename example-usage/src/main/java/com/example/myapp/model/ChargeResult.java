package com.example.myapp.model;

/**
 * Outcome of a billing charge, published to {@code billing/charged}. The notification service
 * filters on {@link #status} via a SpEL {@code condition}.
 */
public class ChargeResult {
    public String orderId;
    public String customerId;
    public double amount;
    /** {@code CHARGED} or {@code DECLINED}. */
    public String status;
    public String chargedAt;

    public ChargeResult() {}

    @Override
    public String toString() {
        return String.format("ChargeResult{orderId='%s', amount=%.2f, status='%s'}", orderId, amount, status);
    }
}
