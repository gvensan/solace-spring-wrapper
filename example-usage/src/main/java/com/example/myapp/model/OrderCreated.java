package com.example.myapp.model;

/**
 * Event emitted when a new order is accepted. Published to {@code orders/created} by
 * {@link com.example.myapp.order.OrderService} and consumed (DIRECT mode) by the inventory service.
 *
 * <p>Plain Jackson-friendly POJO (no-arg constructor + public fields) so the wrapper's
 * {@code JsonMessageSerializer} can serialize and deserialize it without extra configuration.</p>
 */
public class OrderCreated {
    public String orderId;
    public String customerId;
    public String sku;
    public int qty;
    public double amount;
    /** {@code STANDARD} or {@code VIP} — drives routing, class-of-service and delivery-mode choices. */
    public String orderType;
    /** Monotonic sequence used to demonstrate the {@code sequenceNumber} publish attribute. */
    public long sequence;
    public String createdAt;

    public OrderCreated() {}

    @Override
    public String toString() {
        return String.format("OrderCreated{orderId='%s', customer='%s', sku='%s', qty=%d, amount=%.2f, type='%s'}",
                orderId, customerId, sku, qty, amount, orderType);
    }
}
