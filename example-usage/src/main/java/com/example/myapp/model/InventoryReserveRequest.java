package com.example.myapp.model;

/**
 * Command asking the inventory service to reserve stock for an order. Produced by transforming an
 * {@link OrderCreated} event and published to {@code inventory/reserve} (persistent).
 */
public class InventoryReserveRequest {
    public String orderId;
    public String customerId;
    public String sku;
    public int qty;
    public double amount;
    public String orderType;

    public InventoryReserveRequest() {}
}
