package com.example.myapp.model;

/**
 * Command asking the billing service to charge for a reserved order. Published to
 * {@code billing/charge} (persistent) by the inventory service after a successful reservation.
 */
public class ChargeRequest {
    public String orderId;
    public String customerId;
    public double amount;
    public String orderType;

    public ChargeRequest() {}
}
