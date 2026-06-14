package com.example.myapp.model;

/**
 * Request for a real-time price quote. Sent by {@link com.example.myapp.pricing.QuoteClient}
 * through the {@code SolaceRequestor} and answered by the {@code @SolaceReplier} pricing service.
 */
public class QuoteRequest {
    public String sku;
    public int qty;
    public String customerTier;

    public QuoteRequest() {}

    public QuoteRequest(String sku, int qty, String customerTier) {
        this.sku = sku;
        this.qty = qty;
        this.customerTier = customerTier;
    }
}
