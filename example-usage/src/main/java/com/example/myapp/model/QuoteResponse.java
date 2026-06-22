package com.example.myapp.model;

/**
 * Reply to a {@link QuoteRequest}. Returned directly from the {@code @SolaceReplier} method —
 * the wrapper serializes it and sends it back on the request's reply-to inbox automatically.
 */
public class QuoteResponse {
    public String sku;
    public int qty;
    public double unitPrice;
    public double totalPrice;
    public String currency;

    public QuoteResponse() {}

    @Override
    public String toString() {
        return String.format("QuoteResponse{sku='%s', qty=%d, unitPrice=%.2f, total=%.2f %s}",
                sku, qty, unitPrice, totalPrice, currency);
    }
}
