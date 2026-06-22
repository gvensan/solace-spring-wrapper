package com.example.myapp.pricing;

import com.example.myapp.model.QuoteRequest;
import com.example.myapp.model.QuoteResponse;
import com.solace.wrapper.requestreply.SolaceRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Client side of the native request-reply pattern. The {@link SolaceRequestor} bean is
 * auto-configured by the wrapper — just inject it and call {@code request(...)}. It sends the request
 * to the pricing service's topic and blocks (or completes a future) on the correlated reply.
 *
 * <p>Demonstrates both the synchronous {@link SolaceRequestor#request(String, Object, Class, Duration)}
 * with an explicit timeout and the asynchronous
 * {@link SolaceRequestor#requestAsync(String, Object, Class)} returning a {@link CompletableFuture}.</p>
 */
@Component
public class QuoteClient {

    private static final Logger logger = LoggerFactory.getLogger(QuoteClient.class);
    private static final String QUOTE_TOPIC = "pricing/quote/v1";

    private final SolaceRequestor requestor;

    public QuoteClient(SolaceRequestor requestor) {
        this.requestor = requestor;
    }

    /** Blocking request with an explicit timeout. */
    public QuoteResponse getQuote(String sku, int qty, String tier) {
        QuoteRequest request = new QuoteRequest(sku, qty, tier);
        logger.info("Requesting quote (sync) for {} x {} [{}]", qty, sku, tier);
        QuoteResponse response = requestor.request(QUOTE_TOPIC, request, QuoteResponse.class, Duration.ofSeconds(5));
        logger.info("Received quote (sync): {}", response);
        return response;
    }

    /** Non-blocking request returning a future the caller can compose on. */
    public CompletableFuture<QuoteResponse> getQuoteAsync(String sku, int qty, String tier) {
        QuoteRequest request = new QuoteRequest(sku, qty, tier);
        logger.info("Requesting quote (async) for {} x {} [{}]", qty, sku, tier);
        return requestor.requestAsync(QUOTE_TOPIC, request, QuoteResponse.class)
                .whenComplete((resp, err) -> {
                    if (err != null) {
                        logger.error("Async quote failed for {}", sku, err);
                    } else {
                        logger.info("Received quote (async): {}", resp);
                    }
                });
    }
}
