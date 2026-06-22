package com.example.samples.requestreply;

import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.annotation.SolaceReplier;
import com.solace.wrapper.requestreply.SolaceRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * Sample 06 — Native request-reply.
 *
 * <p>Two halves, both in this one app so it is self-demonstrating:</p>
 * <ul>
 *   <li><b>Server</b> — {@link SolaceReplier}: the annotated method's <b>return value is the reply</b>.
 *       Correlation and the reply-to inbox are handled automatically by the Solace API.</li>
 *   <li><b>Client</b> — {@link SolaceRequestor} (auto-configured bean): {@code request(...)} blocks for
 *       the reply (with a timeout); {@code requestAsync(...)} returns a {@link java.util.concurrent.CompletableFuture}.</li>
 * </ul>
 *
 * <p>The app issues one sync and one async request, prints the replies, then stays up so the replier
 * keeps serving. Ctrl+C to stop.</p>
 */
@SpringBootApplication
@EnableSolaceAnnotations
public class RequestReplyApplication {

    private static final String QUOTE_TOPIC = "samples/pricing/quote";

    public static void main(String[] args) {
        SpringApplication.run(RequestReplyApplication.class, args);
    }

    /** Server side. */
    @Service
    static class PricingReplier {
        private static final Logger log = LoggerFactory.getLogger(PricingReplier.class);

        @SolaceReplier(topic = QUOTE_TOPIC, replierIdPrefix = "pricing")
        public QuoteResponse quote(QuoteRequest request) {
            double unit = "VIP".equalsIgnoreCase(request.tier) ? 8.50 : 10.00;
            QuoteResponse response = new QuoteResponse(request.sku, request.qty, unit * request.qty);
            log.info("🧮 Quoting {} x {} [{}] -> {}", request.qty, request.sku, request.tier, response.total);
            return response; // returned value is sent back as the reply
        }
    }

    /** Client side. */
    @Service
    static class Runner implements CommandLineRunner {
        private static final Logger log = LoggerFactory.getLogger(Runner.class);
        private final SolaceRequestor requestor;

        Runner(SolaceRequestor requestor) {
            this.requestor = requestor;
        }

        @Override
        public void run(String... args) throws Exception {
            log.info("=== Sample 06: request-reply ===");
            Thread.sleep(1500); // let the replier subscribe

            // Synchronous request with a timeout.
            QuoteResponse sync = requestor.request(
                    QUOTE_TOPIC, new QuoteRequest("WIDGET", 3, "STANDARD"),
                    QuoteResponse.class, Duration.ofSeconds(5));
            log.info("Sync reply: {} x {} = {}", sync.qty, sync.sku, sync.total);

            // Asynchronous request.
            requestor.requestAsync(QUOTE_TOPIC, new QuoteRequest("WIDGET", 3, "VIP"), QuoteResponse.class)
                    .thenAccept(r -> log.info("Async reply: {} x {} = {}", r.qty, r.sku, r.total))
                    .join();

            log.info("Done issuing requests. Ctrl+C to stop.");
            new CountDownLatch(1).await();
        }
    }

    public static class QuoteRequest {
        public String sku;
        public int qty;
        public String tier;

        public QuoteRequest() {}

        public QuoteRequest(String sku, int qty, String tier) {
            this.sku = sku;
            this.qty = qty;
            this.tier = tier;
        }
    }

    public static class QuoteResponse {
        public String sku;
        public int qty;
        public double total;

        public QuoteResponse() {}

        public QuoteResponse(String sku, int qty, double total) {
            this.sku = sku;
            this.qty = qty;
            this.total = total;
        }
    }
}
