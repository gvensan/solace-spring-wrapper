package com.example.samples.publishspel;

import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.annotation.SolacePublish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

/**
 * Sample 02 — SpEL in {@link SolacePublish}.
 *
 * <p>Three things every non-trivial publisher needs:</p>
 * <ul>
 *   <li><b>Dynamic destination</b> — route by data using a SpEL ternary over the return value.</li>
 *   <li><b>Conditional publish</b> — {@code condition} suppresses the send when it is false.</li>
 *   <li><b>Named parameters</b> — reference method args by name (e.g. {@code #orderId}); requires the
 *       {@code -parameters} compiler flag, which this module's parent pom enables.</li>
 * </ul>
 *
 * <p>Publish attributes use Spring <b>template</b> syntax — wrap expressions in {@code #{ ... }}.</p>
 */
@SpringBootApplication
@EnableSolaceAnnotations
public class PublishSpelApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublishSpelApplication.class, args);
    }

    @Service
    static class PricedOrderPublisher {
        private static final Logger log = LoggerFactory.getLogger(PricedOrderPublisher.class);

        /** Routes VIP vs standard onto different topics and copies fields into user properties. */
        @SolacePublish(
                destination = "samples/orders/created/#{result.tier == 'VIP' ? 'vip' : 'standard'}",
                deliveryMode = "PERSISTENT",   // reaches both the direct (03) and queue (04) consumer samples
                correlationId = "#{result.orderId}",
                userProperties = {
                        "tier=#{result.tier}",
                        "amount=#{result.amount}"
                }
        )
        public Order place(String orderId, String tier, double amount) {
            log.info("Placing order {} (tier={})", orderId, tier);
            return new Order(orderId, tier, amount);
        }

        /** Only publishes when a reason is supplied — note the named param {@code #reason}. */
        @SolacePublish(
                destination = "samples/orders/cancelled",
                deliveryMode = "PERSISTENT",
                correlationId = "#{#orderId}",
                condition = "#{#reason != null and !#reason.isBlank()}"
        )
        public Cancellation cancel(String orderId, String reason) {
            if (reason == null || reason.isBlank()) {
                log.info("cancel({}) has no reason -> condition is false, nothing is published", orderId);
            } else {
                log.info("cancel({}) -> publishing cancellation", orderId);
            }
            return new Cancellation(orderId, reason);
        }
    }

    @Service
    static class Runner implements CommandLineRunner {
        private static final Logger log = LoggerFactory.getLogger(Runner.class);
        private final PricedOrderPublisher publisher;

        Runner(PricedOrderPublisher publisher) {
            this.publisher = publisher;
        }

        @Override
        public void run(String... args) throws Exception {
            log.info("=== Sample 02: SpEL @SolacePublish ===");
            publisher.place("ord-1", "STANDARD", 49.95); // -> samples/orders/created/standard
            publisher.place("ord-2", "VIP", 2500.00);    // -> samples/orders/created/vip
            publisher.cancel("ord-2", "changed mind");   // condition true  -> published
            publisher.cancel("ord-1", "");               // condition false -> suppressed
            Thread.sleep(500);
            log.info("Done.");
        }
    }

    /** Same field set as the other order samples (01, 03, 04) so messages round-trip cleanly. */
    public static class Order {
        public String orderId;
        public String customer = "sample-customer";
        public String tier;
        public double amount;

        public Order() {}

        public Order(String orderId, String tier, double amount) {
            this.orderId = orderId;
            this.tier = tier;
            this.amount = amount;
        }
    }

    public static class Cancellation {
        public String orderId;
        public String reason;

        public Cancellation() {}

        public Cancellation(String orderId, String reason) {
            this.orderId = orderId;
            this.reason = reason;
        }
    }
}
