package com.example.samples.publishbasic;

import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.annotation.SolacePublish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Sample 01 — Basic publishing with {@link SolacePublish}.
 *
 * <p>The single concept: annotate a method with {@code @SolacePublish}. Whatever the method returns
 * is serialized (JSON) and published to the configured destination. The method body has no idea
 * Solace exists — publishing is entirely declarative.</p>
 *
 * <p>Run: {@code mvn spring-boot:run} (with a broker reachable at the configured host).</p>
 */
@SpringBootApplication
@EnableSolaceAnnotations
public class PublishBasicApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublishBasicApplication.class, args);
    }

    /** Publishes whatever it returns to {@code samples/orders/created}. */
    @Service
    static class OrderPublisher {
        private static final Logger log = LoggerFactory.getLogger(OrderPublisher.class);

        @SolacePublish(
                destination = "samples/orders/created",
                deliveryMode = "PERSISTENT",            // persistent => reaches both the direct (03) and queue (04) consumer samples
                correlationId = "#{result.orderId}",    // SpEL over the return value
                userProperties = {"source=publish-basic-sample"}
        )
        public Order createOrder(String customer, double amount) {
            Order order = new Order(UUID.randomUUID().toString(), customer, amount);
            log.info("Returning order {} -> it will be published to samples/orders/created", order.orderId);
            return order;
        }
    }

    /** Drives the sample once, then exits. */
    @Service
    static class Runner implements CommandLineRunner {
        private static final Logger log = LoggerFactory.getLogger(Runner.class);
        private final OrderPublisher publisher;

        Runner(OrderPublisher publisher) {
            this.publisher = publisher;
        }

        @Override
        public void run(String... args) throws Exception {
            log.info("=== Sample 01: basic @SolacePublish ===");
            publisher.createOrder("alice", 49.95);
            publisher.createOrder("bob", 1200.00);
            Thread.sleep(500); // let the publishes flush before the context closes
            log.info("Published 2 orders. Done.");
        }
    }

    /**
     * Minimal Jackson-friendly payload (no-arg ctor + public fields). Shares the same field set
     * ({@code orderId, customer, tier, amount}) as the other order samples (02, 03, 04) so messages
     * round-trip cleanly between them.
     */
    public static class Order {
        public String orderId;
        public String customer;
        public String tier = "STANDARD";
        public double amount;

        public Order() {}

        public Order(String orderId, String customer, double amount) {
            this.orderId = orderId;
            this.customer = customer;
            this.amount = amount;
        }
    }
}
