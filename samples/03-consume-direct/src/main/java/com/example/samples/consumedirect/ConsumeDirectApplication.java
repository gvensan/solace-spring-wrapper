package com.example.samples.consumedirect;

import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.publisher.SolacePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

/**
 * Sample 03 — Consuming with {@link SolaceConsumer} in DIRECT mode.
 *
 * <p>DIRECT mode subscribes straight to a topic (no queue), with best-effort delivery — ideal for
 * live events where missing the occasional message is acceptable. The annotated method is invoked
 * with the deserialized payload (and, optionally, the raw {@link InboundMessage}).</p>
 *
 * <p><b>Topic alignment:</b> this consumer subscribes to the shared <i>order-created</i> branch
 * ({@code samples/ordersd/created} and {@code samples/ordersd/created/>}), so it receives orders from
 * the publisher samples too — run sample 01 or 02 in another terminal and watch them arrive here.
 * Note the wildcard {@code >} is used only in the <b>subscription</b>; the self-test below publishes
 * to a <b>concrete</b> topic.</p>
 *
 * <p>To stay self-contained it also publishes a few test orders; then it stays up so the subscription
 * keeps running — press Ctrl+C to stop.</p>
 */
@SpringBootApplication
@EnableSolaceAnnotations
public class ConsumeDirectApplication {

    /** Concrete topic this sample publishes its own test messages to. */
    private static final String PUBLISH_TOPIC = "samples/ordersd/created";

    public static void main(String[] args) {
        SpringApplication.run(ConsumeDirectApplication.class, args);
    }

    /** The star of the sample: a DIRECT topic consumer over the shared order-created branch. */
    @Service
    static class OrderConsumer {
        private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

        @SolaceConsumer(
                // bare "created" (sample 01) + any sub-level like "created/vip" (sample 02);
                // deliberately NOT "samples/ordersd/>", so order-cancelled events don't land here.
                topics = {"samples/ordersd/created", "samples/ordersd/created/>"},
                mode = SolaceConsumer.MessagingMode.DIRECT,
                consumerIdPrefix = "orders"
        )
        public void onOrder(Order order, InboundMessage raw) {
            log.info("📥 Received order {} (tier={}, amount={})", order.orderId, order.tier, order.amount);
        }
    }

    /** Publishes test orders, then keeps the app alive so the consumer keeps subscribing. */
    @Service
    static class Runner implements CommandLineRunner {
        private static final Logger log = LoggerFactory.getLogger(Runner.class);
        private final SolacePublisher publisher;

        Runner(SolacePublisher publisher) {
            this.publisher = publisher;
        }

        @Override
        public void run(String... args) throws Exception {
            log.info("=== Sample 03: DIRECT @SolaceConsumer ===");
            Thread.sleep(1500); // give the subscription time to be established
            for (int i = 1; i <= 3; i++) {
                publisher.publishToTopic(PUBLISH_TOPIC, new Order("ord-" + i, "customer-" + i, "STANDARD", 10.0 * i));
            }
            log.info("Published 3 orders to {} — watch for receipts above. Ctrl+C to stop.", PUBLISH_TOPIC);
            new CountDownLatch(1).await(); // keep the JVM (and the subscription) alive
        }
    }

    /** Same field set as the other order samples (01, 02, 04) so messages round-trip cleanly. */
    public static class Order {
        public String orderId;
        public String customer;
        public String tier = "STANDARD";
        public double amount;

        public Order() {}

        public Order(String orderId, String customer, String tier, double amount) {
            this.orderId = orderId;
            this.customer = customer;
            this.tier = tier;
            this.amount = amount;
        }
    }
}
