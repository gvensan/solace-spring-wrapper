package com.example.samples.consumequeue;

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
 * Sample 04 — Consuming from a <b>persistent queue</b> with {@link SolaceConsumer}.
 *
 * <p>Persistent (guaranteed) messaging: the queue spools messages so nothing is lost if the consumer
 * is briefly down. Key attributes shown:</p>
 * <ul>
 *   <li>{@code queue} + {@code topics} — the queue is subscribed to topics, so anything published to
 *       a matching topic <b>persistently</b> is spooled to the queue. (Direct publishes are not
 *       spooled — they only reach direct subscribers like sample 03.)</li>
 *   <li>{@code autoCreateQueue} — provisions the queue on brokers that allow it (otherwise pre-create it).</li>
 *   <li>{@code maxRetries} / {@code retryDelay} — broker redelivery on failure (the wrapper acks on
 *       normal return, nacks on exception).</li>
 * </ul>
 *
 * <p><b>Topic alignment:</b> the queue subscribes to the shared <i>order-created</i> branch, so the
 * <b>persistent</b> orders published by samples 01 and 02 are spooled here too — run one of them in
 * another terminal and watch this queue process them. The self-test below publishes persistently to a
 * concrete topic. Ctrl+C to stop.</p>
 */
@SpringBootApplication
@EnableSolaceAnnotations
public class ConsumeQueueApplication {

    private static final String QUEUE = "samples.orders.queue";
    private static final String PUBLISH_TOPIC = "samples/ordersq/created";

    public static void main(String[] args) {
        SpringApplication.run(ConsumeQueueApplication.class, args);
    }

    @Service
    static class OrderWorker {
        private static final Logger log = LoggerFactory.getLogger(OrderWorker.class);

        @SolaceConsumer(
                queue = QUEUE,
                // bare "created" (sample 01) + sub-levels like "created/vip" (sample 02).
                topics = {"samples/ordersq/created", "samples/ordersq/created/>"},
                autoCreateQueue = true,
                maxRetries = 3,
                retryDelay = 1000,
                consumerIdPrefix = "order-worker"
        )
        public void process(Order order) {
            log.info("🛠  Processing order {} for {} (tier={}, ${})",
                    order.orderId, order.customer, order.tier, order.amount);
            // Normal return => the wrapper acknowledges the message. Throwing would trigger redelivery.
        }
    }

    @Service
    static class Runner implements CommandLineRunner {
        private static final Logger log = LoggerFactory.getLogger(Runner.class);
        private final SolacePublisher publisher;

        Runner(SolacePublisher publisher) {
            this.publisher = publisher;
        }

        @Override
        public void run(String... args) throws Exception {
            log.info("=== Sample 04: persistent queue @SolaceConsumer ===");
            Thread.sleep(1500); // let the queue bind/subscribe first
            for (int i = 1; i <= 3; i++) {
                // Persistent publish so the message is spooled to the subscribed queue.
                publisher.publishPersistentToTopicAsync(PUBLISH_TOPIC, new Order("ord-" + i, "customer-" + i, "STANDARD", 10.0 * i));
            }
            log.info("Published 3 persistent orders to {} (queue {}). Ctrl+C to stop.", PUBLISH_TOPIC, QUEUE);
            new CountDownLatch(1).await();
        }
    }

    /** Same field set as the other order samples (01, 02, 03) so messages round-trip cleanly. */
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
