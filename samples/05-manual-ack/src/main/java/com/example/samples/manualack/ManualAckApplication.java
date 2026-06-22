package com.example.samples.manualack;

import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.annotation.SolaceConsumer;
import com.solace.wrapper.consumer.SolaceAckContext;
import com.solace.wrapper.publisher.SolacePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

/**
 * Sample 05 — <b>Manual acknowledgement</b> with {@link SolaceConsumer} + {@link SolaceAckContext}.
 *
 * <p>With {@code ackMode = MANUAL} you decide the fate of each message: call {@code ack.ack()} to
 * settle it, or {@code ack.fail()} to negatively acknowledge (so the broker can redeliver / route to
 * a DMQ). The method also receives the raw {@link InboundMessage} for header access.</p>
 *
 * <p>Local backoff retry ({@code localMaxAttempts} + the {@code localBackoff*} knobs) retries the
 * handler in-process before the message is failed — useful for transient downstream errors.</p>
 *
 * <p>The app feeds one valid and one invalid payment so you can see both an ack and a fail. Ctrl+C to stop.</p>
 */
@SpringBootApplication
@EnableSolaceAnnotations
public class ManualAckApplication {

    private static final String QUEUE = "samples.payments.queue";
    private static final String TOPIC = "samples/payments/incoming";

    public static void main(String[] args) {
        SpringApplication.run(ManualAckApplication.class, args);
    }

    @Service
    static class PaymentConsumer {
        private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

        @SolaceConsumer(
                queue = QUEUE,
                topics = {TOPIC},
                ackMode = SolaceConsumer.AckMode.MANUAL,
                autoCreateQueue = true,
                consumerIdPrefix = "payments",
                localMaxAttempts = 2,
                localBackoffInitialMs = 250,
                localBackoffMultiplier = 2.0,
                localBackoffMaxMs = 1000
        )
        public void onPayment(Payment payment, InboundMessage raw, SolaceAckContext ack) {
            log.info("💳 Payment {} amount={} (correlationId={})",
                    payment.paymentId, payment.amount, raw.getCorrelationId());
            if (payment.amount <= 0) {
                log.warn("   invalid amount -> ack.fail()");
                ack.fail();   // negative ack: broker may redeliver or route to DMQ
            } else {
                log.info("   ok -> ack.ack()");
                ack.ack();    // settle the message
            }
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
            log.info("=== Sample 05: MANUAL ack @SolaceConsumer ===");
            Thread.sleep(1500);
            publisher.publishPersistentToTopicAsync(TOPIC, new Payment("pay-1", 42.00));  // -> ack
            publisher.publishPersistentToTopicAsync(TOPIC, new Payment("pay-2", -5.00));  // -> fail
            log.info("Published 1 valid + 1 invalid payment. Ctrl+C to stop.");
            new CountDownLatch(1).await();
        }
    }

    public static class Payment {
        public String paymentId;
        public double amount;

        public Payment() {}

        public Payment(String paymentId, double amount) {
            this.paymentId = paymentId;
            this.amount = amount;
        }
    }
}
