package com.example.myapp;

import com.example.myapp.model.QuoteResponse;
import com.example.myapp.observability.OrderMetrics;
import com.example.myapp.order.OrderService;
import com.example.myapp.pricing.QuoteClient;
import com.example.myapp.programmatic.ProgrammaticOrderGateway;
import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.metrics.SolaceMetrics;
import io.micrometer.core.instrument.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.CompletableFuture;

/**
 * Example Spring Boot app for the Solace Spring Wrapper, modelled as a single end-to-end
 * <strong>order-fulfillment pipeline</strong>:
 *
 * <pre>
 *   OrderService ──orders/created──▶ InventoryService ──inventory/reserve──▶ (reserve, MANUAL ack)
 *        │                                   │
 *        │                                   └──billing/charge──▶ BillingService ──billing/charged──▶ NotificationService
 *        └──orders/status/*──────────────────────────────────────────────────────────────────────▶ (deferred consumer)
 *
 *   QuoteClient ──request/reply──▶ PricingService            (native @SolaceReplier + SolaceRequestor)
 *   ProgrammaticOrderGateway                                  (programmatic SolacePublisher / ConsumerManager)
 * </pre>
 *
 * <p>The driver below exercises every supported annotation feature: see the per-service classes for
 * the attribute-by-attribute walkthrough, and {@code README.md} for the coverage matrix.</p>
 *
 * <p>To run: start a broker
 * ({@code docker run -d -p 55555:55555 -p 8080:8080 solace/solace-pubsub-standard}),
 * {@code mvn -f .. clean install -DskipTests}, then {@code mvn spring-boot:run}.</p>
 */
@SpringBootApplication
@EnableSolaceAnnotations
public class ExampleApplication {

    private static final Logger logger = LoggerFactory.getLogger(ExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
    }

    @Bean
    public CommandLineRunner demo(OrderService orderService,
                                  QuoteClient quoteClient,
                                  ProgrammaticOrderGateway gateway,
                                  SolaceConsumerManager consumerManager,
                                  OrderMetrics orderMetrics,
                                  SolaceMetrics solaceMetrics) {
        return args -> {
            banner("SOLACE SPRING WRAPPER — ORDER FULFILLMENT DEMO");

            // Let annotation consumers connect and subscribe.
            Thread.sleep(2000);

            // The status-notification consumer is autoStart=false; activate it on demand.
            logger.info("Starting deferred consumers (autoStart=false) ...");
            consumerManager.startAllConsumers();

            // 1) Drive the pipeline with a standard and a VIP order.
            banner("1) Annotation pipeline: create orders");
            var standard = orderService.createOrder("cust-001", "WIDGET-A", 2, 49.98, "STANDARD");
            var vip = orderService.createOrder("cust-002", "WIDGET-B", 1, 2500.00, "VIP");

            Thread.sleep(2000); // allow inventory -> billing -> notification to flow

            // 2) Emit a couple of status transitions (eliding-eligible "ticks").
            banner("2) Status transitions");
            orderService.changeStatus(standard.orderId, "CREATED", "PACKED");
            orderService.changeStatus(standard.orderId, "PACKED", "SHIPPED");

            // 3) Native request-reply for live pricing (sync + async).
            banner("3) Request-reply pricing");
            QuoteResponse syncQuote = quoteClient.getQuote("WIDGET-C", 5, "STANDARD");
            logger.info("Sync quote total: {} {}", syncQuote.totalPrice, syncQuote.currency);
            CompletableFuture<QuoteResponse> asyncQuote = quoteClient.getQuoteAsync("WIDGET-C", 5, "VIP");
            asyncQuote.join();

            // 4) Conditional publish: one cancel with a reason (publishes), one without (suppressed).
            banner("4) Conditional cancellation");
            orderService.cancelOrder(vip.orderId, "customer changed their mind"); // publishes
            orderService.cancelOrder(standard.orderId, "");                       // suppressed by condition

            // 5) Programmatic API alternative.
            banner("5) Programmatic gateway");
            gateway.notifyCustomer("cust-001", "EMAIL", "Your order has shipped");
            gateway.notifyCustomerAsync("cust-002", "SMS", "VIP order update");

            Thread.sleep(2000);

            // 6) Observability snapshot.
            banner("6) Metrics snapshot (also at /actuator/prometheus)");
            logger.info("SolaceMetrics enabled: {}", solaceMetrics.isEnabled());
            logger.info("Orders in flight (custom gauge): {}", orderMetrics.ordersInFlight());
            dumpMeters(solaceMetrics);

            banner("DEMO COMPLETE — app stays up to keep consuming. Ctrl+C to stop.");
        };
    }

    private static void dumpMeters(SolaceMetrics metrics) {
        if (!metrics.isEnabled()) {
            return;
        }
        for (Meter meter : metrics.getRegistry().getMeters()) {
            String name = meter.getId().getName();
            if (name.startsWith("solace.") || name.startsWith("example.")) {
                meter.measure().forEach(m ->
                        logger.info("  {} [{}] {} = {}", name, meter.getId().getTags(),
                                m.getStatistic(), m.getValue()));
            }
        }
    }

    private static void banner(String text) {
        logger.info("\n═══════════════════════════════════════════════════════════════\n  {}\n" +
                "═══════════════════════════════════════════════════════════════", text);
    }
}
