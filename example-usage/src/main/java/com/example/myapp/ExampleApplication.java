package com.example.myapp;

import com.example.myapp.service.AnnotationBasedOrderService;
import com.solace.wrapper.annotation.EnableSolaceAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Example Spring Boot application demonstrating the Solace Spring Wrapper.
 *
 * This is a minimal runnable example that shows:
 * 1. @EnableSolaceAnnotations to enable annotation processing
 * 2. @SolacePublish for automatic message publishing
 * 3. @SolaceConsumer for automatic message consumption
 *
 * To run:
 *   1. Start a Solace broker (docker run -d -p 55555:55555 solace/solace-pubsub-standard)
 *   2. Build the parent wrapper: cd .. && mvn clean install -DskipTests
 *   3. Run this app: mvn spring-boot:run
 */
@SpringBootApplication
@EnableSolaceAnnotations
public class ExampleApplication {

    private static final Logger logger = LoggerFactory.getLogger(ExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ExampleApplication.class, args);
    }

    @Autowired
    private AnnotationBasedOrderService orderService;

    @Bean
    public CommandLineRunner demo() {
        return args -> {
            logger.info("\n" +
                "═══════════════════════════════════════════════════════════════\n" +
                "  SOLACE SPRING WRAPPER - EXAMPLE APPLICATION\n" +
                "═══════════════════════════════════════════════════════════════\n" +
                "  This demo shows @SolacePublish and @SolaceConsumer in action.\n" +
                "  Messages are automatically published/consumed via annotations.\n" +
                "═══════════════════════════════════════════════════════════════\n");

            // Give consumers time to start
            Thread.sleep(2000);

            logger.info("Creating orders...");

            // Create a standard order - automatically published to "orders/created"
            AnnotationBasedOrderService.OrderMessage order1 = orderService.createOrder(
                "customer-123", "product-456", 150.0, "STANDARD"
            );
            logger.info("Created order: {}", order1.getOrderId());

            // Create a VIP order - automatically published to "orders/created"
            AnnotationBasedOrderService.OrderMessage order2 = orderService.createOrder(
                "customer-789", "product-101", 2500.0, "VIP"
            );
            logger.info("Created VIP order: {}", order2.getOrderId());

            // Send orders to processing - routes to vip/standard queues based on type
            logger.info("Sending orders to processing...");
            orderService.sendToProcessing(order1);
            orderService.sendToProcessing(order2);

            // Wait a bit for messages to process
            Thread.sleep(3000);

            // Update order status
            logger.info("Updating order status...");
            orderService.updateOrderStatus(order1.getOrderId(), "CREATED", "SHIPPED");

            // Bulk update
            orderService.updateMultipleOrders(
                new String[]{order1.getOrderId(), order2.getOrderId()},
                "DELIVERED"
            );

            logger.info("\n" +
                "═══════════════════════════════════════════════════════════════\n" +
                "  DEMO COMPLETE\n" +
                "═══════════════════════════════════════════════════════════════\n" +
                "  Check logs above for message publish/consume activity.\n" +
                "  The app will keep running to receive messages.\n" +
                "  Press Ctrl+C to stop.\n" +
                "═══════════════════════════════════════════════════════════════\n");
        };
    }
}
