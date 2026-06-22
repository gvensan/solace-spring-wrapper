package com.example.myapp.programmatic;

import com.example.myapp.model.Notification;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.publisher.MessageProperties;
import com.solace.wrapper.publisher.SolacePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.UUID;

/**
 * The <strong>programmatic alternative</strong> to the annotation pipeline, kept deliberately small
 * and namespaced under {@code programmatic/*} topics so it never interferes with the annotation flow.
 *
 * <p>Use this style when you need runtime control the annotations don't give you: creating/removing
 * consumers dynamically, building per-message {@link MessageProperties}, or driving async publishes
 * with {@link java.util.concurrent.CompletableFuture} callbacks.</p>
 *
 * <p>Demonstrates: {@link SolaceConsumerManager#createConsumer} (with manual lifecycle cleanup in
 * {@link #shutdown()}), {@link SolacePublisher#publishToTopic}, persistent publish with explicit
 * {@link MessageProperties}, and async publish.</p>
 */
@Component
public class ProgrammaticOrderGateway {

    private static final Logger logger = LoggerFactory.getLogger(ProgrammaticOrderGateway.class);

    private final SolacePublisher publisher;
    private final SolaceConsumerManager consumerManager;
    private String notificationConsumerId;

    public ProgrammaticOrderGateway(SolacePublisher publisher, SolaceConsumerManager consumerManager) {
        this.publisher = publisher;
        this.consumerManager = consumerManager;
    }

    @PostConstruct
    public void start() {
        notificationConsumerId = consumerManager.createConsumer(
                "programmatic-notifications",
                Notification.class,
                this::onNotification);
        logger.info("Programmatic notification consumer registered: {}", notificationConsumerId);
    }

    @PreDestroy
    public void shutdown() {
        if (notificationConsumerId != null) {
            consumerManager.removeConsumer(notificationConsumerId);
            logger.info("Programmatic notification consumer removed");
        }
    }

    /** Fire a notification with custom headers via explicit {@link MessageProperties}, persistently. */
    public void notifyCustomer(String customerId, String channel, String text) {
        Notification notification = new Notification();
        notification.notificationId = UUID.randomUUID().toString();
        notification.customerId = customerId;
        notification.channel = channel;
        notification.message = text;
        notification.timestamp = System.currentTimeMillis();

        MessageProperties properties = new MessageProperties()
                .setCorrelationId(notification.notificationId)
                .setTimeToLive(120_000)
                .setPriority(6)
                .setDeliveryMode("PERSISTENT")
                .addUserProperty("channel", channel)
                .addUserProperty("customerId", customerId);

        publisher.publishPersistentToTopicWithProperties(
                "programmatic/notifications/" + channel.toLowerCase(), notification, properties);
        logger.info("Programmatically published notification {} for {}", notification.notificationId, customerId);
    }

    /** Async variant — non-blocking publish with completion callbacks. */
    public void notifyCustomerAsync(String customerId, String channel, String text) {
        Notification notification = new Notification();
        notification.notificationId = UUID.randomUUID().toString();
        notification.customerId = customerId;
        notification.channel = channel;
        notification.message = text;
        notification.timestamp = System.currentTimeMillis();

        publisher.publishPersistentToTopicAsync("programmatic/notifications/" + channel.toLowerCase(), notification)
                .thenRun(() -> logger.debug("Async notification sent: {}", notification.notificationId))
                .exceptionally(err -> {
                    logger.error("Async notification failed: {}", notification.notificationId, err);
                    return null;
                });
    }

    private void onNotification(Notification notification, InboundMessage raw) {
        logger.info("Programmatic consumer received notification {} on channel {} for customer {}",
                notification.notificationId, notification.channel, notification.customerId);
    }
}
