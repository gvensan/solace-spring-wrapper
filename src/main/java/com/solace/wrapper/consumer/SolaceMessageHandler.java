package com.solace.wrapper.consumer;

import com.solace.messaging.receiver.InboundMessage;

/**
 * Interface for handling received Solace messages using the new Solace Java API.
 * For manual acknowledgment, use {@link SolaceManualAckMessageHandler}.
 */
@FunctionalInterface
public interface SolaceMessageHandler<T> {

    /**
     * Handles a received message.
     *
     * @param message The deserialized message object
     * @param originalMessage The original InboundMessage from Solace
     */
    void handleMessage(T message, InboundMessage originalMessage);
}
