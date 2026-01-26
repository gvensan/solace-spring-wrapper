package com.solace.wrapper.consumer;

import com.solace.messaging.receiver.InboundMessage;

/**
 * Handler that allows explicit ack/nack via {@link SolaceAckContext}.
 */
@FunctionalInterface
public interface SolaceManualAckMessageHandler<T> {

    /**
     * Handles a received message with manual acknowledgment control.
     *
     * @param message The deserialized message object
     * @param originalMessage The original InboundMessage from Solace
     * @param ackContext Manual acknowledgment context
     */
    void handleMessage(T message, InboundMessage originalMessage, SolaceAckContext ackContext);
}
