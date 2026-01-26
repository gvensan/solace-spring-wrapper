package com.solace.wrapper.serialization;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.receiver.InboundMessage;

/**
 * Interface for serializing and deserializing messages using the new Solace Java API.
 */
public interface MessageSerializer {

    /**
     * Serializes an object to an OutboundMessage.
     *
     * @param messagingService The messaging service
     * @param object The object to serialize
     * @return The serialized OutboundMessage
     */
    OutboundMessage serialize(MessagingService messagingService, Object object);

    /**
     * Serializes an object to an OutboundMessage with destination information.
     *
     * @param messagingService The messaging service
     * @param object The object to serialize
     * @param destination The destination (Topic or Queue)
     * @return The serialized OutboundMessage
     */
    OutboundMessage serialize(MessagingService messagingService, Object object, Object destination);

    /**
     * Deserializes an InboundMessage to an object of the specified type.
     *
     * @param message The InboundMessage to deserialize
     * @param targetType The target class type
     * @param <T> The type parameter
     * @return The deserialized object
     */
    <T> T deserialize(InboundMessage message, Class<T> targetType);

    /**
     * Deserializes an InboundMessage to a String.
     *
     * @param message The InboundMessage to deserialize
     * @return The deserialized string
     */
    String deserializeToString(InboundMessage message);

    /**
     * Serializes an object to raw bytes for custom message building.
     *
     * @param messagingService The messaging service (optional for stateless serializers)
     * @param object The object to serialize
     * @return The resulting byte array payload
     */
    byte[] serializeToBytes(MessagingService messagingService, Object object);
}
