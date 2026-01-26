package com.solace.wrapper.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.receiver.InboundMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;


/**
 * JSON implementation of MessageSerializer using Jackson and the new Solace Java API.
 */
@Component
public class JsonMessageSerializer implements MessageSerializer {

    private static final Logger logger = LoggerFactory.getLogger(JsonMessageSerializer.class);
    private final ObjectMapper objectMapper;
    public static final String CONTENT_TYPE = "content-type";

    public JsonMessageSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    public JsonMessageSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public OutboundMessage serialize(MessagingService messagingService, Object object) {
        return serialize(messagingService, object, null);
    }

    @Override
    public OutboundMessage serialize(MessagingService messagingService, Object object, Object destination) {
        try {
            byte[] payload = serializeToBytes(messagingService, object);
            OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();

            // Stick to official API only; do not set custom headers

            OutboundMessage message = messageBuilder.build(payload);

            logger.debug("Serialized object to JSON message: {}", new String(payload, StandardCharsets.UTF_8));
            return message;

        } catch (Exception e) {
            logger.error("Failed to create outbound message", e);
            throw new RuntimeException("Failed to create outbound message", e);
        }
    }

    @Override
    public <T> T deserialize(InboundMessage message, Class<T> targetType) {
        try {
            // Get message content as string
            String jsonString = message.getPayloadAsString();
            if (jsonString == null || jsonString.trim().isEmpty()) {
                logger.warn("Received empty message payload");
                return null;
            }
            
            logger.debug("Deserializing JSON message: {}", jsonString);
            
            if (targetType == String.class) {
                return targetType.cast(jsonString);
            }
            
            return objectMapper.readValue(jsonString, targetType);
            
        } catch (Exception e) {
            logger.error("Failed to deserialize message to type: {}", targetType.getName(), e);
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }

    @Override
    public String deserializeToString(InboundMessage message) {
        try {
            return message.getPayloadAsString();
        } catch (Exception e) {
            logger.error("Failed to deserialize message to string", e);
            throw new RuntimeException("Failed to deserialize message to string", e);
        }
    }

    @Override
    public byte[] serializeToBytes(MessagingService messagingService, Object object) {
        try {
            String jsonString;
            if (object instanceof String) {
                jsonString = (String) object;
            } else {
                jsonString = objectMapper.writeValueAsString(object);
            }
            return jsonString.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize object to JSON", e);
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }

    /**
     * Gets the underlying ObjectMapper for custom configuration.
     */
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    /**
     * Helper method to serialize an object to JSON string.
     * Used by other components that need access to JSON serialization.
     */
    public String serializeToJson(Object object) throws JsonProcessingException {
        if (object instanceof String) {
            return (String) object;
        }
        return objectMapper.writeValueAsString(object);
    }

    /**
     * Creates an OutboundMessage with custom properties.
     * 
     * @param messagingService The messaging service
     * @param object The object to serialize
     * @param properties Custom properties to add to the message
     * @return The configured OutboundMessage
     */
    public OutboundMessage serializeWithProperties(MessagingService messagingService, Object object, 
                                                  java.util.Map<String, String> properties) {
        try {
            String jsonString;
            if (object instanceof String) {
                jsonString = (String) object;
            } else {
                jsonString = objectMapper.writeValueAsString(object);
            }
            
            OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();
            
            // Do not set arbitrary properties; rely on payload only
            
            OutboundMessage message = messageBuilder.build(jsonString.getBytes(StandardCharsets.UTF_8));
            
            logger.debug("Serialized object to JSON message with properties: {}", jsonString);
            return message;
            
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize object to JSON", e);
            throw new RuntimeException("Failed to serialize object to JSON", e);
        } catch (Exception e) {
            logger.error("Failed to create outbound message with properties", e);
            throw new RuntimeException("Failed to create outbound message with properties", e);
        }
    }

    /**
     * Creates an OutboundMessage with correlation ID.
     * 
     * @param messagingService The messaging service
     * @param object The object to serialize
     * @param correlationId The correlation ID to set
     * @return The configured OutboundMessage
     */
    /*
    public OutboundMessage serializeWithCorrelationId(MessagingService messagingService, Object object, 
                                                     String correlationId) {
        try {
            String jsonString;
            if (object instanceof String) {
                jsonString = (String) object;
            } else {
                jsonString = objectMapper.writeValueAsString(object);
            }
            
            OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();
            
            // Set content type for JSON
            messageBuilder.withProperty("content-type", "application/json");
            
            // Set correlation ID
            if (correlationId != null && !correlationId.trim().isEmpty()) {
                messageBuilder.withCorrelationId(correlationId);
            }
            
            OutboundMessage message = messageBuilder.build(jsonString);
            
            logger.debug("Serialized object to JSON message with correlation ID {}: {}", correlationId, jsonString);
            return message;
            
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize object to JSON", e);
            throw new RuntimeException("Failed to serialize object to JSON", e);
        } catch (Exception e) {
            logger.error("Failed to create outbound message with correlation ID", e);
            throw new RuntimeException("Failed to create outbound message with correlation ID", e);
        }
    }
    */

    /**
     * Creates an OutboundMessage from byte array (for binary data).
     * 
     * @param messagingService The messaging service
     * @param data The byte array data
     * @param contentType The content type
     * @return The configured OutboundMessage
     */
    public OutboundMessage serializeBytes(MessagingService messagingService, byte[] data, String contentType) {
        try {
            OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();

            // Do not set content-type header to stay within official API property set
            
            OutboundMessage message = messageBuilder.build(data);
            
            logger.debug("Created message from byte array, length: {}, content-type: {}", data.length, contentType);
            return message;
            
        } catch (Exception e) {
            logger.error("Failed to create outbound message from bytes", e);
            throw new RuntimeException("Failed to create outbound message from bytes", e);
        }
    }

    /**
     * Helper method to extract correlation ID from an inbound message.
     * 
     * @param message The inbound message
     * @return The correlation ID, or null if not present
     */
    public String getCorrelationId(InboundMessage message) {
        try {
            return message.getCorrelationId();
        } catch (Exception e) {
            logger.debug("No correlation ID found in message", e);
            return null;
        }
    }

    /**
     * Helper method to extract a property from an inbound message.
     * 
     * @param message The inbound message
     * @param propertyName The property name to extract
     * @return The property value, or null if not present
     */
    public String getMessageProperty(InboundMessage message, String propertyName) {
        try {
            return message.getProperty(propertyName);
        } catch (Exception e) {
            logger.debug("Property '{}' not found in message", propertyName, e);
            return null;
        }
    }

    /**
     * Helper method to get message size.
     * 
     * @param message The inbound message
     * @return The message size in bytes, or -1 if unable to determine
     */
    public long getMessageSize(InboundMessage message) {
        try {
            byte[] payload = message.getPayloadAsBytes();
            return payload != null ? payload.length : 0;
        } catch (Exception e) {
            logger.debug("Unable to determine message size", e);
            return -1;
        }
    }

    /**
     * Helper method to check if message is JSON.
     * 
     * @param message The inbound message
     * @return true if content-type indicates JSON
     */
    public boolean isJsonMessage(InboundMessage message) {
        String contentType = getMessageProperty(message, "content-type");
        return contentType != null && contentType.toLowerCase().contains("json");
    }
}
