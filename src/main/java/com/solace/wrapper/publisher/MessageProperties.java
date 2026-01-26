package com.solace.wrapper.publisher;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for holding custom message properties for the new Solace Java API.
 */
public class MessageProperties {

    private String correlationId;
    private String replyTo;
    private long timeToLive = -1;
    private int priority = -1;
    private String applicationMessageType;
    private Map<String, Object> userProperties = new HashMap<>();
    
    // New properties
    private String applicationMessageId;
    private Boolean elidingEligible;
    private Integer classOfService;
    private String deliveryMode;
    private Long messageExpiration;
    private Long sequenceNumber;
    // Additional documented properties
    private String senderId;
    private String httpContentType;
    private String httpContentEncoding;
    // Persistent-only documented properties
    private Long persistentTimeToLive;
    private Long persistentExpiration;
    private Boolean persistentAckImmediately;
    private Boolean persistentDmqEligible;

    public MessageProperties() {}

    public String getCorrelationId() {
        return correlationId;
    }

    public MessageProperties setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public MessageProperties setReplyTo(String replyTo) {
        this.replyTo = replyTo;
        return this;
    }

    public long getTimeToLive() {
        return timeToLive;
    }

    public MessageProperties setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    public int getPriority() {
        return priority;
    }

    public MessageProperties setPriority(int priority) {
        this.priority = priority;
        return this;
    }

    public String getApplicationMessageType() {
        return applicationMessageType;
    }

    public MessageProperties setApplicationMessageType(String applicationMessageType) {
        this.applicationMessageType = applicationMessageType;
        return this;
    }

    public Map<String, Object> getUserProperties() {
        return userProperties;
    }

    public MessageProperties setUserProperties(Map<String, Object> userProperties) {
        this.userProperties = userProperties != null ? userProperties : new HashMap<>();
        return this;
    }

    public MessageProperties addUserProperty(String key, Object value) {
        this.userProperties.put(key, value);
        return this;
    }

    public MessageProperties removeUserProperty(String key) {
        this.userProperties.remove(key);
        return this;
    }

    // New property getters and setters

    public String getApplicationMessageId() {
        return applicationMessageId;
    }

    public MessageProperties setApplicationMessageId(String applicationMessageId) {
        this.applicationMessageId = applicationMessageId;
        return this;
    }

    public Boolean getElidingEligible() {
        return elidingEligible;
    }

    public MessageProperties setElidingEligible(Boolean elidingEligible) {
        this.elidingEligible = elidingEligible;
        return this;
    }

    public Integer getClassOfService() {
        return classOfService;
    }

    public MessageProperties setClassOfService(Integer classOfService) {
        this.classOfService = classOfService;
        return this;
    }

    public String getDeliveryMode() {
        return deliveryMode;
    }

    public MessageProperties setDeliveryMode(String deliveryMode) {
        this.deliveryMode = deliveryMode;
        return this;
    }

    public Long getMessageExpiration() {
        return messageExpiration;
    }

    public MessageProperties setMessageExpiration(Long messageExpiration) {
        this.messageExpiration = messageExpiration;
        return this;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public MessageProperties setSequenceNumber(Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
        return this;
    }

    public String getSenderId() {
        return senderId;
    }

    public MessageProperties setSenderId(String senderId) {
        this.senderId = senderId;
        return this;
    }

    public String getHttpContentType() {
        return httpContentType;
    }

    public MessageProperties setHttpContentType(String httpContentType) {
        this.httpContentType = httpContentType;
        return this;
    }

    public String getHttpContentEncoding() {
        return httpContentEncoding;
    }

    public MessageProperties setHttpContentEncoding(String httpContentEncoding) {
        this.httpContentEncoding = httpContentEncoding;
        return this;
    }

    public Long getPersistentTimeToLive() {
        return persistentTimeToLive;
    }

    public MessageProperties setPersistentTimeToLive(Long persistentTimeToLive) {
        this.persistentTimeToLive = persistentTimeToLive;
        return this;
    }

    public Long getPersistentExpiration() {
        return persistentExpiration;
    }

    public MessageProperties setPersistentExpiration(Long persistentExpiration) {
        this.persistentExpiration = persistentExpiration;
        return this;
    }

    public Boolean getPersistentAckImmediately() {
        return persistentAckImmediately;
    }

    public MessageProperties setPersistentAckImmediately(Boolean persistentAckImmediately) {
        this.persistentAckImmediately = persistentAckImmediately;
        return this;
    }

    public Boolean getPersistentDmqEligible() {
        return persistentDmqEligible;
    }

    public MessageProperties setPersistentDmqEligible(Boolean persistentDmqEligible) {
        this.persistentDmqEligible = persistentDmqEligible;
        return this;
    }
}
