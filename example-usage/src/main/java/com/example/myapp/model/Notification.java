package com.example.myapp.model;

/**
 * Customer-facing notification message used by the programmatic gateway demo.
 */
public class Notification {
    public String notificationId;
    public String customerId;
    public String channel;
    public String message;
    public long timestamp;

    public Notification() {}

    @Override
    public String toString() {
        return String.format("Notification{id='%s', customer='%s', channel='%s'}",
                notificationId, customerId, channel);
    }
}
