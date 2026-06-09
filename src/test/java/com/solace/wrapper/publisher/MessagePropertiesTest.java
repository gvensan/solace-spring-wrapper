package com.solace.wrapper.publisher;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link MessageProperties} fluent value object: defaults, chained setters,
 * and user-property add/remove/replace semantics.
 */
class MessagePropertiesTest {

    @Test
    void defaults_use_sentinel_values() {
        MessageProperties p = new MessageProperties();
        assertThat(p.getTimeToLive()).isEqualTo(-1);
        assertThat(p.getPriority()).isEqualTo(-1);
        assertThat(p.getCorrelationId()).isNull();
        assertThat(p.getUserProperties()).isEmpty();
        assertThat(p.getElidingEligible()).isNull();
        assertThat(p.getClassOfService()).isNull();
    }

    @Test
    void fluent_setters_return_same_instance_and_round_trip() {
        MessageProperties p = new MessageProperties();
        MessageProperties returned = p
                .setCorrelationId("cid")
                .setReplyTo("reply/topic")
                .setTimeToLive(5000)
                .setPriority(7)
                .setApplicationMessageType("order")
                .setApplicationMessageId("amid")
                .setElidingEligible(Boolean.TRUE)
                .setClassOfService(2)
                .setDeliveryMode("PERSISTENT")
                .setMessageExpiration(123L)
                .setSequenceNumber(42L)
                .setSenderId("sender")
                .setHttpContentType("application/json")
                .setHttpContentEncoding("gzip")
                .setPersistentTimeToLive(1000L)
                .setPersistentExpiration(2000L)
                .setPersistentAckImmediately(Boolean.TRUE)
                .setPersistentDmqEligible(Boolean.FALSE);

        assertThat(returned).isSameAs(p);
        assertThat(p.getCorrelationId()).isEqualTo("cid");
        assertThat(p.getReplyTo()).isEqualTo("reply/topic");
        assertThat(p.getTimeToLive()).isEqualTo(5000);
        assertThat(p.getPriority()).isEqualTo(7);
        assertThat(p.getApplicationMessageType()).isEqualTo("order");
        assertThat(p.getApplicationMessageId()).isEqualTo("amid");
        assertThat(p.getElidingEligible()).isTrue();
        assertThat(p.getClassOfService()).isEqualTo(2);
        assertThat(p.getDeliveryMode()).isEqualTo("PERSISTENT");
        assertThat(p.getMessageExpiration()).isEqualTo(123L);
        assertThat(p.getSequenceNumber()).isEqualTo(42L);
        assertThat(p.getSenderId()).isEqualTo("sender");
        assertThat(p.getHttpContentType()).isEqualTo("application/json");
        assertThat(p.getHttpContentEncoding()).isEqualTo("gzip");
        assertThat(p.getPersistentTimeToLive()).isEqualTo(1000L);
        assertThat(p.getPersistentExpiration()).isEqualTo(2000L);
        assertThat(p.getPersistentAckImmediately()).isTrue();
        assertThat(p.getPersistentDmqEligible()).isFalse();
    }

    @Test
    void user_properties_add_remove_and_replace() {
        MessageProperties p = new MessageProperties()
                .addUserProperty("a", "1")
                .addUserProperty("b", 2);
        assertThat(p.getUserProperties()).containsEntry("a", "1").containsEntry("b", 2);

        p.removeUserProperty("a");
        assertThat(p.getUserProperties()).doesNotContainKey("a").containsEntry("b", 2);

        Map<String, Object> replacement = new HashMap<>();
        replacement.put("x", "y");
        p.setUserProperties(replacement);
        assertThat(p.getUserProperties()).containsExactly(Map.entry("x", "y"));
    }

    @Test
    void setting_null_user_properties_falls_back_to_empty_map() {
        MessageProperties p = new MessageProperties().addUserProperty("a", "1");
        p.setUserProperties(null);
        assertThat(p.getUserProperties()).isNotNull().isEmpty();
        // The map must remain usable after a null reset.
        p.addUserProperty("c", "3");
        assertThat(p.getUserProperties()).containsEntry("c", "3");
    }
}
