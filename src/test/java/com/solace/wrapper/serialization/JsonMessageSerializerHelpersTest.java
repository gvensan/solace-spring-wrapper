package com.solace.wrapper.serialization;

import com.solace.messaging.receiver.InboundMessage;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the helper/utility methods on {@link JsonMessageSerializer} (correlation id, property
 * extraction, size, JSON detection, string serialization) including their lenient error paths.
 */
class JsonMessageSerializerHelpersTest {

    private final JsonMessageSerializer ser = new JsonMessageSerializer();

    /** Builds an InboundMessage proxy from a map of method name -> value; values that are
     * RuntimeExceptions are thrown to simulate failures. */
    private static InboundMessage inbound(Map<String, Object> behavior) {
        return (InboundMessage) Proxy.newProxyInstance(
                JsonMessageSerializerHelpersTest.class.getClassLoader(),
                new Class[]{InboundMessage.class}, (p, m, a) -> {
                    Object v = behavior.get(m.getName());
                    if (v instanceof RuntimeException re) {
                        throw re;
                    }
                    if (v != null) {
                        return v;
                    }
                    if (m.getReturnType().equals(boolean.class)) return false;
                    if (m.getReturnType().equals(int.class)) return 0;
                    if (m.getReturnType().equals(long.class)) return 0L;
                    return null;
                });
    }

    @Test
    void getCorrelationId_returns_value_and_null_on_error() {
        assertThat(ser.getCorrelationId(inbound(Map.of("getCorrelationId", "cid-1")))).isEqualTo("cid-1");
        assertThat(ser.getCorrelationId(inbound(Map.of("getCorrelationId", new RuntimeException("boom")))))
                .isNull();
    }

    @Test
    void getMessageProperty_returns_value_and_null_on_error() {
        assertThat(ser.getMessageProperty(inbound(Map.of("getProperty", "v")), "k")).isEqualTo("v");
        assertThat(ser.getMessageProperty(inbound(Map.of("getProperty", new RuntimeException("x"))), "k"))
                .isNull();
    }

    @Test
    void getMessageSize_returns_length_zero_or_minus_one() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        assertThat(ser.getMessageSize(inbound(Map.of("getPayloadAsBytes", payload)))).isEqualTo(5L);
        // Null payload -> 0
        assertThat(ser.getMessageSize(inbound(Map.of()))).isZero();
        // Exception -> -1
        assertThat(ser.getMessageSize(inbound(Map.of("getPayloadAsBytes", new RuntimeException("x")))))
                .isEqualTo(-1L);
    }

    @Test
    void isJsonMessage_detects_json_content_type() {
        assertThat(ser.isJsonMessage(inbound(Map.of("getProperty", "application/json")))).isTrue();
        assertThat(ser.isJsonMessage(inbound(Map.of("getProperty", "text/plain")))).isFalse();
        // Missing content-type -> false
        assertThat(ser.isJsonMessage(inbound(Map.of()))).isFalse();
    }

    @Test
    void serializeToJson_passes_through_strings_and_writes_objects() throws Exception {
        assertThat(ser.serializeToJson("raw")).isEqualTo("raw");
        assertThat(ser.serializeToJson(Map.of("k", 1))).isEqualTo("{\"k\":1}");
    }

    @Test
    void deserialize_empty_payload_returns_null() {
        assertThat(ser.deserialize(inbound(Map.of("getPayloadAsString", "  ")), String.class)).isNull();
        assertThat(ser.deserialize(inbound(Map.of()), String.class)).isNull();
    }

    @Test
    void deserialize_invalid_json_throws_runtime() {
        assertThatThrownBy(() ->
                ser.deserialize(inbound(Map.of("getPayloadAsString", "not-json")), java.util.List.class))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void deserializeToString_returns_payload_and_throws_on_error() {
        assertThat(ser.deserializeToString(inbound(Map.of("getPayloadAsString", "body")))).isEqualTo("body");
        assertThatThrownBy(() ->
                ser.deserializeToString(inbound(Map.of("getPayloadAsString", new RuntimeException("x")))))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void getObjectMapper_is_exposed() {
        assertThat(ser.getObjectMapper()).isNotNull();
        assertThat(new JsonMessageSerializer(ser.getObjectMapper()).getObjectMapper())
                .isSameAs(ser.getObjectMapper());
    }
}
