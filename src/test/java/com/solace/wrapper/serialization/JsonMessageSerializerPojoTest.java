package com.solace.wrapper.serialization;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.receiver.InboundMessage;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the POJO (non-String) serialization paths of {@link JsonMessageSerializer} and their
 * Jackson-failure branches, plus the String fast-paths in deserialize.
 */
class JsonMessageSerializerPojoTest {

    private final JsonMessageSerializer ser = new JsonMessageSerializer();

    private byte[] lastBuilt;

    private MessagingService capturingService() {
        return (MessagingService) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[]{MessagingService.class}, (p, m, a) -> {
                    if ("messageBuilder".equals(m.getName())) {
                        return Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class[]{OutboundMessageBuilder.class}, (bp, bm, ba) -> {
                                    if ("build".equals(bm.getName())) {
                                        lastBuilt = (byte[]) ba[0];
                                        return Proxy.newProxyInstance(getClass().getClassLoader(),
                                                new Class[]{OutboundMessage.class}, (mp, mm, ma) -> null);
                                    }
                                    return bp;
                                });
                    }
                    return null;
                });
    }

    private static InboundMessage inboundWithPayload(String payload) {
        return (InboundMessage) Proxy.newProxyInstance(JsonMessageSerializerPojoTest.class.getClassLoader(),
                new Class[]{InboundMessage.class}, (p, m, a) ->
                        "getPayloadAsString".equals(m.getName()) ? payload : null);
    }

    @Test
    void serializeToBytes_serializes_pojo_to_json() {
        byte[] bytes = ser.serializeToBytes(null, Map.of("k", 1));
        assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("{\"k\":1}");
    }

    @Test
    void serializeToBytes_throws_on_unserializable_object() {
        // A bean with no serializable properties trips Jackson's FAIL_ON_EMPTY_BEANS.
        assertThatThrownBy(() -> ser.serializeToBytes(null, new Object()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("serialize");
    }

    @Test
    void serializeWithProperties_serializes_pojo() {
        ser.serializeWithProperties(capturingService(), Map.of("a", "b"), Map.of("h", "v"));
        assertThat(new String(lastBuilt, StandardCharsets.UTF_8)).isEqualTo("{\"a\":\"b\"}");
    }

    @Test
    void serializeWithProperties_throws_on_unserializable_object() {
        assertThatThrownBy(() -> ser.serializeWithProperties(capturingService(), new Object(), Map.of()))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void deserialize_string_target_returns_raw_payload() {
        assertThat(ser.deserialize(inboundWithPayload("just text"), String.class)).isEqualTo("just text");
    }

    @Test
    void deserialize_pojo_from_json() {
        @SuppressWarnings("unchecked")
        Map<String, Object> result = ser.deserialize(inboundWithPayload("{\"x\":5}"), Map.class);
        assertThat(result).containsEntry("x", 5);
    }
}
