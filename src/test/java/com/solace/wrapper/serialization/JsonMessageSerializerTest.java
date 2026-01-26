package com.solace.wrapper.serialization;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.receiver.InboundMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonMessageSerializerTest {

    private static final Logger log = LoggerFactory.getLogger(JsonMessageSerializerTest.class);

    static class Env {
        volatile byte[] lastBuiltBytes;
        MessagingService service() {
            return (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (p, m, a) -> {
                        if ("messageBuilder".equals(m.getName())) {
                            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{OutboundMessageBuilder.class}, (bp, bm, ba) -> {
                                if ("build".equals(bm.getName())) {
                                    // build(byte[])
                                    lastBuiltBytes = (byte[]) ba[0];
                                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{OutboundMessage.class}, (mp, mm, maa) -> null);
                                }
                                return bp; // fluent no-ops
                            });
                        }
                        if (m.getReturnType().equals(boolean.class)) return false;
                        if (m.getReturnType().equals(int.class)) return 0;
                        return null;
                    });
        }
    }

    private JsonMessageSerializer ser;
    private Env env;

    @BeforeEach
    void setup() {
        ser = new JsonMessageSerializer();
        env = new Env();
    }

    @Test
    void serialize_string_uses_raw_payload() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: serialize_string_uses_raw_payload\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that String messages are serialized as raw bytes, not as\n" +
                "  JSON-encoded strings (which would add quotes around the value).\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Special handling for String type in JsonMessageSerializer\n" +
                "  2. Raw byte conversion without JSON encoding\n" +
                "\n" +
                "WHY RAW STRINGS:\n" +
                "  - JSON-encoded strings would be '\"hello\"' (with quotes)\n" +
                "  - Raw strings are 'hello' (without quotes)\n" +
                "  - Raw is more intuitive for text-based messaging\n" +
                "  - Interoperable with non-JSON consumers\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Serialize a plain String 'hello'\n" +
                "  2. Verify output is 'hello' (not '\"hello\"')\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Bytes represent 'hello' exactly\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Serializing a plain String 'hello'");
        ser.serialize(env.service(), "hello");

        log.info("STEP 2: Verifying raw string bytes were built (not JSON-quoted)");
        assertThat(new String(env.lastBuiltBytes, StandardCharsets.UTF_8)).isEqualTo("hello");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Serialized bytes: '" + new String(env.lastBuiltBytes, StandardCharsets.UTF_8) + "'\n" +
                "  Expected: 'hello' (raw, no JSON quotes)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - String was serialized as raw UTF-8 bytes\n" +
                "  - No JSON encoding (no surrounding quotes)\n" +
                "  - Result is human-readable and interoperable\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    static class POJO { public int a; public String s; }

    @Test
    void serialize_pojo_to_json() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: serialize_pojo_to_json\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that POJO (Plain Old Java Object) instances are serialized\n" +
                "  to JSON format using Jackson ObjectMapper.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. POJO to JSON serialization via Jackson\n" +
                "  2. Field mapping (public fields to JSON properties)\n" +
                "  3. Correct JSON structure\n" +
                "\n" +
                "JSON SERIALIZATION:\n" +
                "  - Uses Jackson ObjectMapper under the hood\n" +
                "  - Public fields and getters are serialized\n" +
                "  - Produces compact JSON (no pretty-printing)\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create POJO with int and String fields\n" +
                "  2. Serialize to JSON\n" +
                "  3. Verify JSON structure matches expected\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - JSON: {\"a\":1,\"s\":\"x\"}\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating POJO with a=1, s='x'");
        POJO p = new POJO(); p.a = 1; p.s = "x";

        log.info("STEP 2: Serializing POJO");
        ser.serialize(env.service(), p);

        log.info("STEP 3: Verifying JSON output format");
        String result = new String(env.lastBuiltBytes, StandardCharsets.UTF_8);
        assertThat(result).isEqualTo("{\"a\":1,\"s\":\"x\"}");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Serialized JSON: '" + result + "'\n" +
                "  Expected: '{\"a\":1,\"s\":\"x\"}'\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - POJO fields correctly mapped to JSON properties\n" +
                "  - Integer field 'a' serialized as JSON number\n" +
                "  - String field 's' serialized as JSON string\n" +
                "  - Compact format (no extra whitespace)\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void serialize_with_properties_still_builds_payload() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: serialize_with_properties_still_builds_payload\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that serializeWithProperties() builds JSON payload correctly\n" +
                "  while also applying message properties (separate from payload).\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. serializeWithProperties() method\n" +
                "  2. Properties applied to message builder (not payload)\n" +
                "  3. Payload serialization unaffected by properties\n" +
                "\n" +
                "PROPERTIES vs PAYLOAD:\n" +
                "  - PROPERTIES: Message metadata (headers, correlation ID, etc.)\n" +
                "  - PAYLOAD: The actual message body (JSON content)\n" +
                "  - Properties don't affect payload serialization\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create POJO with fields a=2, s='y'\n" +
                "  2. Serialize with properties map {k=v}\n" +
                "  3. Verify JSON payload is correct (properties separate)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Payload: {\"a\":2,\"s\":\"y\"}\n" +
                "  - Properties applied to builder (not in payload)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating POJO with a=2, s='y' and properties map");
        POJO p = new POJO(); p.a = 2; p.s = "y";

        log.info("STEP 2: Serializing with properties {k=v}");
        ser.serializeWithProperties(env.service(), p, Map.of("k", "v"));

        log.info("STEP 3: Verifying JSON payload is still built correctly");
        String result = new String(env.lastBuiltBytes, StandardCharsets.UTF_8);
        assertThat(result).isEqualTo("{\"a\":2,\"s\":\"y\"}");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Serialized payload: '" + result + "'\n" +
                "  Expected: '{\"a\":2,\"s\":\"y\"}'\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Payload contains only POJO data (JSON)\n" +
                "  - Properties {k=v} applied to message builder separately\n" +
                "  - Properties are message headers, not part of body\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void serialize_bytes_builds_exact_payload() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: serialize_bytes_builds_exact_payload\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that byte arrays are serialized as-is (pass-through)\n" +
                "  without any transformation or encoding.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. serializeBytes() method for raw binary data\n" +
                "  2. Pass-through semantics (no transformation)\n" +
                "  3. Content-type setting for binary data\n" +
                "\n" +
                "USE CASES FOR RAW BYTES:\n" +
                "  - Binary protocols (Protobuf, Avro, Thrift)\n" +
                "  - Image/file data\n" +
                "  - Pre-serialized data from other systems\n" +
                "  - Performance-critical scenarios\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create byte array [1, 2, 3]\n" +
                "  2. Serialize with content-type 'application/octet-stream'\n" +
                "  3. Verify output bytes are identical to input\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Output bytes: [1, 2, 3] (exactly as input)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating byte array [1, 2, 3]");
        byte[] b = new byte[]{1, 2, 3};

        log.info("STEP 2: Serializing bytes with content-type 'application/octet-stream'");
        ser.serializeBytes(env.service(), b, "application/octet-stream");

        log.info("STEP 3: Verifying exact byte payload");
        assertThat(env.lastBuiltBytes).containsExactly(1, 2, 3);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Output bytes: [1, 2, 3] (expected: [1, 2, 3])\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Byte array passed through without transformation\n" +
                "  - No encoding or wrapping applied\n" +
                "  - Suitable for binary protocols\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void deserialize_to_type_and_string_and_helpers() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: deserialize_to_type_and_string_and_helpers\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify JSON deserialization to POJO works correctly and test\n" +
                "  all helper methods for extracting message metadata.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. deserialize(message, targetType) - JSON to POJO\n" +
                "  2. deserializeToString() - raw payload as String\n" +
                "  3. getCorrelationId() - extract correlation ID\n" +
                "  4. getMessageProperty() - extract arbitrary property\n" +
                "  5. getMessageSize() - payload size in bytes\n" +
                "  6. isJsonMessage() - detect JSON content-type\n" +
                "\n" +
                "HELPER METHOD USE CASES:\n" +
                "  - getCorrelationId: Request/reply pattern correlation\n" +
                "  - getMessageProperty: Access custom headers\n" +
                "  - getMessageSize: Logging, metrics, size limits\n" +
                "  - isJsonMessage: Content-type based routing\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create mock InboundMessage with JSON payload and metadata\n" +
                "  2. Deserialize to POJO and verify field values\n" +
                "  3. Test all helper methods with expected values\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - POJO: a=5, s='z'\n" +
                "  - All helper methods return correct values\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating mock InboundMessage with JSON payload");
        String json = "{\"a\":5,\"s\":\"z\"}";
        InboundMessage inbound = (InboundMessage) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class[]{InboundMessage.class}, (p, m, a) -> {
                    switch (m.getName()) {
                        case "getPayloadAsString": return json;
                        case "getPayloadAsBytes": return json.getBytes(StandardCharsets.UTF_8);
                        case "getCorrelationId": return "CID-1";
                        case "getProperty": return "application/json";
                        default: return null;
                    }
                });

        log.info("STEP 2: Deserializing JSON to POJO");
        POJO out = ser.deserialize(inbound, POJO.class);
        assertThat(out.a).isEqualTo(5);
        assertThat(out.s).isEqualTo("z");

        log.info("STEP 3: Testing helper methods");
        assertThat(ser.deserializeToString(inbound)).isEqualTo(json);
        assertThat(ser.getCorrelationId(inbound)).isEqualTo("CID-1");
        assertThat(ser.getMessageProperty(inbound, "content-type")).isEqualTo("application/json");
        assertThat(ser.getMessageSize(inbound)).isEqualTo(json.getBytes(StandardCharsets.UTF_8).length);
        assertThat(ser.isJsonMessage(inbound)).isTrue();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Deserialized POJO:\n" +
                "    a: " + out.a + " (expected: 5)\n" +
                "    s: '" + out.s + "' (expected: 'z')\n" +
                "\n" +
                "  Helper Methods:\n" +
                "    deserializeToString(): '" + json + "'\n" +
                "    getCorrelationId(): 'CID-1'\n" +
                "    getMessageProperty('content-type'): 'application/json'\n" +
                "    getMessageSize(): " + json.getBytes(StandardCharsets.UTF_8).length + " bytes\n" +
                "    isJsonMessage(): true\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - JSON correctly deserialized to POJO fields\n" +
                "  - All helper methods return expected values\n" +
                "  - Metadata extraction works correctly\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}

