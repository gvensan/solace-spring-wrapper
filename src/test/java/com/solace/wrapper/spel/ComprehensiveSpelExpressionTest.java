package com.solace.wrapper.spel;

import com.solace.wrapper.annotation.SolacePublish;
import com.solace.wrapper.annotation.processor.SolacePublishAspect;
import com.solace.wrapper.publisher.MessageProperties;
import com.solace.wrapper.publisher.SolacePublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

public class ComprehensiveSpelExpressionTest {

    private static final Logger logger = LoggerFactory.getLogger(ComprehensiveSpelExpressionTest.class);

    private RecordingPublisher publisher;
    private TestService proxy;

    @BeforeEach
    void setup() {
        publisher = new RecordingPublisher();
        SolacePublishAspect aspect = new SolacePublishAspect();
        aspect.solacePublisher = publisher;
        aspect.setSpelResolver(new com.solace.wrapper.annotation.processor.SpelExpressionResolver());
        AspectJProxyFactory f = new AspectJProxyFactory(new TestService());
        f.addAspect(aspect);
        proxy = f.getProxy();
    }

    @Test
    void destination_spel_and_basic_properties_map() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: destination_spel_and_basic_properties_map\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify SpEL expression resolution for destination and message properties.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. destination = \"#{#dest}\" - dynamic topic from method parameter\n" +
                "  2. correlationId = \"#{#cid}\" - from method parameter\n" +
                "  3. applicationMessageType = \"#{#typ}\" - from method parameter\n" +
                "  4. timeToLive = 2500 (literal) - fixed TTL in milliseconds\n" +
                "  5. priority = 7 (literal) - fixed message priority\n" +
                "  6. userProperties = {\"k=v\"} - static user property\n" +
                "\n" +
                "ANNOTATION:\n" +
                "  @SolacePublish(destination=\"#{#dest}\", correlationId=\"#{#cid}\",\n" +
                "    applicationMessageType=\"#{#typ}\", timeToLive=2500, priority=7,\n" +
                "    userProperties={\"k=v\"})\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Destination resolved to passed parameter value\n" +
                "  - CorrelationId, type resolved from parameters\n" +
                "  - TTL and priority set from literal values\n" +
                "  - User property 'k=v' present\n" +
                "───────────────────────────────────────────────────────────────\n");

        String dest = "spel/test/" + System.nanoTime();
        String out = proxy.make(dest, "CID-1", "TYPE-A", 2500, 7);
        MessageProperties mp = publisher.lastProps;

        assertWithLog(
                "destination_spel_and_basic_properties_map",
                "Destination and basic properties resolve via SpEL and literals",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo(dest);
                    assertThat(publisher.lastBody).isEqualTo(out);
                    assertThat(mp.getCorrelationId()).isEqualTo("CID-1");
                    assertThat(mp.getApplicationMessageType()).isEqualTo("TYPE-A");
                    assertThat(mp.getTimeToLive()).isEqualTo(2500);
                    assertThat(mp.getPriority()).isEqualTo(7);
                    assertThat(mp.getUserProperties()).containsEntry("k", "v");
                }
        );
    }

    @Test
    void advanced_properties_and_persistent_async_route() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: advanced_properties_and_persistent_async_route\n" +
                "PURPOSE: Verify advanced SpEL properties (eliding, COS, expiration, sequence) and async persistent routing\n" +
                "───────────────────────────────────────────────────────────────");

        long exp = Instant.now().plusSeconds(3600).toEpochMilli();
        proxy.advanced("adv/topic", true, 2, exp, 42L);

        MessageProperties mp = publisher.lastProps;
        assertWithLog(
                "advanced_properties_and_persistent_async_route",
                "Advanced properties resolve; async persistent route is selected",
                () -> {
                    assertThat(publisher.lastPersistent).isTrue();
                    assertThat(publisher.lastAsync).isTrue();
                    assertThat(mp.getElidingEligible()).isTrue();
                    assertThat(mp.getClassOfService()).isEqualTo(2);
                    assertThat(mp.getMessageExpiration()).isEqualTo(exp);
                    assertThat(mp.getSequenceNumber()).isEqualTo(42L);
                    assertThat(mp.getDeliveryMode()).isEqualTo("PERSISTENT");
                }
        );
    }

    @Test
    void condition_true_publishes_false_skips() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: condition_true_publishes_false_skips\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the @SolacePublish 'condition' attribute controls whether\n" +
                "  a message is published based on SpEL expression evaluation.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  - condition = \"#{#ok}\" - evaluates boolean method parameter\n" +
                "  - If condition is FALSE, publish is SKIPPED entirely\n" +
                "  - If condition is TRUE, message is published normally\n" +
                "\n" +
                "ANNOTATION:\n" +
                "  @SolacePublish(destination=\"cond/topic\", condition=\"#{#ok}\",\n" +
                "    correlationId=\"#{#cid}\")\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Call with ok=false -> condition evaluates to false -> NO publish\n" +
                "  2. Call with ok=true -> condition evaluates to true -> message published\n" +
                "\n" +
                "USE CASES:\n" +
                "  - Only publish order events when order.status == 'COMPLETED'\n" +
                "  - Skip notification if user has notifications disabled\n" +
                "  - Conditional audit logging based on severity level\n" +
                "───────────────────────────────────────────────────────────────\n");

        // false -> skip
        logger.info("STEP 1: Calling with ok=false - should SKIP publishing");
        proxy.conditional(false, "C-0");
        assertWithLog(
                "condition_true_publishes_false_skips_false_path",
                "Condition false should skip publish",
                () -> assertThat(publisher.lastTopic).isNull()
        );

        // true -> publish
        logger.info("STEP 2: Calling with ok=true - should PUBLISH message");
        proxy.conditional(true, "C-1");
        assertWithLog(
                "condition_true_publishes_false_skips_true_path",
                "Condition true should publish with correlation id and direct sync path",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo("cond/topic");
                    assertThat(publisher.lastProps.getCorrelationId()).isEqualTo("C-1");
                    assertThat(publisher.lastAsync).isFalse();
                    assertThat(publisher.lastPersistent).isFalse();
                }
        );
    }

    @Test
    void replyTo_and_result_and_direct_hash_expr() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: replyTo_and_result_and_direct_hash_expr\n" +
                "PURPOSE: Verify replyTo and #result expressions resolve correctly\n" +
                "───────────────────────────────────────────────────────────────");

        String d = "r/topic";
        String body = proxy.replyToAndResult(d, "CID-R");
        MessageProperties mp = publisher.lastProps;

        assertWithLog(
                "replyTo_and_result_and_direct_hash_expr",
                "Reply-to and #result expressions resolve for correlationId/appMessageType",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo(d);
                    assertThat(mp.getReplyTo()).isEqualTo(d + "/reply");
                    assertThat(mp.getCorrelationId()).isEqualTo(body);
                    assertThat(mp.getApplicationMessageType()).isEqualTo(body);
                    assertThat(publisher.lastAsync).isFalse();
                    assertThat(publisher.lastPersistent).isFalse();
                }
        );
    }

    @Test
    void empty_destination_skips_publish() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: empty_destination_skips_publish\n" +
                "PURPOSE: Verify empty destination string skips publishing\n" +
                "───────────────────────────────────────────────────────────────");

        proxy.dynamicDest("");
        assertWithLog(
                "empty_destination_skips_publish",
                "Empty destination should skip publishing",
                () -> assertThat(publisher.lastTopic).isNull()
        );
    }

    @Test
    void user_properties_values_support_spel_expressions() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: user_properties_values_support_spel_expressions\n" +
                "PURPOSE: Verify user properties support SpEL value interpolation\n" +
                "───────────────────────────────────────────────────────────────");

        String out = proxy.userProps("up/topic", "CID-UP", "PAY");
        MessageProperties mp = publisher.lastProps;
        assertWithLog(
                "user_properties_values_support_spel_expressions",
                "User properties support SpEL value interpolation",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo("up/topic");
                    assertThat(mp.getUserProperties()).containsEntry("a", "CID-UP-x");
                    assertThat(mp.getUserProperties()).containsEntry("b", out + "-y");
                }
        );
    }

    @Test
    void delivery_mode_expression_switches_between_direct_and_persistent() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: delivery_mode_expression_switches_between_direct_and_persistent\n" +
                "PURPOSE: Verify deliveryMode SpEL expression dynamically selects DIRECT or PERSISTENT\n" +
                "───────────────────────────────────────────────────────────────");

        proxy.modeSwitch("m/topic", false);
        assertWithLog(
                "delivery_mode_expression_switches_between_direct_and_persistent_direct",
                "Delivery mode expression should select direct",
                () -> assertThat(publisher.lastPersistent).isFalse()
        );

        proxy.modeSwitch("m/topic", true);
        assertWithLog(
                "delivery_mode_expression_switches_between_direct_and_persistent_persistent",
                "Delivery mode expression should select persistent",
                () -> assertThat(publisher.lastPersistent).isTrue()
        );
    }

    @Test
    void null_result_skips_publishing() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: null_result_skips_publishing\n" +
                "PURPOSE: Verify null method result skips publishing\n" +
                "───────────────────────────────────────────────────────────────");

        proxy.nullResult("n/topic");
        assertWithLog(
                "null_result_skips_publishing",
                "Null result should skip publishing",
                () -> assertThat(publisher.lastTopic).isNull()
        );
    }

    @Test
    void literal_destination_no_properties_uses_simple_sync_publish() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: literal_destination_no_properties_uses_simple_sync_publish\n" +
                "PURPOSE: Verify literal destination with no properties uses sync direct publish\n" +
                "───────────────────────────────────────────────────────────────");

        String rv = proxy.simpleLiteral("hello");
        assertWithLog(
                "literal_destination_no_properties_uses_simple_sync_publish",
                "Literal destination with no properties uses sync direct publish and empty properties",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo("lit/topic");
                    assertThat(publisher.lastBody).isEqualTo(rv);
                    MessageProperties mp = publisher.lastProps;
                    assertThat(mp).isNotNull();
                    assertThat(mp.getCorrelationId()).isNull();
                    assertThat(mp.getReplyTo()).isNull();
                    assertThat(mp.getApplicationMessageType()).isNull();
                    assertThat(mp.getApplicationMessageId()).isNull();
                    assertThat(mp.getElidingEligible()).isNull();
                    assertThat(mp.getClassOfService()).isNull();
                    assertThat(mp.getDeliveryMode()).isNull();
                    assertThat(mp.getMessageExpiration()).isNull();
                    assertThat(mp.getSequenceNumber()).isNull();
                    assertThat(mp.getUserProperties()).isEmpty();
                    assertThat(publisher.lastAsync).isFalse();
                    assertThat(publisher.lastPersistent).isFalse();
                }
        );
    }

    @Test
    void async_direct_path_is_used_when_async_true_and_direct_mode() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: async_direct_path_is_used_when_async_true_and_direct_mode\n" +
                "PURPOSE: Verify async=true with direct mode uses async publish path\n" +
                "───────────────────────────────────────────────────────────────");

        proxy.asyncDirect("ad/topic", "P1");
        assertWithLog(
                "async_direct_path_is_used_when_async_true_and_direct_mode",
                "Async direct publish path should be selected",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo("ad/topic");
                    assertThat(publisher.lastAsync).isTrue();
                    assertThat(publisher.lastPersistent).isFalse();
                }
        );
    }

    @Test
    void result_field_and_plain_variable_destination_and_args_access() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: result_field_and_plain_variable_destination_and_args_access\n" +
                "PURPOSE: Verify #result field access and args[] array access in SpEL\n" +
                "───────────────────────────────────────────────────────────────");

        Order o = new Order();
        o.id = "OID-9"; o.customerId = "C-7";
        Order out = proxy.resultFieldAndArgs("pv/topic", "CID-PLAIN", o);
        MessageProperties mp = publisher.lastProps;
        assertWithLog(
                "result_field_and_plain_variable_destination_and_args_access",
                "Plain variable and args access should resolve for destination and properties",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo("pv/topic");
                    assertThat(mp.getCorrelationId()).isEqualTo("OID-9");
                    assertThat(mp.getUserProperties()).containsEntry("cid", "CID-PLAIN");
                    assertThat(mp.getApplicationMessageType()).isEqualTo("CID-PLAIN");
                    assertThat(out).isNotNull();
                }
        );
    }

    @Test
    void condition_between_operator_support() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: condition_between_operator_support\n" +
                "PURPOSE: Verify SpEL 'between' operator works in condition expressions\n" +
                "───────────────────────────────────────────────────────────────");

        // priority 0 -> skip
        proxy.conditionBetween("cb/topic", 0, "x");
        assertWithLog(
                "condition_between_operator_support_skip",
                "Between operator should skip out-of-range values",
                () -> assertThat(publisher.lastTopic).isNull()
        );
        // priority 2 -> publish
        proxy.conditionBetween("cb/topic", 2, "x");
        assertWithLog(
                "condition_between_operator_support_publish",
                "Between operator should allow in-range values",
                () -> assertThat(publisher.lastTopic).isEqualTo("cb/topic")
        );
    }

    @Test
    void application_message_id_is_set_via_spel() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: application_message_id_is_set_via_spel\n" +
                "PURPOSE: Verify applicationMessageId resolves via SpEL expression\n" +
                "───────────────────────────────────────────────────────────────");

        proxy.appMsgId("ami/topic", "AMID-1", "PAY");
        assertWithLog(
                "application_message_id_is_set_via_spel",
                "Application message ID should resolve via SpEL",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo("ami/topic");
                    assertThat(publisher.lastProps.getApplicationMessageId()).isEqualTo("AMID-1");
                }
        );
    }

    @Test
    void client_name_is_resolved_and_forwarded() {
        logger.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: client_name_is_resolved_and_forwarded\n" +
                "PURPOSE: Verify clientName SpEL expression is resolved and passed to publisher context\n" +
                "───────────────────────────────────────────────────────────────");

        proxy.clientName("client-1", "BODY");
        assertWithLog(
                "client_name_is_resolved_and_forwarded",
                "Client name should be resolved via SpEL and passed to publisher context",
                () -> {
                    assertThat(publisher.lastTopic).isEqualTo("client/topic");
                    assertThat(publisher.lastClientName).isEqualTo("client-1");
                    assertThat(publisher.lastPublisherKey).isEqualTo("client:client-1");
                }
        );
    }

    // Test service with rich @SolacePublish usage
    static class TestService {
        @SolacePublish(destination = "#{#dest}",
                correlationId = "#{#cid}",
                applicationMessageType = "#{#typ}",
                timeToLive = 2500,
                priority = 7,
                userProperties = {"k=v"})
        public String make(String dest, String cid, String typ, int ttl, int pr) { return "B-"+dest; }

        @SolacePublish(destination = "#{#d}", async = true, deliveryMode = "PERSISTENT",
                elidingEligible = "#{true}", classOfService = "#{#cos}",
                messageExpiration = "#{#exp}", sequenceNumber = "#{#seq}")
        public String advanced(String d, boolean elide, int cos, long exp, long seq) { return "X"; }

        @SolacePublish(destination = "cond/topic", condition = "#{#ok}", correlationId = "#{#cid}")
        public String conditional(boolean ok, String cid) { return "COND"; }

        @SolacePublish(destination = "#{#d}", replyTo = "#{#d + '/reply'}", correlationId = "#{#result}", applicationMessageType = "#result")
        public String replyToAndResult(String d, String cid) { return "B-" + d; }

        @SolacePublish(destination = "#{#dest}")
        public String dynamicDest(String dest) { return "BDY"; }

        @SolacePublish(destination = "#{#d}", userProperties = {"a=#{#cid + '-x'}", "b=#{#result + '-y'}"})
        public String userProps(String d, String cid, String payload) { return payload; }

        @SolacePublish(destination = "#{#d}", deliveryMode = "#{#persist ? 'PERSISTENT' : 'DIRECT'}")
        public String modeSwitch(String d, boolean persist) { return "MS"; }

        @SolacePublish(destination = "#{#d}")
        public String nullResult(String d) { return null; }

        @SolacePublish(destination = "lit/topic")
        public String simpleLiteral(String payload) { return "L-" + payload; }

        @SolacePublish(destination = "#{#d}", async = true)
        public String asyncDirect(String d, String payload) { return payload; }

        // destination as plain variable name without #; correlationId from result.id; user property from args[1]
        @SolacePublish(destination = "d", correlationId = "#{result.id}", applicationMessageType = "cid", userProperties = {"cid=args[1]"})
        public Order resultFieldAndArgs(String d, String cid, Order o) { return o; }

        @SolacePublish(destination = "#{#d}", condition = "#{ #priority between 1 and 3 }")
        public String conditionBetween(String d, int priority, String payload) { return payload; }

        @SolacePublish(destination = "#{#d}", applicationMessageId = "#{#id}")
        public String appMsgId(String d, String id, String payload) { return payload; }

        @SolacePublish(destination = "client/topic", clientName = "#{#client}")
        public String clientName(String client, String payload) { return payload; }
    }

    static class Order {
        public String id;
        public String customerId;
    }

    // Recording test-double for SolacePublisher
    static class RecordingPublisher extends SolacePublisher {
        String lastTopic; Object lastBody; MessageProperties lastProps;
        boolean lastAsync; boolean lastPersistent;
        String lastClientName; String lastPublisherKey;
        RecordingPublisher() { super(new NoopCM(defaultProps()), null); }
        @Override public void publishWithContext(String topicName, Object message, MessageProperties properties,
                                                 boolean persistent, String clientName, String publisherKey) {
            record(false, persistent, topicName, message, properties, clientName, publisherKey);
        }
        @Override public CompletableFuture<Void> publishWithContextAsync(String topicName, Object message, MessageProperties properties,
                                                                         boolean persistent, String clientName, String publisherKey) {
            record(true, persistent, topicName, message, properties, clientName, publisherKey);
            return CompletableFuture.completedFuture(null);
        }
        @Override public void publishToTopic(String topicName, Object message) { record(false,false, topicName, message, null); }
        @Override public void publishToTopicWithProperties(String topicName, Object message, MessageProperties props) { record(false,false, topicName, message, props); }
        @Override public CompletableFuture<Void> publishToTopicAsync(String topicName, Object message) { record(true,false, topicName, message, null); return CompletableFuture.completedFuture(null);}    
        @Override public CompletableFuture<Void> publishToTopicWithPropertiesAsync(String topicName, Object message, MessageProperties props) { record(true,false, topicName, message, props); return CompletableFuture.completedFuture(null);} 
        @Override public void publishPersistentToTopic(String topicName, Object message) { record(false,true, topicName, message, null); }
        @Override public void publishPersistentToTopicWithProperties(String topicName, Object message, MessageProperties props) { record(false,true, topicName, message, props); }
        @Override public CompletableFuture<Void> publishPersistentToTopicAsync(String topicName, Object message) { record(true,true, topicName, message, null); return CompletableFuture.completedFuture(null);} 
        @Override public CompletableFuture<Void> publishPersistentToTopicWithPropertiesAsync(String topicName, Object message, MessageProperties props) { record(true,true, topicName, message, props); return CompletableFuture.completedFuture(null);} 
        private void record(boolean async, boolean persistent, String topic, Object body, MessageProperties props) {
            this.lastAsync = async; this.lastPersistent = persistent;
            this.lastTopic = topic; this.lastBody = body; this.lastProps = props;
            this.lastClientName = null; this.lastPublisherKey = null;
        }
        private void record(boolean async, boolean persistent, String topic, Object body, MessageProperties props,
                            String clientName, String publisherKey) {
            this.lastAsync = async; this.lastPersistent = persistent;
            this.lastTopic = topic; this.lastBody = body; this.lastProps = props;
            this.lastClientName = clientName; this.lastPublisherKey = publisherKey;
        }
        private static com.solace.wrapper.config.SolaceProperties defaultProps() {
            com.solace.wrapper.config.SolaceProperties p = new com.solace.wrapper.config.SolaceProperties();
            p.setHost("tcp://noop");
            p.setMsgVpn("default");
            p.setClientUsername("default");
            p.setClientPassword("");
            return p;
        }
        // No-op connection manager that returns a dummy MessagingService; avoids real broker calls
        static class NoopCM extends com.solace.wrapper.connection.SolaceConnectionManager {
            NoopCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
            @Override public com.solace.messaging.MessagingService createMessagingService() { return DummyMessagingService.INSTANCE; }
        }
        static class DummyMessagingService {
            static final com.solace.messaging.MessagingService INSTANCE = (com.solace.messaging.MessagingService)
                    java.lang.reflect.Proxy.newProxyInstance(
                            ComprehensiveSpelExpressionTest.class.getClassLoader(),
                            new Class<?>[]{com.solace.messaging.MessagingService.class},
                            (proxy, method, args) -> {
                                String name = method.getName();
                                if ("connect".equals(name) || "disconnect".equals(name)) return proxy;
                                if ("isConnected".equals(name)) return Boolean.TRUE;
                                if (method.getReturnType().equals(boolean.class)) return false;
                                if (method.getReturnType().equals(int.class)) return 0;
                                return null;
            });
        }
    }

    private void assertWithLog(String testName, String expectation, Runnable assertions) {
        String actual = publishSummary();
        try {
            assertions.run();
            logger.info("{} PASS - {}. {}", testName, expectation, actual);
        } catch (AssertionError e) {
            logger.error("{} FAIL - {}. {}. Reason: {}", testName, expectation, actual, e.getMessage());
            throw e;
        }
    }

    private String publishSummary() {
        return "topic=" + publisher.lastTopic
                + ", body=" + publisher.lastBody
                + ", async=" + publisher.lastAsync
                + ", persistent=" + publisher.lastPersistent
                + ", clientName=" + publisher.lastClientName
                + ", publisherKey=" + publisher.lastPublisherKey
                + ", props=" + summarizeProps(publisher.lastProps);
    }

    private String summarizeProps(MessageProperties mp) {
        if (mp == null) {
            return "null";
        }
        return "correlationId=" + mp.getCorrelationId()
                + ", replyTo=" + mp.getReplyTo()
                + ", ttl=" + mp.getTimeToLive()
                + ", priority=" + mp.getPriority()
                + ", appType=" + mp.getApplicationMessageType()
                + ", appId=" + mp.getApplicationMessageId()
                + ", elidingEligible=" + mp.getElidingEligible()
                + ", classOfService=" + mp.getClassOfService()
                + ", deliveryMode=" + mp.getDeliveryMode()
                + ", messageExpiration=" + mp.getMessageExpiration()
                + ", sequenceNumber=" + mp.getSequenceNumber()
                + ", userProps=" + mp.getUserProperties();
    }
}
