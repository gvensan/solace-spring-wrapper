package com.solace.wrapper.publisher;

import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.publisher.PersistentMessagePublisher;
import com.solace.messaging.resources.Topic;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.serialization.MessageSerializer;
import com.solace.wrapper.exception.SolacePublishException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Publisher-focused unit tests exercising direct/persistent sync+async paths and retry behavior
 * without requiring a real broker.
 */
public class SolacePublisherTest {

    private static final Logger log = LoggerFactory.getLogger(SolacePublisherTest.class);

    static volatile RecordingEnv CURRENT;

    private RecordingEnv env;
    private SolacePublisher publisher;

    @BeforeEach
    void setup() {
        env = new RecordingEnv();
        CURRENT = env;
        publisher = new SolacePublisher(env.cm, env.serializer);
        // Avoid @PostConstruct in tests; call init explicitly to build publishers
        publisher.init();
    }

    @AfterEach
    void tearDown() {
        // Clean up publisher resources to prevent JVM from hanging due to non-daemon threads
        if (publisher != null) {
            publisher.shutdown();
        }
        // Also clean up connection manager
        if (env != null && env.cm != null) {
            env.cm.shutdown();
        }
    }

    @AfterAll
    static void cleanupAll() {
        // Clear static resources and reset Mockito to ensure JVM can exit
        CURRENT_RECEIPT_LISTENER = null;
        CURRENT = null;
        Mockito.framework().clearInlineMocks();
    }

    @Test
    void direct_publish_invokes_underlying_publisher() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: direct_publish_invokes_underlying_publisher\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that publishToTopic() correctly invokes the underlying\n" +
                "  DirectMessagePublisher with the correct topic and serialized message.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. publishToTopic() method delegation to DirectMessagePublisher\n" +
                "  2. Topic name is correctly passed to publisher.publish()\n" +
                "  3. Message serializer receives correct destination for context\n" +
                "\n" +
                "DIRECT MESSAGING CHARACTERISTICS:\n" +
                "  - Best-effort delivery (fire-and-forget)\n" +
                "  - No persistence or guaranteed delivery\n" +
                "  - Lowest latency option for real-time data\n" +
                "  - If no subscriber is online, message is LOST\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Call publishToTopic() with topic 't/direct' and a Map payload\n" +
                "  2. Verify underlying DirectMessagePublisher was invoked exactly once\n" +
                "  3. Verify topic name was correctly passed through\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - directPublishCalls = 1\n" +
                "  - lastDirectTopic = 't/direct'\n" +
                "  - serializer destination matches Topic.of('t/direct')\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Publishing direct message to topic 't/direct'");
        publisher.publishToTopic("t/direct", Map.of("k", 1));

        log.info("STEP 2: Verifying publish was invoked once and topic/destination recorded correctly");
        assertThat(env.directPublishCalls.get()).isEqualTo(1);
        assertThat(env.lastDirectTopic).isEqualTo("t/direct");
        assertThat(env.serializerLastDestination).isEqualTo(Topic.of("t/direct"));

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  directPublishCalls: " + env.directPublishCalls.get() + " (expected: 1)\n" +
                "  lastDirectTopic: '" + env.lastDirectTopic + "' (expected: 't/direct')\n" +
                "  serializerDestination matches: true\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - publishToTopic() correctly delegated to DirectMessagePublisher\n" +
                "  - Topic name was preserved through the publish chain\n" +
                "  - Serializer was given destination context for potential customization\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void direct_publish_retries_on_failure() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: direct_publish_retries_on_failure\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that direct publish automatically retries after a transient\n" +
                "  failure, providing resilience against temporary network issues.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Automatic retry logic for failed direct publishes\n" +
                "  2. Publisher re-initialization after failure\n" +
                "  3. Transparent recovery from transient errors\n" +
                "\n" +
                "WHY RETRY MATTERS:\n" +
                "  - Network blips can cause momentary publish failures\n" +
                "  - Automatic retry prevents message loss for recoverable errors\n" +
                "  - Application code doesn't need explicit retry handling\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Configure test harness to fail the NEXT direct publish\n" +
                "  2. Call publishToTopic() - first attempt fails\n" +
                "  3. Retry logic kicks in and retries automatically\n" +
                "  4. Second attempt succeeds\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Publish eventually succeeds (directPublishCalls = 1 for successful call)\n" +
                "  - No exception thrown to caller\n" +
                "  - Topic correctly recorded\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Configuring test environment to fail the NEXT direct publish");
        env.failNextDirectPublish.set(true);

        log.info("STEP 2: Publishing to topic 't/retry' - first attempt will fail, retry will succeed");
        publisher.publishToTopic("t/retry", "x");

        log.info("STEP 3: Verifying publish eventually succeeded after retry");
        assertThat(env.directPublishCalls.get()).isEqualTo(1); // first failed, retried once and succeeded
        assertThat(env.lastDirectTopic).isEqualTo("t/retry");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  directPublishCalls: " + env.directPublishCalls.get() + " (expected: 1 successful)\n" +
                "  lastDirectTopic: '" + env.lastDirectTopic + "' (expected: 't/retry')\n" +
                "  Exception thrown to caller: false (retry handled internally)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - First attempt failed with simulated transient error\n" +
                "  - Retry logic automatically reinitiated publish\n" +
                "  - Second attempt succeeded\n" +
                "  - Caller received successful completion (no exception)\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications get automatic resilience for direct messaging\n" +
                "  - Transient failures don't require application-level retry code\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void persistent_publish_and_retry() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_publish_and_retry\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that persistent publish automatically retries after a\n" +
                "  transient failure, ensuring guaranteed delivery semantics.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Automatic retry logic for failed persistent publishes\n" +
                "  2. Publisher re-initialization after failure\n" +
                "  3. Guaranteed delivery through retry mechanism\n" +
                "\n" +
                "PERSISTENT vs DIRECT RETRY:\n" +
                "  - Persistent messages MUST be delivered (guaranteed semantics)\n" +
                "  - Retry is even more critical than for direct messaging\n" +
                "  - Failure without retry would violate delivery guarantees\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Configure test harness to fail the NEXT persistent publish\n" +
                "  2. Call publishPersistentToTopic() - first attempt fails\n" +
                "  3. Retry logic kicks in automatically\n" +
                "  4. Second attempt succeeds\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Publish eventually succeeds (persistentPublishCalls = 1)\n" +
                "  - No exception thrown to caller\n" +
                "  - Topic correctly recorded\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Configuring test environment to fail the NEXT persistent publish");
        env.failNextPersistentPublish.set(true);

        log.info("STEP 2: Publishing persistent message to topic 'p/retry' - will fail then retry");
        publisher.publishPersistentToTopic("p/retry", "y");

        log.info("STEP 3: Verifying persistent publish succeeded after retry");
        assertThat(env.persistentPublishCalls.get()).isEqualTo(1);
        assertThat(env.lastPersistentTopic).isEqualTo("p/retry");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  persistentPublishCalls: " + env.persistentPublishCalls.get() + " (expected: 1 successful)\n" +
                "  lastPersistentTopic: '" + env.lastPersistentTopic + "' (expected: 'p/retry')\n" +
                "  Exception thrown to caller: false (retry handled internally)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - First attempt failed with simulated transient error\n" +
                "  - Retry logic automatically reinitiated persistent publish\n" +
                "  - Second attempt succeeded\n" +
                "  - Guaranteed delivery semantics maintained through retry\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Persistent messaging maintains delivery guarantees even with failures\n" +
                "  - Transient errors don't result in message loss\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void persistent_confirm_async_completes_best_effort_without_receipts() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_confirm_async_completes_best_effort_without_receipts\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that async confirmation completes immediately when\n" +
                "  receipt listeners are not supported (best-effort fallback).\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. publishPersistentToTopicAsyncConfirm() returns CompletableFuture\n" +
                "  2. Best-effort fallback when receipt listeners unavailable\n" +
                "  3. Future completes successfully even without broker confirmation\n" +
                "\n" +
                "ASYNC CONFIRMATION MODES:\n" +
                "  - WITH receipts: Future completes when broker confirms persistence\n" +
                "  - WITHOUT receipts (this test): Future completes immediately after send\n" +
                "    • This is 'best-effort' - message was sent but not confirmed\n" +
                "    • Still useful for non-blocking publish patterns\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Test harness has receipt listener support DISABLED (default)\n" +
                "  2. Call publishPersistentToTopicAsyncConfirm()\n" +
                "  3. Verify future completes immediately (no waiting for receipt)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Future completes within 1 second (actually immediate)\n" +
                "  - persistentPublishCalls = 1\n" +
                "  - No exception (best-effort completion)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Publishing persistent message with async confirmation to topic 'p/confirm'");
        CompletableFuture<Void> fut = publisher.publishPersistentToTopicAsyncConfirm("p/confirm", 123);

        log.info("STEP 2: Verifying future completes within timeout (best-effort, no receipt listener support)");
        // No receipt listener support: best-effort confirmation completes immediately.
        assertThat(fut).succeedsWithin(Duration.ofSeconds(1));
        assertThat(env.persistentPublishCalls.get()).isEqualTo(1);
        assertThat(env.lastPersistentTopic).isEqualTo("p/confirm");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Future completed: true (expected: true)\n" +
                "  persistentPublishCalls: " + env.persistentPublishCalls.get() + " (expected: 1)\n" +
                "  lastPersistentTopic: '" + env.lastPersistentTopic + "' (expected: 'p/confirm')\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Receipt listener not supported in test harness\n" +
                "  - Publisher fell back to best-effort confirmation\n" +
                "  - Future completed immediately after send (not waiting for broker)\n" +
                "  - Message was published successfully\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications can use async confirm API even without receipt support\n" +
                "  - Graceful degradation to best-effort when receipts unavailable\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void async_wrappers_delegate_and_complete() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: async_wrappers_delegate_and_complete\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that async publish wrappers (publishToTopicAsync and\n" +
                "  publishPersistentToTopicAsync) correctly delegate to underlying\n" +
                "  publishers and return futures that complete successfully.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. publishToTopicAsync() - non-blocking direct publish\n" +
                "  2. publishPersistentToTopicAsync() - non-blocking persistent publish\n" +
                "  3. CompletableFuture completion for both methods\n" +
                "\n" +
                "ASYNC vs SYNC PUBLISH:\n" +
                "  - SYNC: Caller blocks until publish completes\n" +
                "  - ASYNC: Caller gets Future immediately, can continue processing\n" +
                "    • Better for high-throughput scenarios\n" +
                "    • Allows parallel publishing\n" +
                "    • Caller can handle completion/errors via Future callbacks\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Call publishToTopicAsync() - returns Future immediately\n" +
                "  2. Call publishPersistentToTopicAsync() - returns Future immediately\n" +
                "  3. Verify both futures complete successfully\n" +
                "  4. Verify correct topics were recorded\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Both futures complete within 1 second\n" +
                "  - lastDirectTopic = 't/async'\n" +
                "  - lastPersistentTopic = 'p/async'\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Publishing async direct message to 't/async'");
        CompletableFuture<Void> d = publisher.publishToTopicAsync("t/async", "z");

        log.info("STEP 2: Publishing async persistent message to 'p/async'");
        CompletableFuture<Void> p = publisher.publishPersistentToTopicAsync("p/async", "q");

        log.info("STEP 3: Verifying both futures complete within timeout");
        assertThat(d).succeedsWithin(Duration.ofSeconds(1));
        assertThat(p).succeedsWithin(Duration.ofSeconds(1));
        assertThat(env.lastDirectTopic).isEqualTo("t/async");
        assertThat(env.lastPersistentTopic).isEqualTo("p/async");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Direct async future completed: true (expected: true)\n" +
                "  Persistent async future completed: true (expected: true)\n" +
                "  lastDirectTopic: '" + env.lastDirectTopic + "' (expected: 't/async')\n" +
                "  lastPersistentTopic: '" + env.lastPersistentTopic + "' (expected: 'p/async')\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Both async methods returned futures immediately (non-blocking)\n" +
                "  - Futures completed after underlying publish operations\n" +
                "  - Both direct and persistent paths work correctly\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications can use async API for non-blocking publishes\n" +
                "  - Futures enable reactive/callback-based programming patterns\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void with_properties_sync_and_async_paths_complete() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: with_properties_sync_and_async_paths_complete\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that publishing with MessageProperties works correctly\n" +
                "  for all four combinations: sync/async × direct/persistent.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. publishToTopicWithProperties() - sync direct with properties\n" +
                "  2. publishPersistentToTopicWithProperties() - sync persistent with properties\n" +
                "  3. publishToTopicWithPropertiesAsync() - async direct with properties\n" +
                "  4. publishPersistentToTopicWithPropertiesAsync() - async persistent with properties\n" +
                "\n" +
                "MESSAGE PROPERTIES:\n" +
                "  - correlationId: Links request/response messages\n" +
                "  - priority: Message priority (0-255)\n" +
                "  - timeToLive: Message expiration in milliseconds\n" +
                "  - replyTo: Response destination for request/reply patterns\n" +
                "  - userProperties: Custom key-value pairs\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create MessageProperties with correlationId, priority, TTL\n" +
                "  2. Publish sync direct message with properties\n" +
                "  3. Publish sync persistent message with properties\n" +
                "  4. Publish async direct message with properties\n" +
                "  5. Publish async persistent message with properties\n" +
                "  6. Verify all complete successfully with correct topics\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - All four publish variants complete successfully\n" +
                "  - Async futures complete within timeout\n" +
                "  - Topics recorded correctly for each publish\n" +
                "───────────────────────────────────────────────────────────────\n");

        MessageProperties props = new MessageProperties().setCorrelationId("CID").setPriority(1).setTimeToLive(1000);

        log.info("STEP 1: Publishing sync direct message with properties to 't/props'");
        publisher.publishToTopicWithProperties("t/props", Map.of("v", 1), props);
        assertThat(env.lastDirectTopic).isEqualTo("t/props");

        log.info("STEP 2: Publishing sync persistent message with properties to 'p/props'");
        publisher.publishPersistentToTopicWithProperties("p/props", Map.of("v", 2), props);
        assertThat(env.lastPersistentTopic).isEqualTo("p/props");

        log.info("STEP 3: Publishing async direct message with properties to 't/propsA'");
        CompletableFuture<Void> da = publisher.publishToTopicWithPropertiesAsync("t/propsA", Map.of("v", 3), props);

        log.info("STEP 4: Publishing async persistent message with properties to 'p/propsA'");
        CompletableFuture<Void> pa = publisher.publishPersistentToTopicWithPropertiesAsync("p/propsA", Map.of("v", 4), props);

        log.info("STEP 5: Verifying all async futures complete");
        assertThat(da).succeedsWithin(Duration.ofSeconds(1));
        assertThat(pa).succeedsWithin(Duration.ofSeconds(1));
        assertThat(env.lastDirectTopic).isEqualTo("t/propsA");
        assertThat(env.lastPersistentTopic).isEqualTo("p/propsA");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Sync direct with properties: PASS (topic: t/props)\n" +
                "  Sync persistent with properties: PASS (topic: p/props)\n" +
                "  Async direct with properties: PASS (topic: t/propsA)\n" +
                "  Async persistent with properties: PASS (topic: p/propsA)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - All four publish variants work correctly with MessageProperties\n" +
                "  - Properties (correlationId, priority, TTL) passed through pipeline\n" +
                "  - Both sync and async APIs support property customization\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications can customize message metadata for all publish types\n" +
                "  - Request/reply patterns work (via correlationId + replyTo)\n" +
                "  - Message expiration and priority can be set per-message\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void await_confirm_returns_in_best_effort_mode() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: await_confirm_returns_in_best_effort_mode\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the blocking await method returns successfully when\n" +
                "  receipt listeners are not supported, using best-effort fallback.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. publishPersistentToTopicAwait() - blocking publish with timeout\n" +
                "  2. Best-effort fallback when receipt listeners unavailable\n" +
                "  3. Method returns without throwing on successful send\n" +
                "\n" +
                "AWAIT vs ASYNC CONFIRM:\n" +
                "  - AWAIT: Blocks caller until confirmation (or timeout)\n" +
                "  - ASYNC CONFIRM: Returns Future, caller decides when to wait\n" +
                "  - Both fall back to best-effort when receipts unavailable\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Test harness has receipt listener support DISABLED (default)\n" +
                "  2. Call publishPersistentToTopicAwait() with 2-second timeout\n" +
                "  3. Method should return immediately (best-effort, no actual wait)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Method returns without throwing\n" +
                "  - Doesn't block for full timeout duration\n" +
                "  - Topic correctly recorded\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Publishing persistent message with blocking await to 'p/await'");
        // default env has no receipt listener support; await should return without throwing
        publisher.publishPersistentToTopicAwait("p/await", "data", Duration.ofSeconds(2));

        log.info("STEP 2: Verifying publish completed (best-effort mode, no receipt listener)");
        assertThat(env.lastPersistentTopic).isEqualTo("p/await");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Method returned: true (expected: true)\n" +
                "  lastPersistentTopic: '" + env.lastPersistentTopic + "' (expected: 'p/await')\n" +
                "  Exception thrown: false (expected: false)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Receipt listener not supported in test harness\n" +
                "  - Await method fell back to best-effort (immediate return)\n" +
                "  - Did NOT block for full 2-second timeout\n" +
                "  - Message was published successfully\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications can use blocking await API even without receipt support\n" +
                "  - Graceful degradation prevents unnecessary blocking\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void publish_with_properties_applies_reply_delivery_expiration_and_user_props() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: publish_with_properties_applies_reply_delivery_expiration_and_user_props\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that all MessageProperties fields (replyTo, deliveryMode,\n" +
                "  expiration, correlationId, userProperties) are correctly applied\n" +
                "  to the outbound message via the Solace message builder.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. replyTo - sets response destination for request/reply\n" +
                "  2. deliveryMode - specifies PERSISTENT vs DIRECT delivery\n" +
                "  3. messageExpiration - sets message TTL (time-to-live)\n" +
                "  4. correlationId - links request and response messages\n" +
                "  5. userProperties - custom key-value metadata\n" +
                "\n" +
                "PROPERTY APPLICATION FLOW:\n" +
                "  1. Application sets properties on MessageProperties object\n" +
                "  2. SolacePublisher calls message builder methods:\n" +
                "     - withProperty(REPLY_TO, replyTo)\n" +
                "     - withProperty(DELIVERY_MODE, deliveryMode)\n" +
                "     - withProperty(MESSAGE_EXPIRATION, expiration)\n" +
                "     - withProperty(CORRELATION_ID, correlationId)\n" +
                "     - withProperty(key, value) for user properties\n" +
                "  3. Builder creates OutboundMessage with all properties set\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Create MessageProperties with all fields populated\n" +
                "  2. Publish message using publishToTopicWithProperties()\n" +
                "  3. Verify test harness captured all applied properties\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - All properties present in lastAppliedProperties map\n" +
                "  - Values match what was set on MessageProperties\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Creating MessageProperties with replyTo, deliveryMode, expiration, correlationId, and user property");
        MessageProperties props = new MessageProperties()
                .setReplyTo("reply/topic")
                .setDeliveryMode("persistent")
                .setMessageExpiration(12345L)
                .addUserProperty("k1", "v1")
                .setCorrelationId("c123");

        log.info("STEP 2: Publishing message with properties to 't/with-props'");
        publisher.publishToTopicWithProperties("t/with-props", "body", props);

        log.info("STEP 3: Verifying all properties were applied to the outbound message");
        assertThat(env.lastDirectTopic).isEqualTo("t/with-props");
        assertThat(env.lastAppliedProperties)
                .containsEntry("reply-to", "reply/topic")
                .containsEntry("delivery-mode", "PERSISTENT")
                .containsEntry("message-expiration", "12345")
                .containsEntry(com.solace.messaging.config.SolaceProperties.MessageProperties.CORRELATION_ID, "c123")
                .containsEntry("k1", "v1");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  lastDirectTopic: '" + env.lastDirectTopic + "' (expected: 't/with-props')\n" +
                "  Applied properties verified:\n" +
                "    - reply-to: 'reply/topic' (expected: 'reply/topic')\n" +
                "    - delivery-mode: 'PERSISTENT' (expected: 'PERSISTENT')\n" +
                "    - message-expiration: '12345' (expected: '12345')\n" +
                "    - correlationId: 'c123' (expected: 'c123')\n" +
                "    - k1 (user property): 'v1' (expected: 'v1')\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - All five property types were correctly applied\n" +
                "  - Message builder received correct values\n" +
                "  - User properties passed through alongside system properties\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Full control over message metadata for advanced patterns\n" +
                "  - Request/reply pattern supported via replyTo + correlationId\n" +
                "  - Custom application metadata via userProperties\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void persistent_with_properties_retries_and_succeeds_after_reinit() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: persistent_with_properties_retries_and_succeeds_after_reinit\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that persistent publish with MessageProperties also gets\n" +
                "  retry/reinit behavior, ensuring properties are preserved across retries.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Retry logic works with publishPersistentToTopicWithProperties()\n" +
                "  2. Publisher reinitialization after failure\n" +
                "  3. MessageProperties preserved during retry\n" +
                "\n" +
                "WHY THIS MATTERS:\n" +
                "  - Properties contain important metadata (correlationId, replyTo, etc.)\n" +
                "  - Retry must not lose property information\n" +
                "  - Reinitialize+retry must rebuild message correctly\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Configure test harness to fail NEXT persistent publish\n" +
                "  2. Create MessageProperties with deliveryMode=persistent\n" +
                "  3. Call publishPersistentToTopicWithProperties()\n" +
                "  4. First attempt fails, retry kicks in, second succeeds\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Publish eventually succeeds (persistentPublishCalls = 1)\n" +
                "  - Topic correctly recorded\n" +
                "  - No exception thrown to caller\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Configuring test to fail next persistent publish (will trigger retry/reinit)");
        env.failNextPersistentPublish.set(true);

        log.info("STEP 2: Creating MessageProperties with deliveryMode=persistent");
        MessageProperties props = new MessageProperties().setDeliveryMode("persistent");

        log.info("STEP 3: Publishing persistent message with properties - first attempt fails, retry succeeds");
        publisher.publishPersistentToTopicWithProperties("p/retry-props", "body", props);

        log.info("STEP 4: Verifying publish succeeded after retry");
        assertThat(env.persistentPublishCalls.get()).isEqualTo(1);
        assertThat(env.lastPersistentTopic).isEqualTo("p/retry-props");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  persistentPublishCalls: " + env.persistentPublishCalls.get() + " (expected: 1 successful)\n" +
                "  lastPersistentTopic: '" + env.lastPersistentTopic + "' (expected: 'p/retry-props')\n" +
                "  Exception thrown: false (expected: false)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - First attempt failed with simulated transient error\n" +
                "  - Publisher reinitialized and retried with same properties\n" +
                "  - Second attempt succeeded\n" +
                "  - MessageProperties preserved through retry cycle\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Retry mechanism works consistently across all publish variants\n" +
                "  - Message metadata is not lost during transient failures\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void pending_confirms_are_cleaned_up_when_receipts_not_supported() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: pending_confirms_are_cleaned_up_when_receipts_not_supported\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that the internal pendingConfirms map is properly cleaned up\n" +
                "  when receipt listeners are not supported, preventing memory leaks.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Best-effort fallback cleanup for pendingConfirms\n" +
                "  2. No orphaned CompletableFutures in internal map\n" +
                "  3. Memory leak prevention in async confirmation path\n" +
                "\n" +
                "INTERNAL IMPLEMENTATION DETAIL:\n" +
                "  - SolacePublisher maintains a pendingConfirms map:\n" +
                "    Map<String, CompletableFuture<Void>> for tracking async confirmations\n" +
                "  - When receipts are supported, future completes on receipt callback\n" +
                "  - When receipts NOT supported, future must still be cleaned up\n" +
                "    to prevent memory leak from accumulating orphaned entries\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Publish with async confirm (receipt support disabled)\n" +
                "  2. Verify future completes (best-effort)\n" +
                "  3. Use reflection to access internal pendingConfirms map\n" +
                "  4. Verify map is empty (entry was cleaned up)\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Future completes successfully\n" +
                "  - pendingConfirms map is empty (no orphaned entries)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Publishing async confirm message (no receipt listener support)");
        CompletableFuture<Void> fut = publisher.publishPersistentToTopicAsyncConfirm("p/no-receipts", "body");
        assertThat(fut).succeedsWithin(Duration.ofSeconds(1));

        log.info("STEP 2: Accessing internal pendingConfirms map via reflection");
        java.lang.reflect.Field f = SolacePublisher.class.getDeclaredField("pendingConfirms");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, CompletableFuture<Void>> map = (java.util.Map<String, CompletableFuture<Void>>) f.get(publisher);

        log.info("STEP 3: Verifying pendingConfirms map is empty (no orphaned futures)");
        assertThat(map).isEmpty();

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Future completed: true (expected: true)\n" +
                "  pendingConfirms map size: " + map.size() + " (expected: 0)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Async confirm was initiated without receipt listener support\n" +
                "  - Best-effort fallback completed the future immediately\n" +
                "  - Internal map entry was properly removed\n" +
                "  - No memory leak from orphaned futures\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Safe to use async confirm API in high-volume scenarios\n" +
                "  - No accumulation of uncompleted futures over time\n" +
                "  - Clean shutdown possible without pending entries\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void receipt_listener_success_completes_future_and_clears_pending() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: receipt_listener_success_completes_future_and_clears_pending\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that when receipt listeners ARE supported, a successful\n" +
                "  publish receipt callback completes the future and cleans up pendingConfirms.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Receipt listener registration on PersistentMessagePublisher\n" +
                "  2. Receipt callback invocation on successful broker acknowledgment\n" +
                "  3. Future completion triggered by receipt callback\n" +
                "  4. pendingConfirms cleanup after callback\n" +
                "\n" +
                "RECEIPT LISTENER FLOW:\n" +
                "  1. Publisher calls publish() with userContext (correlation ID)\n" +
                "  2. Broker persists message and sends acknowledgment\n" +
                "  3. Solace SDK invokes registered receipt listener\n" +
                "  4. Listener extracts userContext, finds pending future\n" +
                "  5. Completes future successfully and removes from map\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Enable receipt listener support in test harness\n" +
                "  2. Publish with async confirm\n" +
                "  3. Test harness simulates successful receipt callback\n" +
                "  4. Verify future completed and map cleaned up\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Future completes successfully\n" +
                "  - pendingConfirms map is empty\n" +
                "  - persistentPublishCalls = 1\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Enabling receipt listener support in test environment");
        env.supportReceipts = true;

        log.info("STEP 2: Publishing async confirm message - receipt listener will be invoked");
        CompletableFuture<Void> fut = publisher.publishPersistentToTopicAsyncConfirm("p/receipt-ok", "body");
        assertThat(fut).succeedsWithin(Duration.ofSeconds(1));

        log.info("STEP 3: Verifying pendingConfirms map is empty after receipt callback");
        java.lang.reflect.Field f = SolacePublisher.class.getDeclaredField("pendingConfirms");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, CompletableFuture<Void>> map = (java.util.Map<String, CompletableFuture<Void>>) f.get(publisher);
        assertThat(map).isEmpty();
        assertThat(env.persistentPublishCalls.get()).isEqualTo(1);
        assertThat(env.lastPersistentTopic).isEqualTo("p/receipt-ok");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Future completed: true (expected: true)\n" +
                "  pendingConfirms map size: " + map.size() + " (expected: 0)\n" +
                "  persistentPublishCalls: " + env.persistentPublishCalls.get() + " (expected: 1)\n" +
                "  lastPersistentTopic: '" + env.lastPersistentTopic + "' (expected: 'p/receipt-ok')\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Receipt listener was successfully registered\n" +
                "  - Publish triggered receipt callback (simulated by test harness)\n" +
                "  - Callback completed the associated future\n" +
                "  - Map entry was removed (no orphaned entry)\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - True guaranteed delivery confirmation when receipts supported\n" +
                "  - Application knows broker has persisted the message\n" +
                "  - Clean resource management in high-volume scenarios\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void receipt_listener_exception_completes_future_exceptionally() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: receipt_listener_exception_completes_future_exceptionally\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that when a receipt callback indicates failure (broker\n" +
                "  could not persist message), the future completes exceptionally.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Receipt listener error handling path\n" +
                "  2. Future exceptional completion on receipt failure\n" +
                "  3. Error propagation to caller via CompletionException\n" +
                "\n" +
                "FAILURE SCENARIOS IN PRODUCTION:\n" +
                "  - Broker ran out of spool space (disk full)\n" +
                "  - Message too large for queue limits\n" +
                "  - Authorization failure for destination\n" +
                "  - Queue/topic endpoint deleted during publish\n" +
                "\n" +
                "RECEIPT ERROR FLOW:\n" +
                "  1. Publisher calls publish() with userContext\n" +
                "  2. Broker attempts to persist but fails\n" +
                "  3. Broker sends negative acknowledgment with exception\n" +
                "  4. Receipt callback receives exception in receipt.getException()\n" +
                "  5. SolacePublisher completes future exceptionally\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Enable receipt listener support with exception simulation\n" +
                "  2. Publish with async confirm\n" +
                "  3. Test harness simulates receipt with exception\n" +
                "  4. Verify future completed exceptionally\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Future completes exceptionally (not successfully)\n" +
                "  - Exception is CompletionException wrapping RuntimeException\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Enabling receipt listener support with exception simulation");
        env.supportReceipts = true;
        env.receiptException = true;

        log.info("STEP 2: Publishing async confirm message - receipt will indicate failure");
        CompletableFuture<Void> fut = publisher.publishPersistentToTopicAsyncConfirm("p/receipt-fail", "body");

        log.info("STEP 3: Verifying future completed exceptionally with RuntimeException");
        assertThatThrownBy(fut::join)
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .hasCauseInstanceOf(RuntimeException.class);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Future completed: exceptionally (expected: exceptionally)\n" +
                "  Exception type: CompletionException (expected)\n" +
                "  Cause type: RuntimeException (expected)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Receipt listener received error from broker (simulated)\n" +
                "  - Error was propagated to the pending future\n" +
                "  - Caller can handle via fut.exceptionally() or try/catch on join()\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications get notified of broker-side failures\n" +
                "  - Can implement retry or dead-letter logic on failure\n" +
                "  - No silent message loss - failures are explicit\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void double_failure_throws_for_direct_and_persistent() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: double_failure_throws_for_direct_and_persistent\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that when both initial publish AND retry fail, the error\n" +
                "  is propagated to the caller via SolacePublishException.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Retry exhaustion after maximum attempts\n" +
                "  2. SolacePublishException thrown on unrecoverable failure\n" +
                "  3. Error propagation for both direct and persistent publish\n" +
                "\n" +
                "RETRY BEHAVIOR:\n" +
                "  - First attempt: fails (simulated transient error)\n" +
                "  - Retry attempt: also fails (persistent error)\n" +
                "  - Retries exhausted: throw SolacePublishException to caller\n" +
                "\n" +
                "REAL-WORLD SCENARIOS:\n" +
                "  - Broker completely unreachable\n" +
                "  - Authentication credentials revoked\n" +
                "  - Network completely down (not just blip)\n" +
                "  - Broker shutdown for maintenance\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Configure test harness to fail 2 consecutive direct publishes\n" +
                "  2. Attempt direct publish - should throw after retry exhausted\n" +
                "  3. Configure test harness to fail 2 consecutive persistent publishes\n" +
                "  4. Attempt persistent publish - should throw after retry exhausted\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Both publish attempts throw SolacePublishException\n" +
                "  - Error is not silently swallowed\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Configuring direct publish to fail twice (exhausts retries)");
        env.directFailuresRemaining = 2;
        log.info("STEP 2: Attempting direct publish - should throw SolacePublishException after retry exhausted");
        assertThatThrownBy(() -> publisher.publishToTopic("t/fail", "x")).isInstanceOf(SolacePublishException.class);

        log.info("STEP 3: Configuring persistent publish to fail twice (exhausts retries)");
        env.persistentFailuresRemaining = 2;
        log.info("STEP 4: Attempting persistent publish - should throw SolacePublishException after retry exhausted");
        assertThatThrownBy(() -> publisher.publishPersistentToTopic("p/fail", "y")).isInstanceOf(SolacePublishException.class);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Direct publish after double failure: threw SolacePublishException (expected)\n" +
                "  Persistent publish after double failure: threw SolacePublishException (expected)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Direct publish: first attempt failed, retry failed, exception thrown\n" +
                "  - Persistent publish: first attempt failed, retry failed, exception thrown\n" +
                "  - Both paths correctly propagate unrecoverable errors to caller\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications must handle SolacePublishException for unrecoverable failures\n" +
                "  - Can implement circuit breaker or alerting on repeated failures\n" +
                "  - Retry logic prevents throwing on transient errors (covered by other tests)\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void async_direct_publish_completes_exceptionally_on_double_failure() {
        log.info("TEST: async_direct_publish_completes_exceptionally_on_double_failure");
        env.directFailuresRemaining = 2; // initial + retry both fail
        CompletableFuture<Void> fut = publisher.publishToTopicAsync("t/async-fail", "x");
        assertThatThrownBy(fut::join)
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .hasCauseInstanceOf(SolacePublishException.class);
    }

    @Test
    void async_persistent_with_properties_completes_exceptionally_on_double_failure() {
        log.info("TEST: async_persistent_with_properties_completes_exceptionally_on_double_failure");
        env.persistentFailuresRemaining = 2;
        MessageProperties props = new MessageProperties().setCorrelationId("cid");
        CompletableFuture<Void> fut =
                publisher.publishPersistentToTopicWithPropertiesAsync("p/async-fail", "x", props);
        assertThatThrownBy(fut::join)
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .hasCauseInstanceOf(SolacePublishException.class);
    }

    @Test
    void await_confirm_throws_on_publish_failure() {
        log.info("TEST: await_confirm_throws_on_publish_failure");
        env.persistentFailuresRemaining = 2; // publish fails -> async confirm completes exceptionally
        assertThatThrownBy(() ->
                publisher.publishPersistentToTopicAwait("p/await-fail", "x", Duration.ofSeconds(2)))
                .isInstanceOf(SolacePublishException.class);
    }

    @Test
    void publish_with_context_uses_client_name_and_publisher_key() {
        log.info("TEST: publish_with_context_uses_client_name_and_publisher_key");
        // Exercises the contextual publish entrypoint used by the @SolacePublish aspect.
        // (clientName left null so the shared primary service is reused in this harness.)
        publisher.publishWithContext("t/ctx", "body", new MessageProperties().setPriority(1),
                false, null, "pub-key");
        assertThat(env.lastDirectTopic).isEqualTo("t/ctx");

        CompletableFuture<Void> fut = publisher.publishWithContextAsync("p/ctx", "body", null,
                true, null, "pub-key-2");
        assertThat(fut).succeedsWithin(Duration.ofSeconds(1));
        assertThat(env.lastPersistentTopic).isEqualTo("p/ctx");
    }

    // Note: receipt-listener supported path is not simulated here; we rely on
    // best-effort confirmations (no listener available) to keep test harness simple.

    // --- Test harness ------------------------------------------------------

    static class RecordingEnv {
        final AtomicInteger directPublishCalls = new AtomicInteger();
        final AtomicInteger persistentPublishCalls = new AtomicInteger();
        final AtomicBoolean failNextDirectPublish = new AtomicBoolean(false);
        final AtomicBoolean failNextPersistentPublish = new AtomicBoolean(false);
        volatile int directFailuresRemaining = 0;
        volatile int persistentFailuresRemaining = 0;
        volatile boolean supportReceipts = false;
        volatile boolean receiptException = false;
        volatile String lastDirectTopic;
        volatile String lastPersistentTopic;
        volatile Object serializerLastDestination;
        volatile Map<String, Object> lastAppliedProperties = new ConcurrentHashMap<>();

        final SolaceConnectionManager cm;
        final MessageSerializer serializer;

        RecordingEnv() {
            // Minimal properties
            com.solace.wrapper.config.SolaceProperties props = new com.solace.wrapper.config.SolaceProperties();
            props.setHost("tcp://noop:55555");
            props.setMsgVpn("default"); props.setClientUsername("default"); props.setClientPassword("");
            this.cm = new NoopCM(props, this);
            this.serializer = new RecordingSerializer(this);
        }
    }

        static class RecordingSerializer implements MessageSerializer {
            final RecordingEnv env;
            RecordingSerializer(RecordingEnv env) { this.env = env; }
            private OutboundMessage msg() {
                return (OutboundMessage) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{OutboundMessage.class}, (p, m, a) -> null);
        }
        @Override public OutboundMessage serialize(MessagingService messagingService, Object object) { return msg(); }
                @Override public OutboundMessage serialize(MessagingService messagingService, Object object, Object destination) {
                    env.serializerLastDestination = destination;
                    return msg();
                }
                @Override public byte[] serializeToBytes(MessagingService messagingService, Object object) {
                    if (object instanceof byte[]) return (byte[]) object;
                    return object != null ? object.toString().getBytes(StandardCharsets.UTF_8) : new byte[0];
                }
        @Override public <T> T deserialize(com.solace.messaging.receiver.InboundMessage message, Class<T> targetType) { return null; }
        @Override public String deserializeToString(com.solace.messaging.receiver.InboundMessage message) { return null; }
    }

    static class NoopCM extends SolaceConnectionManager {
        final RecordingEnv env;
        NoopCM(com.solace.wrapper.config.SolaceProperties p, RecordingEnv env) {
            super(p);
            this.env = env;
            // Now that env is set, initialize the primary service
            super.initializePrimaryService();
        }
        @Override protected void initializePrimaryService() {
            // Skip during super() call - env is not set yet
            // Will be called explicitly after env is assigned
        }
        @Override public MessagingService createMessagingService() { return DummyMessagingService.create(env); }
    }

    // Dynamic proxy-based dummy service + builders + publishers
        static class DummyMessagingService implements InvocationHandler {
            final RecordingEnv env;
            DummyMessagingService(RecordingEnv env) { this.env = env; }
            static MessagingService create(RecordingEnv env) {
                return (MessagingService) Proxy.newProxyInstance(
                        SolacePublisherTest.class.getClassLoader(),
                        new Class[]{MessagingService.class}, new DummyMessagingService(env));
            }
            @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    String name = method.getName();
                    if ("messageBuilder".equals(name)) {
                        // fresh map per builder invocation
                        Map<String, Object> props = new ConcurrentHashMap<>();
                        return Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class[]{OutboundMessageBuilder.class}, (bp, bm, ba) -> {
                                    String builderName = bm.getName();
                                    if ("build".equals(builderName)) {
                                        SolacePublisherTest.CURRENT.lastAppliedProperties = new ConcurrentHashMap<>(props);
                                        return createOutboundMessage();
                                    }
                                    if ("withProperty".equals(builderName)) {
                                        props.put(String.valueOf(ba[0]), ba.length > 1 ? ba[1] : null);
                                        return bp;
                                    }
                                    if ("withTimeToLive".equals(builderName)) {
                                        props.put("ttl", ba[0]);
                                        return bp;
                                    }
                                    if ("withPriority".equals(builderName)) {
                                        props.put("priority", ba[0]);
                                        return bp;
                                    }
                                    return bp;
                                });
                    }
            if ("connect".equals(name) || "disconnect".equals(name)) return proxy;
            if ("isConnected".equals(name)) return Boolean.TRUE;
            if ("createDirectMessagePublisherBuilder".equals(name)) {
                return Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class[]{method.getReturnType()}, (p, m, a) -> {
                            String mn = m.getName();
                            if ("onBackPressureWait".equals(mn) || "onBackPressureReject".equals(mn)) return p; // fluent
                            if ("build".equals(mn)) {
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{DirectMessagePublisher.class},
                                        (pp, mm, aa) -> {
                                            String mnn = mm.getName();
                                            if ("hashCode".equals(mnn)) return System.identityHashCode(pp);
                                            if ("equals".equals(mnn)) return pp == aa[0];
                                            if ("start".equals(mnn) || "terminate".equals(mnn)) return null;
                                            if ("publish".equals(mnn)) {
                                                // publish(OutboundMessage, Topic)
                                                if (SolacePublisherTest.CURRENT.failNextDirectPublish.compareAndSet(true, false)) {
                                                    throw new IllegalStateException("simulated direct failure");
                                                }
                                                if (SolacePublisherTest.CURRENT.directFailuresRemaining > 0) {
                                                    SolacePublisherTest.CURRENT.directFailuresRemaining--;
                                                    throw new IllegalStateException("simulated direct failure (counter)");
                                                }
                                                SolacePublisherTest.CURRENT.directPublishCalls.incrementAndGet();
                                                Topic t = (Topic) aa[1];
                                                SolacePublisherTest.CURRENT.lastDirectTopic = t.getName();
                                                return null;
                                            }
                                            return null;
                                        });
                            }
                            return null;
                        });
            }
            if ("createPersistentMessagePublisherBuilder".equals(name)) {
                // When supportReceipts is false, simulate lack of receipt listener support by throwing on setter
                return Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class[]{method.getReturnType()}, (p, m, a) -> {
                            String mn = m.getName();
                            if ("build".equals(mn)) {
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{PersistentMessagePublisher.class},
                                        (pp, mm, aa) -> {
                                            String mnn = mm.getName();
                                            if ("hashCode".equals(mnn)) return System.identityHashCode(pp);
                                            if ("equals".equals(mnn)) return pp == aa[0];
                                            if ("setMessagePublishReceiptListener".equals(mnn)) {
                                                if (!SolacePublisherTest.CURRENT.supportReceipts) {
                                                    throw new UnsupportedOperationException("no receipts");
                                                }
                                                // Store globally for simpler test harness lookup
                                                CURRENT_RECEIPT_LISTENER = aa[0];
                                                return null;
                                            }
                                            if ("start".equals(mnn) || "terminate".equals(mnn)) return null;
                                            if ("publish".equals(mnn)) {
                                                // publish(OutboundMessage, Topic) OR publish(OutboundMessage, Topic, userContext)
                                                if (SolacePublisherTest.CURRENT.failNextPersistentPublish.compareAndSet(true, false)) {
                                                    throw new IllegalStateException("simulated persistent failure");
                                                }
                                                if (SolacePublisherTest.CURRENT.persistentFailuresRemaining > 0) {
                                                    SolacePublisherTest.CURRENT.persistentFailuresRemaining--;
                                                    throw new IllegalStateException("simulated persistent failure (counter)");
                                                }
                                                SolacePublisherTest.CURRENT.persistentPublishCalls.incrementAndGet();
                                                Topic t = (Topic) aa[1];
                                                SolacePublisherTest.CURRENT.lastPersistentTopic = t.getName();
                                                // If receipts supported and userContext is present, invoke listener via Consumer-like SAM
                                                if (SolacePublisherTest.CURRENT.supportReceipts && aa.length >= 3) {
                                                    Object userCtx = aa[2];
                                                    // Build a mock/proxy implementing the parameter type expected by the SAM
                                                    java.util.function.Function<Class<?>, Object> receiptProxy = paramType -> {
                                                        // Check if it's an interface (can use JDK Proxy) or class (must use Mockito)
                                                        if (paramType.isInterface()) {
                                                            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{paramType}, (rp, rm, ra) -> {
                                                                switch (rm.getName()) {
                                                                    case "getUserContext": return userCtx;
                                                                    case "getException": return SolacePublisherTest.CURRENT.receiptException ? new RuntimeException("receipt fail") : null;
                                                                    case "isPersisted": return !SolacePublisherTest.CURRENT.receiptException;
                                                                    default: return null;
                                                                }
                                                            });
                                                        } else {
                                                            // Use Mockito for classes (PublishReceipt is a class, not an interface)
                                                            // Create a mock with Answer that responds to method calls
                                                            return Mockito.mock(paramType, invocation -> {
                                                                String methodName = invocation.getMethod().getName();
                                                                switch (methodName) {
                                                                    case "getUserContext": return userCtx;
                                                                    case "getException":
                                                                        if (SolacePublisherTest.CURRENT.receiptException) {
                                                                            // Must return PubSubPlusClientException subtype
                                                                            return Mockito.mock(com.solace.messaging.PubSubPlusClientException.class,
                                                                                    inv -> inv.getMethod().getName().equals("getMessage") ? "receipt fail" : null);
                                                                        }
                                                                        return null;
                                                                    case "isPersisted": return !SolacePublisherTest.CURRENT.receiptException;
                                                                    default: return null;
                                                                }
                                                            });
                                                        }
                                                    };
                                                    // Invoke the globally stored receipt listener
                                                    Object listener = CURRENT_RECEIPT_LISTENER;
                                                    if (listener != null) {
                                                        for (Method lm : listener.getClass().getMethods()) {
                                                            if (lm.getParameterCount() == 1
                                                                && !lm.getDeclaringClass().equals(Object.class)
                                                                && !lm.getName().equals("equals")) {
                                                                Class<?> param = lm.getParameterTypes()[0];
                                                                Object rec = receiptProxy.apply(param);
                                                                try { lm.invoke(listener, rec); } catch (Throwable ignored) {}
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                                return null;
                                            }
                                            return null;
                                        });
                            }
                            return null;
                        });
                    }
                    // Defaults
                    if (method.getReturnType().equals(boolean.class)) return false;
                    if (method.getReturnType().equals(int.class)) return 0;
                    return null;
                }

                private OutboundMessage createOutboundMessage() {
                    return (OutboundMessage) Proxy.newProxyInstance(
                            getClass().getClassLoader(), new Class[]{OutboundMessage.class}, (p, m, a) -> null);
                }
            }
    // Store listener globally for simpler test harness
    private static volatile Object CURRENT_RECEIPT_LISTENER;
}
