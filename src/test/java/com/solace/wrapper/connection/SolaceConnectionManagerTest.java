package com.solace.wrapper.connection;

import com.solace.messaging.MessagingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class SolaceConnectionManagerTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceConnectionManagerTest.class);

    static class CountingCM extends SolaceConnectionManager {
        AtomicInteger created = new AtomicInteger();
        volatile MessagingService lastService;
        volatile String lastClientNameOverride;

        CountingCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }

        @Override public MessagingService createMessagingService() {
            return createStubService(null);
        }

        @Override public MessagingService createMessagingService(String clientNameOverride) {
            return createStubService(clientNameOverride);
        }

        private MessagingService createStubService(String clientNameOverride) {
            if (created == null) created = new AtomicInteger();
            created.incrementAndGet();
            lastClientNameOverride = clientNameOverride;
            AtomicBoolean connected = new AtomicBoolean(true);
            MessagingService svc = (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (p, m, a) -> {
                        String n = m.getName();
                        if ("connect".equals(n)) return p;
                        if ("disconnect".equals(n)) { connected.set(false); return null; }
                        if ("isConnected".equals(n)) return connected.get();
                        // Provide equals/hashCode for map keys
                        if ("hashCode".equals(n)) return System.identityHashCode(p);
                        if ("equals".equals(n)) return p == a[0];
                        return null;
                    });
            lastService = svc;
            return svc;
        }
    }

    private CountingCM cm;

    @BeforeEach
    void init() {
        com.solace.wrapper.config.SolaceProperties props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555"); props.setMsgVpn("default"); props.setClientUsername("default"); props.setClientPassword("");
        cm = new CountingCM(props);
    }

    @Test
    void getPrimaryService_reconnects_when_disconnected() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: getPrimaryService_reconnects_when_disconnected\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that getPrimaryService() automatically reconnects when the\n" +
                "  current service is disconnected. This is critical for resilience.\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Auto-reconnection logic in SolaceConnectionManager\n" +
                "  2. Detection of disconnected state via isConnected() check\n" +
                "  3. Lazy creation of new MessagingService on demand\n" +
                "\n" +
                "REAL-WORLD SCENARIOS THIS SIMULATES:\n" +
                "  - Network disruption causes existing connection to drop\n" +
                "  - Broker restart terminates all client connections\n" +
                "  - Load balancer drains connections to a specific broker instance\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. Get initial primary service (triggers creation)\n" +
                "  2. Simulate disconnect by calling disconnect() on the service\n" +
                "  3. Call getPrimaryService() again - should detect disconnect and create new service\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - First call returns a valid service\n" +
                "  - After disconnect, next call creates a NEW service\n" +
                "  - Creation count increases (proves new service was created)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Getting initial primary service");
        MessagingService s1 = cm.getPrimaryService();
        assertThat(s1).isNotNull();
        int c1 = cm.created.get();
        log.info("        Initial service obtained, created count=" + c1);

        log.info("STEP 2: Simulating disconnect on primary service");
        log.info("        (This simulates network failure or broker disconnect)");
        // Flip current primary to disconnected by calling disconnect()
        try { s1.getClass().getMethod("disconnect").invoke(s1); } catch (Exception ignore) {}

        log.info("STEP 3: Getting primary service again - should trigger reconnect");
        MessagingService s2 = cm.getPrimaryService();
        assertThat(s2).isNotNull();
        assertThat(cm.created.get()).isGreaterThan(c1);

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Initial service created: count=" + c1 + "\n" +
                "  After disconnect + getPrimaryService(): count=" + cm.created.get() + "\n" +
                "  New service created: " + (cm.created.get() > c1) + " (expected: true)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - getPrimaryService() detected that existing service was disconnected\n" +
                "  - A new MessagingService was automatically created\n" +
                "  - Application code using getPrimaryService() is insulated from reconnection logic\n" +
                "\n" +
                "IMPLICATIONS:\n" +
                "  - Applications don't need explicit reconnection handling\n" +
                "  - Connection manager provides transparent failover\n" +
                "  - Each call to getPrimaryService() guarantees a connected service\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void createConsumerService_reuses_or_isolates_based_on_property() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createConsumerService_reuses_or_isolates_based_on_property\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that consumer services can either share the primary connection\n" +
                "  (default) or use isolated connections (when isolateConsumers=true).\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Default connection sharing for consumers (resource efficiency)\n" +
                "  2. isolateConsumers property for connection isolation\n" +
                "  3. Each isolated consumer gets its own MessagingService instance\n" +
                "\n" +
                "WHY CONNECTION ISOLATION MATTERS:\n" +
                "  - SHARED (default): All consumers use one connection = fewer broker resources\n" +
                "  - ISOLATED: Each consumer has dedicated connection = fault isolation\n" +
                "    • If one consumer has issues, others are unaffected\n" +
                "    • Independent flow control per consumer\n" +
                "    • Better for high-throughput scenarios\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. With default settings, verify consumers A and B share primary service\n" +
                "  2. Enable isolateConsumers=true\n" +
                "  3. Verify new consumers X and Y each get unique, isolated services\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Default: cA == cB == primary (same instance)\n" +
                "  - Isolated: i1 != i2 != primary (different instances)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Default mode - consumers should reuse primary service");
        // Default: reuse primary
        MessagingService p = cm.getPrimaryService();
        MessagingService cA = cm.createConsumerService("A");
        MessagingService cB = cm.createConsumerService("B");
        assertThat(cA).isSameAs(p);
        assertThat(cB).isSameAs(p);
        log.info("        Consumer services A and B share primary service: PASS");

        log.info("STEP 2: Enabling isolateConsumers=true");
        log.info("        (This switches to isolated mode - each consumer gets dedicated connection)");
        // Enable isolation and ensure new instances
        cm.getProperties().setIsolateConsumers(true);

        log.info("STEP 3: Creating consumers X and Y with isolation enabled");
        MessagingService i1 = cm.createConsumerService("X");
        MessagingService i2 = cm.createConsumerService("Y");
        assertThat(i1).isNotSameAs(p);
        assertThat(i2).isNotSameAs(p);
        assertThat(i1).isNotSameAs(i2);
        log.info("        Isolated consumers X and Y each have unique service: PASS");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Default mode (isolateConsumers=false):\n" +
                "    - Consumer A shares primary: true (expected: true)\n" +
                "    - Consumer B shares primary: true (expected: true)\n" +
                "  Isolated mode (isolateConsumers=true):\n" +
                "    - Consumer X isolated from primary: true (expected: true)\n" +
                "    - Consumer Y isolated from primary: true (expected: true)\n" +
                "    - Consumer X isolated from Y: true (expected: true)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Default connection sharing reduces broker connection count\n" +
                "  - Isolation provides fault boundaries between consumers\n" +
                "  - Property change affects only NEW consumers (existing unchanged)\n" +
                "\n" +
                "USE CASE GUIDANCE:\n" +
                "  - Use shared (default) for most applications to minimize resources\n" +
                "  - Use isolated for high-throughput or mission-critical consumers\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void createPublisherService_reuses_or_isolates_based_on_property() {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: createPublisherService_reuses_or_isolates_based_on_property\n" +
                "───────────────────────────────────────────────────────────────\n" +
                "PURPOSE:\n" +
                "  Verify that publisher services can either share the primary connection\n" +
                "  (default) or use isolated connections (when isolatePublishers=true).\n" +
                "\n" +
                "FEATURES UNDER TEST:\n" +
                "  1. Default connection sharing for publishers (resource efficiency)\n" +
                "  2. isolatePublishers property for connection isolation\n" +
                "  3. Each isolated publisher gets its own MessagingService instance\n" +
                "\n" +
                "WHY PUBLISHER ISOLATION MATTERS:\n" +
                "  - SHARED (default): All publishers share one connection = efficient\n" +
                "  - ISOLATED: Each publisher has dedicated connection = \n" +
                "    • Independent flow control and back-pressure handling\n" +
                "    • One slow publisher doesn't block others\n" +
                "    • Better for mixed workloads (bulk vs real-time)\n" +
                "\n" +
                "TEST SCENARIO:\n" +
                "  1. With default settings, verify publishers A and B share primary service\n" +
                "  2. Enable isolatePublishers=true\n" +
                "  3. Verify new publishers X and Y each get unique, isolated services\n" +
                "\n" +
                "EXPECTED RESULT:\n" +
                "  - Default: pA == pB == primary (same instance)\n" +
                "  - Isolated: i1 != i2 != primary (different instances)\n" +
                "───────────────────────────────────────────────────────────────\n");

        log.info("STEP 1: Default mode - publishers should reuse primary service");
        // Default: reuse primary
        MessagingService p = cm.getPrimaryService();
        MessagingService pA = cm.createPublisherService("A", null);
        MessagingService pB = cm.createPublisherService("B", null);
        assertThat(pA).isSameAs(p);
        assertThat(pB).isSameAs(p);
        log.info("        Publisher services A and B share primary service: PASS");

        log.info("STEP 2: Enabling isolatePublishers=true");
        log.info("        (This switches to isolated mode - each publisher gets dedicated connection)");
        // Enable isolation and ensure new instances
        cm.getProperties().setIsolatePublishers(true);

        log.info("STEP 3: Creating publishers X and Y with isolation enabled");
        MessagingService i1 = cm.createPublisherService("X", null);
        MessagingService i2 = cm.createPublisherService("Y", null);
        assertThat(i1).isNotSameAs(p);
        assertThat(i2).isNotSameAs(p);
        assertThat(i1).isNotSameAs(i2);
        log.info("        Isolated publishers X and Y each have unique service: PASS");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT:\n" +
                "  Default mode (isolatePublishers=false):\n" +
                "    - Publisher A shares primary: true (expected: true)\n" +
                "    - Publisher B shares primary: true (expected: true)\n" +
                "  Isolated mode (isolatePublishers=true):\n" +
                "    - Publisher X isolated from primary: true (expected: true)\n" +
                "    - Publisher Y isolated from primary: true (expected: true)\n" +
                "    - Publisher X isolated from Y: true (expected: true)\n" +
                "\n" +
                "ANALYSIS:\n" +
                "  - Default connection sharing reduces broker connection count\n" +
                "  - Isolation provides independent flow control per publisher\n" +
                "  - Property change affects only NEW publishers (existing unchanged)\n" +
                "\n" +
                "USE CASE GUIDANCE:\n" +
                "  - Use shared (default) for most applications\n" +
                "  - Use isolated for high-throughput publishers or mixed workloads\n" +
                "\n" +
                "STATUS: PASS\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}
