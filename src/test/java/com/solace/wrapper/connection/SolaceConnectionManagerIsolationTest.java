package com.solace.wrapper.connection;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.config.SolaceProperties;
import com.solace.wrapper.exception.SolaceConnectionException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the isolated-service paths of {@link SolaceConnectionManager}: per-consumer/per-publisher
 * pool creation, the max-connection limit guard, and disconnect-on-remove — using stub services so
 * no broker is required.
 */
class SolaceConnectionManagerIsolationTest {

    /** Connection manager whose services are non-connecting stubs that track disconnects. */
    static class StubCM extends SolaceConnectionManager {
        final Map<MessagingService, AtomicBoolean> connected = new ConcurrentHashMap<>();

        StubCM(SolaceProperties p) { super(p); }

        @Override protected void initializePrimaryService() { /* no broker */ }
        @Override public MessagingService createMessagingService() { return stub(); }
        @Override public MessagingService createMessagingService(String clientNameOverride) { return stub(); }

        private MessagingService stub() {
            AtomicBoolean up = new AtomicBoolean(true);
            MessagingService svc = (MessagingService) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{MessagingService.class}, (p, m, a) -> {
                        String n = m.getName();
                        if ("connect".equals(n)) return p;
                        if ("disconnect".equals(n)) { up.set(false); return null; }
                        if ("isConnected".equals(n)) return up.get();
                        if ("hashCode".equals(n)) return System.identityHashCode(p);
                        if ("equals".equals(n)) return p == a[0];
                        if (n.startsWith("add") && n.endsWith("Listener")) return null;
                        if (m.getReturnType().equals(boolean.class)) return false;
                        return null;
                    });
            connected.put(svc, up);
            return svc;
        }
    }

    private static SolaceProperties isolatedProps() {
        SolaceProperties p = new SolaceProperties();
        p.setHost("tcp://noop:55555");
        p.setMsgVpn("default");
        p.setClientUsername("default");
        p.setClientPassword("");
        p.setReconnectRetries(true);
        p.setIsolateConsumers(true);
        p.setIsolatePublishers(true);
        return p;
    }

    @Test
    void isolated_consumer_services_are_distinct_and_cached() {
        StubCM cm = new StubCM(isolatedProps());

        MessagingService a = cm.createConsumerService("c1");
        MessagingService a2 = cm.createConsumerService("c1");
        MessagingService b = cm.createConsumerService("c2");

        assertThat(a).isSameAs(a2);      // cached per id
        assertThat(a).isNotSameAs(b);    // distinct per id
        assertThat(cm.getServiceStats().getTotalConsumerServices()).isEqualTo(2);
    }

    @Test
    void consumer_pool_limit_is_enforced() {
        SolaceProperties p = isolatedProps();
        p.setMaxConsumerConnections(2);
        StubCM cm = new StubCM(p);

        cm.createConsumerService("c1");
        cm.createConsumerService("c2");
        assertThatThrownBy(() -> cm.createConsumerService("c3"))
                .isInstanceOf(SolaceConnectionException.class)
                .hasMessageContaining("Maximum consumer connections");
    }

    @Test
    void publisher_pool_limit_is_enforced() {
        SolaceProperties p = isolatedProps();
        p.setMaxPublisherConnections(1);
        StubCM cm = new StubCM(p);

        cm.createPublisherService("p1", null);
        assertThatThrownBy(() -> cm.createPublisherService("p2", null))
                .isInstanceOf(SolaceConnectionException.class)
                .hasMessageContaining("Maximum publisher connections");
    }

    @Test
    void remove_disconnects_isolated_consumer_service() {
        StubCM cm = new StubCM(isolatedProps());
        MessagingService svc = cm.createConsumerService("c1");
        assertThat(cm.connected.get(svc).get()).isTrue();

        cm.removeConsumerService("c1");

        assertThat(cm.connected.get(svc).get()).as("service disconnected on remove").isFalse();
        // Recreating after removal yields a fresh service.
        assertThat(cm.createConsumerService("c1")).isNotSameAs(svc);
    }

    @Test
    void remove_disconnects_isolated_publisher_service() {
        StubCM cm = new StubCM(isolatedProps());
        MessagingService svc = cm.createPublisherService("p1", null);

        cm.removePublisherService("p1");

        assertThat(cm.connected.get(svc).get()).isFalse();
    }

    @Test
    void shutdown_disconnects_all_isolated_services() {
        StubCM cm = new StubCM(isolatedProps());
        MessagingService c = cm.createConsumerService("c1");
        MessagingService p = cm.createPublisherService("p1", null);

        cm.shutdown();

        assertThat(cm.connected.get(c).get()).isFalse();
        assertThat(cm.connected.get(p).get()).isFalse();
        // After shutdown, further creation is rejected.
        assertThatThrownBy(() -> cm.createConsumerService("c2"))
                .isInstanceOf(SolaceConnectionException.class);
    }

    @Test
    void client_name_override_forces_isolated_service_even_when_isolation_off() {
        SolaceProperties p = new SolaceProperties();
        p.setHost("tcp://noop:55555");
        p.setMsgVpn("default");
        p.setClientUsername("default");
        p.setClientPassword("");
        // isolation disabled, but a clientName override should still create a dedicated service
        StubCM cm = new StubCM(p);

        MessagingService svc = cm.createConsumerService("c1", "custom-client");
        assertThat(svc).isNotNull();
        assertThat(cm.getServiceStats().getTotalConsumerServices()).isEqualTo(1);
    }
}
