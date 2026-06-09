package com.solace.wrapper.config;

import com.solace.messaging.MessagingService;
import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.metrics.SolaceMetrics;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.serialization.JsonMessageSerializer;
import com.solace.wrapper.serialization.MessageSerializer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SolaceAutoConfiguration}: the bean factory methods and the
 * {@code SolaceMetricsGaugeBinder} that registers connection/active-endpoint gauges after the
 * context is built. Uses a non-connecting connection manager and stub providers so no broker is
 * required.
 */
class SolaceAutoConfigurationTest {

    /** Connection manager that does not open a broker connection. */
    static class NoConnectCM extends SolaceConnectionManager {
        private final boolean connected;
        NoConnectCM(SolaceProperties p, boolean connected) { super(p); this.connected = connected; }
        @Override protected void initializePrimaryService() { }
        @Override public boolean isConnected() { return connected; }
        @Override public MessagingService createMessagingService() { return null; }
    }

    private static SolaceProperties props() {
        SolaceProperties p = new SolaceProperties();
        p.setHost("tcp://noop:55555");
        p.setMsgVpn("default");
        p.setClientUsername("default");
        p.setClientPassword("");
        return p;
    }

    @SuppressWarnings("unchecked")
    private static <T> ObjectProvider<T> provider(T value) {
        ObjectProvider<T> p = mock(ObjectProvider.class);
        when(p.getIfAvailable()).thenReturn(value);
        return p;
    }

    @Test
    void bean_factory_methods_build_components() {
        SolaceAutoConfiguration cfg = new SolaceAutoConfiguration();
        NoConnectCM cm = new NoConnectCM(props(), true);

        MessageSerializer serializer = cfg.messageSerializer();
        assertThat(serializer).isInstanceOf(JsonMessageSerializer.class);

        SolaceMetrics metrics = new SolaceMetrics(new SimpleMeterRegistry(), true);

        SolacePublisher publisher = cfg.solacePublisher(cm, serializer, provider(metrics));
        assertThat(publisher).isNotNull();

        SolaceConsumerManager manager = cfg.solaceConsumerManager(cm, serializer, provider(metrics));
        assertThat(manager).isNotNull();
        assertThat(manager.getMetrics()).isSameAs(metrics);

        publisher.shutdown(); // release the publisher executor
    }

    @Test
    void gauge_binder_registers_connection_and_endpoint_gauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        NoConnectCM cm = new NoConnectCM(props(), true);
        SolacePublisher publisher = mock(SolacePublisher.class);
        when(publisher.getActivePublisherCount()).thenReturn(3);
        SolaceConsumerManager manager = mock(SolaceConsumerManager.class);
        when(manager.getActiveConsumerCount()).thenReturn(5);

        SolaceAutoConfiguration.SolaceMetricsGaugeBinder binder =
                new SolaceAutoConfiguration.SolaceMetricsGaugeBinder(
                        provider(metrics), provider(cm), provider(publisher), provider(manager));

        binder.afterSingletonsInstantiated();

        assertThat(registry.get("solace.connection.up").gauge().value()).isEqualTo(1.0);
        assertThat(registry.get("solace.publishers.active").gauge().value()).isEqualTo(3.0);
        assertThat(registry.get("solace.consumers.active").gauge().value()).isEqualTo(5.0);
    }

    @Test
    void gauge_binder_handles_disconnected_and_absent_components() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        SolaceMetrics metrics = new SolaceMetrics(registry, true);

        NoConnectCM cm = new NoConnectCM(props(), false); // disconnected

        SolaceAutoConfiguration.SolaceMetricsGaugeBinder binder =
                new SolaceAutoConfiguration.SolaceMetricsGaugeBinder(
                        provider(metrics), provider(cm),
                        provider((SolacePublisher) null), provider((SolaceConsumerManager) null));

        binder.afterSingletonsInstantiated();

        assertThat(registry.get("solace.connection.up").gauge().value()).isEqualTo(0.0);
        // Absent publisher/consumer providers report zero rather than throwing.
        assertThat(registry.get("solace.publishers.active").gauge().value()).isEqualTo(0.0);
        assertThat(registry.get("solace.consumers.active").gauge().value()).isEqualTo(0.0);
    }

    @Test
    void gauge_binder_is_noop_when_metrics_disabled() {
        SolaceMetrics disabled = new SolaceMetrics(null); // not enabled
        NoConnectCM cm = new NoConnectCM(props(), true);

        SolaceAutoConfiguration.SolaceMetricsGaugeBinder binder =
                new SolaceAutoConfiguration.SolaceMetricsGaugeBinder(
                        provider(disabled), provider(cm),
                        provider((SolacePublisher) null), provider((SolaceConsumerManager) null));

        // Must not throw even though no registry is present.
        binder.afterSingletonsInstantiated();
        assertThat(disabled.isEnabled()).isFalse();
    }
}
