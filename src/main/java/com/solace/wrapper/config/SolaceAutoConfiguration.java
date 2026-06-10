package com.solace.wrapper.config;

import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.metrics.SolaceMetrics;
import com.solace.wrapper.requestreply.SolaceRequestor;
import com.solace.wrapper.serialization.MessageSerializer;
import com.solace.wrapper.serialization.JsonMessageSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Auto-configuration class for Solace Spring Boot integration.
 */
@Configuration
@EnableConfigurationProperties({SolaceProperties.class, SolaceRequestReplyProperties.class})
@Import(SolaceAnnotationConfiguration.class)
public class SolaceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SolaceConnectionManager solaceConnectionManager(SolaceProperties properties) {
        return new SolaceConnectionManager(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageSerializer messageSerializer() {
        return new JsonMessageSerializer();
    }

    @Bean
    @ConditionalOnMissingBean
    public SolacePublisher solacePublisher(SolaceConnectionManager connectionManager,
                                         MessageSerializer messageSerializer,
                                         ObjectProvider<SolaceMetrics> metricsProvider) {
        SolacePublisher publisher = new SolacePublisher(connectionManager, messageSerializer);
        publisher.setMetrics(metricsProvider.getIfAvailable());
        return publisher;
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceConsumerManager solaceConsumerManager(SolaceConnectionManager connectionManager,
                                                      MessageSerializer messageSerializer,
                                                      ObjectProvider<SolaceMetrics> metricsProvider) {
        SolaceConsumerManager manager = new SolaceConsumerManager(connectionManager, messageSerializer);
        manager.setMetrics(metricsProvider.getIfAvailable());
        return manager;
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceRequestor solaceRequestor(SolaceConnectionManager connectionManager,
                                           MessageSerializer messageSerializer,
                                           SolaceRequestReplyProperties requestReplyProperties) {
        return new SolaceRequestor(connectionManager, messageSerializer,
                requestReplyProperties.getDefaultTimeoutMs());
    }

    /**
     * Binds connection-status and active-publisher/consumer gauges to the metrics registry once all
     * singletons are constructed. Uses lazy {@link ObjectProvider} suppliers evaluated at scrape time
     * to avoid bean initialization ordering issues. No-op when no {@link SolaceMetrics} bean exists.
     */
    @Bean
    @ConditionalOnMissingBean
    public SolaceMetricsGaugeBinder solaceMetricsGaugeBinder(
            ObjectProvider<SolaceMetrics> metricsProvider,
            ObjectProvider<SolaceConnectionManager> connectionManagerProvider,
            ObjectProvider<SolacePublisher> publisherProvider,
            ObjectProvider<SolaceConsumerManager> consumerManagerProvider) {
        return new SolaceMetricsGaugeBinder(metricsProvider, connectionManagerProvider,
                publisherProvider, consumerManagerProvider);
    }

    /**
     * Registers Solace gauges after the application context has finished creating singletons.
     */
    static class SolaceMetricsGaugeBinder implements org.springframework.beans.factory.SmartInitializingSingleton {

        private final ObjectProvider<SolaceMetrics> metricsProvider;
        private final ObjectProvider<SolaceConnectionManager> connectionManagerProvider;
        private final ObjectProvider<SolacePublisher> publisherProvider;
        private final ObjectProvider<SolaceConsumerManager> consumerManagerProvider;

        SolaceMetricsGaugeBinder(ObjectProvider<SolaceMetrics> metricsProvider,
                                 ObjectProvider<SolaceConnectionManager> connectionManagerProvider,
                                 ObjectProvider<SolacePublisher> publisherProvider,
                                 ObjectProvider<SolaceConsumerManager> consumerManagerProvider) {
            this.metricsProvider = metricsProvider;
            this.connectionManagerProvider = connectionManagerProvider;
            this.publisherProvider = publisherProvider;
            this.consumerManagerProvider = consumerManagerProvider;
        }

        @Override
        public void afterSingletonsInstantiated() {
            SolaceMetrics metrics = metricsProvider.getIfAvailable();
            if (metrics == null || !metrics.isEnabled()) {
                return;
            }
            metrics.registerConnectionStatusGauge(() -> {
                SolaceConnectionManager cm = connectionManagerProvider.getIfAvailable();
                return cm != null && cm.isConnected();
            });
            metrics.registerActivePublishersGauge(() -> {
                SolacePublisher publisher = publisherProvider.getIfAvailable();
                return publisher != null ? publisher.getActivePublisherCount() : 0;
            });
            metrics.registerActiveConsumersGauge(() -> {
                SolaceConsumerManager manager = consumerManagerProvider.getIfAvailable();
                return manager != null ? manager.getActiveConsumerCount() : 0;
            });
        }
    }
}
