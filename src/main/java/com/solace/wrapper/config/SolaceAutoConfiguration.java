package com.solace.wrapper.config;

import com.solace.wrapper.connection.SolaceConnectionManager;
import com.solace.wrapper.publisher.SolacePublisher;
import com.solace.wrapper.consumer.SolaceConsumerManager;
import com.solace.wrapper.metrics.SolaceMetrics;
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
@EnableConfigurationProperties(SolaceProperties.class)
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
                                                      MessageSerializer messageSerializer) {
        return new SolaceConsumerManager(connectionManager, messageSerializer);
    }
}
