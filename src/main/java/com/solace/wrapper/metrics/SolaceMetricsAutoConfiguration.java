package com.solace.wrapper.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for Solace wrapper metrics.
 *
 * <p>Only activates when Micrometer is on the classpath (it always is, via
 * {@code spring-boot-starter-actuator}) and {@code solace.metrics.enabled} is not set to
 * {@code false}. A {@link MeterRegistry} is provided only if the application context does not
 * already contain one — under Spring Boot Actuator a registry is normally auto-configured, so the
 * fallback {@link SimpleMeterRegistry} here mainly serves non-Boot or sliced test contexts.</p>
 */
@Configuration
@ConditionalOnClass(MeterRegistry.class)
@ConditionalOnProperty(prefix = "solace.metrics", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(SolaceMetricsProperties.class)
public class SolaceMetricsAutoConfiguration {

    /**
     * Provides a fallback in-memory registry when none is present (e.g. plain Spring contexts).
     * Spring Boot Actuator supplies its own {@code MeterRegistry}, so this is skipped there.
     */
    @Bean
    @ConditionalOnMissingBean(MeterRegistry.class)
    public MeterRegistry solaceSimpleMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceMetrics solaceMetrics(MeterRegistry meterRegistry, SolaceMetricsProperties properties) {
        return new SolaceMetrics(meterRegistry, properties.isIncludeDestinationTag());
    }
}
