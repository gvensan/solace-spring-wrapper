package com.solace.wrapper.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link SolaceMetricsAutoConfiguration} contract: a {@link SolaceMetrics} bean is
 * created with a fallback registry when none exists, an existing {@link MeterRegistry} is reused,
 * and the whole thing can be disabled via {@code solace.metrics.enabled=false}.
 */
class SolaceMetricsAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SolaceMetricsAutoConfiguration.class));

    @Test
    void providesSolaceMetricsAndFallbackRegistryByDefault() {
        runner.run(ctx -> {
            assertThat(ctx).hasSingleBean(SolaceMetrics.class);
            assertThat(ctx).hasSingleBean(MeterRegistry.class);
            assertThat(ctx.getBean(SolaceMetrics.class).isEnabled()).isTrue();
            assertThat(ctx.getBean(MeterRegistry.class)).isInstanceOf(SimpleMeterRegistry.class);
        });
    }

    @Test
    void reusesExistingMeterRegistry() {
        runner.withUserConfiguration(ExistingRegistryConfig.class).run(ctx -> {
            assertThat(ctx).hasSingleBean(MeterRegistry.class);
            assertThat(ctx.getBean(MeterRegistry.class)).isSameAs(ExistingRegistryConfig.REGISTRY);
            assertThat(ctx.getBean(SolaceMetrics.class).getRegistry()).isSameAs(ExistingRegistryConfig.REGISTRY);
        });
    }

    @Test
    void disabledWhenPropertyFalse() {
        runner.withPropertyValues("solace.metrics.enabled=false").run(ctx ->
                assertThat(ctx).doesNotHaveBean(SolaceMetrics.class));
    }

    @Test
    void honorsIncludeDestinationTagProperty() {
        runner.withPropertyValues("solace.metrics.include-destination-tag=false").run(ctx -> {
            SolaceMetrics metrics = ctx.getBean(SolaceMetrics.class);
            metrics.recordConsume(true, "q.a", "c-1");
            MeterRegistry registry = ctx.getBean(MeterRegistry.class);
            long destTags = registry.find(SolaceMetrics.CONSUME_COUNTER).counter()
                    .getId().getTags().stream().filter(t -> t.getKey().equals("destination")).count();
            assertThat(destTags).isZero();
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class ExistingRegistryConfig {
        static final MeterRegistry REGISTRY = new SimpleMeterRegistry();

        @Bean
        MeterRegistry meterRegistry() {
            return REGISTRY;
        }
    }
}
