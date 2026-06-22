package com.solace.wrapper.metrics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link SolaceMetricsProperties} defaults and setters.
 */
class SolaceMetricsPropertiesTest {

    @Test
    void defaults() {
        SolaceMetricsProperties p = new SolaceMetricsProperties();
        assertThat(p.isEnabled()).isTrue();
        assertThat(p.isIncludeDestinationTag()).isTrue();
    }

    @Test
    void setters_round_trip() {
        SolaceMetricsProperties p = new SolaceMetricsProperties();
        p.setEnabled(false);
        p.setIncludeDestinationTag(false);
        assertThat(p.isEnabled()).isFalse();
        assertThat(p.isIncludeDestinationTag()).isFalse();
    }
}
