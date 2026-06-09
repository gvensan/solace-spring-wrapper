package com.solace.wrapper.metrics;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties controlling Solace wrapper metrics emission.
 *
 * <p>Bound under the {@code solace.metrics} prefix, e.g.:</p>
 * <pre>
 * solace:
 *   metrics:
 *     enabled: true
 *     include-destination-tag: true
 * </pre>
 */
@ConfigurationProperties(prefix = "solace.metrics")
public class SolaceMetricsProperties {

    /** Whether Solace metrics are emitted. Defaults to {@code true}. */
    private boolean enabled = true;

    /**
     * Whether to include the destination (topic/queue) as a meter tag. Useful for per-destination
     * dashboards, but can create high-cardinality series when many distinct destinations are used.
     * Defaults to {@code true}.
     */
    private boolean includeDestinationTag = true;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isIncludeDestinationTag() {
        return includeDestinationTag;
    }

    public void setIncludeDestinationTag(boolean includeDestinationTag) {
        this.includeDestinationTag = includeDestinationTag;
    }
}
