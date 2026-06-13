package com.solace.wrapper.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Solace request-reply, bound under {@code solace.request-reply}.
 *
 * <pre>
 * solace:
 *   request-reply:
 *     default-timeout-ms: 5000
 * </pre>
 */
@ConfigurationProperties(prefix = "solace.request-reply")
public class SolaceRequestReplyProperties {

    /** Default reply timeout (ms) applied when a request is made without an explicit timeout. */
    private long defaultTimeoutMs = 5000;

    public long getDefaultTimeoutMs() {
        return defaultTimeoutMs;
    }

    public void setDefaultTimeoutMs(long defaultTimeoutMs) {
        this.defaultTimeoutMs = defaultTimeoutMs;
    }
}
