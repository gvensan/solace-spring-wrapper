package com.solace.wrapper.broker;

import org.junit.jupiter.api.Assumptions;

import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

/**
 * Loads broker test settings from test-broker.properties, system properties, or environment variables.
 * This keeps real broker credentials out of source and allows opt-in runs only.
 */
public class BrokerTestSettings {
    public final String host;
    public final String msgVpn;
    public final String username;
    public final String password;
    public final String topicBase;
    public final String queueBase;
    public final int timeoutSeconds;
    public final boolean allowQueueCreate;

    private BrokerTestSettings(String host, String msgVpn, String username, String password,
                               String topicBase, String queueBase,
                               int timeoutSeconds, boolean allowQueueCreate) {
        this.host = host;
        this.msgVpn = msgVpn;
        this.username = username;
        this.password = password;
        this.topicBase = topicBase;
        this.queueBase = queueBase;
        this.timeoutSeconds = timeoutSeconds;
        this.allowQueueCreate = allowQueueCreate;
    }

    public static BrokerTestSettings load() {
        Properties p = new Properties();
        try (InputStream in = BrokerTestSettings.class.getResourceAsStream("/test-broker.properties")) {
            if (in != null) {
                p.load(in);
            }
        } catch (Exception ignore) { }

        String host = firstNonEmpty(
                System.getProperty("solace.host"),
                getenv("SOLACE_HOST"),
                p.getProperty("solace.host")
        );
        String msgVpn = firstNonEmpty(
                System.getProperty("solace.msgVpn"),
                getenv("SOLACE_MSGVPN"),
                p.getProperty("solace.msgVpn", "default")
        );
        String username = firstNonEmpty(
                System.getProperty("solace.clientUsername"),
                getenv("SOLACE_CLIENTUSERNAME"),
                p.getProperty("solace.clientUsername")
        );
        String password = firstNonEmpty(
                System.getProperty("solace.clientPassword"),
                getenv("SOLACE_CLIENTPASSWORD"),
                p.getProperty("solace.clientPassword")
        );
        String topicBase = firstNonEmpty(
                System.getProperty("topic.base"),
                getenv("TOPIC_BASE"),
                p.getProperty("topic.base", "broker/it")
        );
        String queueBase = firstNonEmpty(
                System.getProperty("queue.base"),
                getenv("QUEUE_BASE"),
                p.getProperty("queue.base", "broker/it/queue")
        );
        int timeout = parseInt(
                firstNonEmpty(System.getProperty("timeout.seconds"), getenv("TIMEOUT_SECONDS"),
                        p.getProperty("timeout.seconds", "10")),
                10
        );
        boolean allowQueueCreate = parseBoolean(
                firstNonEmpty(System.getProperty("allowQueueCreate"), getenv("ALLOWQUEUECREATE"),
                        p.getProperty("allowQueueCreate", "true")),
                true
        );

        return new BrokerTestSettings(host, msgVpn, username, password, topicBase, queueBase, timeout, allowQueueCreate);
    }

    public boolean isConfigured() {
        return notBlank(host) && notBlank(msgVpn) && notBlank(username) && password != null;
    }

    public void assumeConfigured() {
        Assumptions.assumeTrue(isConfigured(),
                "Broker tests skipped: provide solace.host/msgVpn/clientUsername/clientPassword via properties or test-broker.properties");
    }

    public String uniqueTopic(String suffix) {
        return topicBase + "/" + suffix + "/" + UUID.randomUUID();
    }

    public String uniqueQueue(String suffix) {
        return queueBase + "-" + suffix + "-" + UUID.randomUUID();
    }

    private static String firstNonEmpty(String... vals) {
        for (String v : vals) {
            if (notBlank(v)) return v.trim();
        }
        return null;
    }

    private static boolean notBlank(String v) {
        return v != null && !v.trim().isEmpty();
    }

    private static String getenv(String key) {
        if (key == null) return null;
        return System.getenv(key);
    }

    private static int parseInt(String s, int def) {
        try { return Integer.parseInt(s); } catch (Exception e) { return def; }
    }

    private static boolean parseBoolean(String s, boolean def) {
        if (s == null) return def;
        return "true".equalsIgnoreCase(s) || "yes".equalsIgnoreCase(s) || "1".equals(s.trim());
    }
}
