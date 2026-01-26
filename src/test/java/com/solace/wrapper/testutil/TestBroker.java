package com.solace.wrapper.testutil;

import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;

public final class TestBroker {
    private TestBroker() {}

    public static void assumeAvailable() {
        BrokerConfig cfg = loadConfig();
        boolean ok = isReachable(cfg.host, cfg.port, cfg.timeoutMs);
        Assumptions.assumeTrue(ok, String.format(
                "Solace broker not reachable at %s:%d (from test-broker.properties)", cfg.host, cfg.port));
    }

    static class BrokerConfig {
        String host = "localhost";
        int port = 55555; // default SMF port
        int timeoutMs = 500;
    }

    private static BrokerConfig loadConfig() {
        BrokerConfig cfg = new BrokerConfig();
        Properties p = new Properties();
        try (InputStream in = TestBroker.class.getResourceAsStream("/test-broker.properties")) {
            if (in != null) {
                p.load(in);
                // Prefer solace.host if present, and parse host/port from URL
                String solaceHost = p.getProperty("solace.host");
                if (solaceHost != null && !solaceHost.isBlank()) {
                    try {
                        java.net.URI uri = java.net.URI.create(solaceHost.trim());
                        if (uri.getHost() != null) cfg.host = uri.getHost();
                        if (uri.getPort() > 0) cfg.port = uri.getPort();
                        // If no port provided, keep default 55555
                    } catch (Exception ignore) {
                        // Fallback below
                    }
                }
                // Back-compat: allow direct host/port keys if present
                String host = p.getProperty("host");
                if (host != null && !host.isBlank()) cfg.host = host.trim();
                String portStr = p.getProperty("port");
                if (portStr != null && !portStr.isBlank()) {
                    try { cfg.port = Integer.parseInt(portStr.trim()); } catch (NumberFormatException ignore) {}
                }
                // Timeout (new key), fallback to legacy key
                String toStr = p.getProperty("testbroker.timeoutMs", p.getProperty("timeoutMs"));
                if (toStr != null && !toStr.isBlank()) {
                    try { cfg.timeoutMs = Integer.parseInt(toStr.trim()); } catch (NumberFormatException ignore) {}
                }
            }
        } catch (IOException ignore) {
        }
        return cfg;
    }

    private static boolean isReachable(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
