package com.solace.wrapper.connection;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.AuthenticationStrategy;
import com.solace.messaging.config.AuthenticationStrategy.ClientCertificateAuthentication;
import com.solace.messaging.config.AuthenticationStrategy.OAuth2;
import com.solace.messaging.config.SolaceProperties;
import com.solace.messaging.config.TransportSecurityStrategy;
import com.solace.messaging.config.TransportSecurityStrategy.TLS;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.wrapper.exception.SolaceConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages Solace messaging service connections using the new Solace Java API.
 */
@Component
public class SolaceConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(SolaceConnectionManager.class);

    private final com.solace.wrapper.config.SolaceProperties wrapperProperties;
    private final ConcurrentHashMap<String, MessagingService> servicePool;
    private final ConcurrentHashMap<String, MessagingService> publisherServicePool;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private volatile MessagingService primaryService;

    // Resiliency / health tracking (aggregated across services)
    private final AtomicInteger reconnectionAttemptCount = new AtomicInteger(0);
    private final AtomicInteger reconnectedCount = new AtomicInteger(0);
    private final AtomicInteger serviceInterruptionCount = new AtomicInteger(0);
    private volatile long lastReconnectionAttemptTs = 0L;
    private volatile long lastReconnectedTs = 0L;
    private volatile long lastServiceInterruptionTs = 0L;
    private volatile String lastServiceInterruptionCause = null;

    public SolaceConnectionManager(com.solace.wrapper.config.SolaceProperties wrapperProperties) {
        this.wrapperProperties = wrapperProperties;
        this.servicePool = new ConcurrentHashMap<>();
        this.publisherServicePool = new ConcurrentHashMap<>();
        ensureReconnectRetriesForIsolation();
        initializePrimaryService();
    }

    /**
     * Initialize the primary messaging service for general use.
     * Protected to allow test subclasses to skip initialization.
     */
    protected void initializePrimaryService() {
        try {
            this.primaryService = createMessagingService();
            attachResiliencyListeners("primary", this.primaryService);
            this.primaryService.connect();
            logger.info("Primary Solace messaging service initialized and connected successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize primary Solace messaging service", e);
            throw new SolaceConnectionException("Failed to initialize primary messaging service", e);
        }
    }

    /**
     * Creates a new MessagingService with configured properties.
     *
     * Notes on API compatibility:
     * - This component targets Solace PubSub+ Java API version >= 1.8.x.
     * - Optional transport properties like CONNECT_TIMEOUT_IN_MS, READ_TIMEOUT_IN_MS,
     *   and KEEP_ALIVE_INTERVAL_IN_MS are applied when available in the runtime library.
     *   If a particular version ignores them, behavior remains functional, and we log at DEBUG.
     */
    public MessagingService createMessagingService() {
        return createMessagingService(null);
    }

    public MessagingService createMessagingService(String clientNameOverride) {
        if (isShutdown.get()) {
            throw new SolaceConnectionException("Connection manager is shutdown");
        }

        try {
            // Create service configuration properties using only valid properties
            Properties serviceProps = new Properties();
            
            // Basic connection properties - these are the core required properties
            if (wrapperProperties.getHost() == null || wrapperProperties.getHost().trim().isEmpty()) {
                throw new SolaceConnectionException("Missing required 'solace.host' configuration");
            }
            serviceProps.setProperty(SolaceProperties.TransportLayerProperties.HOST, wrapperProperties.getHost());
            serviceProps.setProperty(SolaceProperties.ServiceProperties.VPN_NAME, wrapperProperties.getMsgVpn());
            serviceProps.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_USER_NAME, wrapperProperties.getClientUsername());
            serviceProps.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_PASSWORD, wrapperProperties.getClientPassword());
            
            // Client properties (optional)
            String effectiveClientName = resolveClientName(clientNameOverride);
            if (effectiveClientName != null && !effectiveClientName.isEmpty()) {
                serviceProps.setProperty(SolaceProperties.ClientProperties.NAME, effectiveClientName);
            }
            if (wrapperProperties.getApplicationDescription() != null && !wrapperProperties.getApplicationDescription().isEmpty()) {
                serviceProps.setProperty(SolaceProperties.ClientProperties.APPLICATION_DESCRIPTION, wrapperProperties.getApplicationDescription());
            }

            // Apply optional timeouts/keepalive if supported by API.
            // These keys were validated against Solace Java API around 1.8.x; older/newer
            // versions may ignore them. We log at debug if application fails.
            try {
                // These keys may vary across API versions; apply conservatively
                String connectMs = String.valueOf(wrapperProperties.getConnectTimeoutInMillis());
                String keepAliveMs = String.valueOf(wrapperProperties.getKeepAliveIntervalInMillis());

                serviceProps.setProperty(com.solace.messaging.config.SolaceProperties.TransportLayerProperties.CONNECTION_ATTEMPTS_TIMEOUT, connectMs);
                serviceProps.setProperty(com.solace.messaging.config.SolaceProperties.TransportLayerProperties.KEEP_ALIVE_INTERVAL, keepAliveMs);

                logger.debug("Applied optional transport properties: connectionAttemptsTimeoutMs={}, keepAliveIntervalMs={}",
                        connectMs, keepAliveMs);
            } catch (Exception ex) {
                logger.debug("Optional timeout/keepalive properties could not be fully applied: {}", ex.getMessage());
            }
            
            // Connection and reconnection settings (configurable)
            if (wrapperProperties.isReconnectRetries()) {
                try {
                    if (wrapperProperties.getConnectionRetries() != null) {
                        serviceProps.setProperty(SolaceProperties.TransportLayerProperties.CONNECTION_RETRIES,
                                String.valueOf(wrapperProperties.getConnectionRetries()));
                    }
                    if (wrapperProperties.getConnectionRetriesPerHost() != null) {
                        serviceProps.setProperty(SolaceProperties.TransportLayerProperties.CONNECTION_RETRIES_PER_HOST,
                                String.valueOf(wrapperProperties.getConnectionRetriesPerHost()));
                    }
                    if (wrapperProperties.getReconnectionAttempts() != null) {
                        serviceProps.setProperty(SolaceProperties.TransportLayerProperties.RECONNECTION_ATTEMPTS,
                                String.valueOf(wrapperProperties.getReconnectionAttempts()));
                    }
                    String waitMs = String.valueOf(
                            wrapperProperties.getReconnectionAttemptsWaitIntervalInMillis() != null
                                    ? wrapperProperties.getReconnectionAttemptsWaitIntervalInMillis()
                                    : Math.max(5000, wrapperProperties.getReconnectRetriesWaitInMillis()));
                    serviceProps.setProperty(SolaceProperties.TransportLayerProperties.RECONNECTION_ATTEMPTS_WAIT_INTERVAL, waitMs);

                    logger.debug("Applied reconnection properties: connectionRetries={}, reconnectionAttempts={}, waitIntervalMs={}",
                            wrapperProperties.getConnectionRetries(),
                            wrapperProperties.getReconnectionAttempts(),
                            waitMs);
                } catch (Exception ex) {
                    logger.debug("Optional reconnection properties could not be fully applied: {}", ex.getMessage());
                }
            } else {
                serviceProps.setProperty(SolaceProperties.TransportLayerProperties.CONNECTION_RETRIES, "0");
                serviceProps.setProperty(SolaceProperties.TransportLayerProperties.RECONNECTION_ATTEMPTS, "0");
            }

            // Optional transport tuning
            try {
                if (wrapperProperties.getKeepAliveWithoutResponseLimit() != null) {
                    serviceProps.setProperty(SolaceProperties.TransportLayerProperties.KEEP_ALIVE_WITHOUT_RESPONSE_LIMIT,
                            String.valueOf(wrapperProperties.getKeepAliveWithoutResponseLimit()));
                }
                if (wrapperProperties.getTcpNoDelay() != null) {
                    serviceProps.setProperty(SolaceProperties.TransportLayerProperties.SOCKET_TCP_OPTION_NO_DELAY,
                            String.valueOf(wrapperProperties.getTcpNoDelay()));
                }
            } catch (Exception ex) {
                logger.debug("Optional transport tuning could not be fully applied: {}", ex.getMessage());
            }

            // Service-level helpful defaults
            try {
                serviceProps.setProperty(SolaceProperties.ServiceProperties.RECEIVER_DIRECT_SUBSCRIPTION_REAPPLY,
                        String.valueOf(wrapperProperties.isReapplyDirectSubscriptions()));
                serviceProps.setProperty(SolaceProperties.ServiceProperties.GENERATE_SENDER_ID,
                        String.valueOf(wrapperProperties.isGenerateSenderId()));
                if (wrapperProperties.getGenerateSendTimestamps() != null) {
                    serviceProps.setProperty(SolaceProperties.ServiceProperties.GENERATE_SEND_TIMESTAMPS,
                            String.valueOf(wrapperProperties.getGenerateSendTimestamps()));
                }
                if (wrapperProperties.getGenerateReceiveTimestamps() != null) {
                    serviceProps.setProperty(SolaceProperties.ServiceProperties.GENERATE_RECEIVE_TIMESTAMPS,
                            String.valueOf(wrapperProperties.getGenerateReceiveTimestamps()));
                }
                if (wrapperProperties.getReceiverDirectNoLocal() != null) {
                    serviceProps.setProperty(SolaceProperties.ServiceProperties.RECEIVER_DIRECT_NO_LOCAL_PUBLISHED_MESSAGES,
                            String.valueOf(wrapperProperties.getReceiverDirectNoLocal()));
                }
                if (wrapperProperties.getReceiverPersistentNoLocal() != null) {
                    serviceProps.setProperty(SolaceProperties.ServiceProperties.RECEIVER_PERSISTENT_NO_LOCAL_PUBLISHED_MESSAGES,
                            String.valueOf(wrapperProperties.getReceiverPersistentNoLocal()));
                }
                if (wrapperProperties.getPublisherPersistentAckTimeoutInMs() != null) {
                    serviceProps.setProperty(SolaceProperties.ServiceProperties.PUBLISHER_PERSISTENT_ACK_TIMEOUT,
                            String.valueOf(wrapperProperties.getPublisherPersistentAckTimeoutInMs()));
                }
                if (wrapperProperties.getPublisherPersistentAckWindowSize() != null) {
                    serviceProps.setProperty(SolaceProperties.ServiceProperties.PUBLISHER_PERSISTENT_ACK_WINDOW_SIZE,
                            String.valueOf(wrapperProperties.getPublisherPersistentAckWindowSize()));
                }
            } catch (Exception ex) {
                logger.debug("Optional service properties could not be fully applied: {}", ex.getMessage());
            }

            // Build messaging service with optional TLS/auth strategies
            final var builderInit = MessagingService.builder(ConfigurationProfile.V1)
                    .fromProperties(serviceProps);
            var builder = builderInit;

            // Configure TLS truststore if provided
            try {
                TransportSecurityStrategy transportSecurity = buildTlsStrategyIfConfigured();
                if (transportSecurity != null) {
                    builder = builder.withTransportSecurityStrategy(transportSecurity);
                }
            } catch (Throwable t) {
                logger.warn("TLS configuration not fully applied: {}", t.toString());
            }

            // Configure authentication strategy precedence: OAuth2 > ClientCert > Basic(default)
            try {
                AuthenticationStrategy auth = buildAuthStrategyIfConfigured();
                if (auth != null) {
                    builder = builder.withAuthenticationStrategy(auth);
                }
            } catch (Throwable t) {
                logger.warn("Auth strategy not fully applied: {}", t.toString());
            }

            MessagingService messagingService = builder.build();

            logger.info("Created new Solace messaging service with host: {}", wrapperProperties.getHost());
            return messagingService;
            
        } catch (Exception e) {
            logger.error("Failed to create Solace messaging service", e);
            throw new SolaceConnectionException("Failed to create messaging service", e);
        }
    }

    private TransportSecurityStrategy buildTlsStrategyIfConfigured() {
        try {
            String trustPath = wrapperProperties.getTlsTrustStorePath();
            String trustPass = wrapperProperties.getTlsTrustStorePassword();
            if ((trustPath != null && !trustPath.isEmpty()) || (trustPass != null && !trustPass.isEmpty())) {
                boolean ignoreExp = wrapperProperties.isTlsIgnoreCertificateExpiration();
                String fmt = wrapperProperties.getTlsTrustStoreFormat();
                com.solace.messaging.util.SecureStoreFormat format = parseStoreFormat(fmt);
                TLS tls = TLS.create();
                if (trustPath != null && !trustPath.isEmpty()) {
                    if (trustPass != null && !trustPass.isEmpty()) {
                        return tls.withCertificateValidation(trustPass, ignoreExp, format, trustPath);
                    } else {
                        // path without password is unusual; proceed without validation customization
                        return tls;
                    }
                } else {
                    // password only
                    return tls.withCertificateValidation(trustPass, ignoreExp);
                }
            }
        } catch (Throwable t) {
            logger.debug("Could not apply TLS strategy: {}", t.toString());
        }
        return null;
    }

    private AuthenticationStrategy buildAuthStrategyIfConfigured() {
        // OAuth2
        try {
            String accessToken = wrapperProperties.getOauth2AccessToken();
            if (accessToken != null && !accessToken.isEmpty()) {
                String issuer = wrapperProperties.getOauth2IssuerIdentifier();
                if (issuer != null && !issuer.isEmpty()) {
                    return OAuth2.of(accessToken).withIssuerIdentifier(issuer);
                }
                return OAuth2.of(accessToken);
            }
        } catch (Throwable t) {
            logger.debug("OAuth2 strategy not applied: {}", t.toString());
        }

        // Client certificate authentication (mTLS)
        try {
            String ksPath = wrapperProperties.getClientCertKeystorePath();
            String ksPass = wrapperProperties.getClientCertKeystorePassword();
            if (ksPath != null && !ksPath.isEmpty() && ksPass != null && !ksPass.isEmpty()) {
                String fmt = wrapperProperties.getClientCertKeystoreFormat();
                ClientCertificateAuthentication cca;
                if (fmt != null && !fmt.isEmpty()) {
                    cca = ClientCertificateAuthentication.of(ksPath, ksPass, parseStoreFormat(fmt));
                } else {
                    cca = ClientCertificateAuthentication.of(ksPath, ksPass);
                }
                String pkPass = wrapperProperties.getClientCertPrivateKeyPassword();
                if (pkPass != null && !pkPass.isEmpty()) {
                    cca = cca.withPrivateKeyPassword(pkPass);
                }
                return cca;
            }
        } catch (Throwable t) {
            logger.debug("Client certificate auth not applied: {}", t.toString());
        }

        return null; // fall back to basic via properties
    }

    private com.solace.messaging.util.SecureStoreFormat parseStoreFormat(String fmt) {
        if (fmt == null) return com.solace.messaging.util.SecureStoreFormat.JKS;
        String f = fmt.trim().toUpperCase();
        try {
            if ("PKCS12".equals(f) || "PKCS12".equalsIgnoreCase(f)) {
                return com.solace.messaging.util.SecureStoreFormat.PKCS12;
            }
        } catch (Exception e) {
            logger.debug("Error parsing store format '{}': {}", fmt, e.getMessage());
        }
        return com.solace.messaging.util.SecureStoreFormat.JKS;
    }

    /**
     * Gets the primary messaging service for general operations.
     */
    public MessagingService getPrimaryService() {
        if (isShutdown.get()) {
            throw new SolaceConnectionException("Connection manager is shutdown");
        }
        
        if (primaryService == null || !primaryService.isConnected()) {
            synchronized (this) {
                if (primaryService == null || !primaryService.isConnected()) {
                    if (primaryService != null) {
                        try {
                            primaryService.disconnect();
                        } catch (Exception e) {
                            logger.warn("Error disconnecting old service", e);
                        }
                    }
                    initializePrimaryService();
                }
            }
        }
        
        return primaryService;
    }

    /**
     * Creates or reuses a messaging service for a specific consumer depending on configuration.
     * If {@code solace.isolate-consumers=true}, creates a dedicated service per consumer ID
     * (using the same ConfigurationProfile and connection properties). Otherwise reuses the primary.
     */
    public MessagingService createConsumerService(String consumerId) {
        return createConsumerService(consumerId, null);
    }

    /**
     * Creates or reuses a messaging service for a specific consumer depending on configuration.
     * If {@code solace.isolate-consumers=true}, creates a dedicated service per consumer ID.
     * If a {@code clientNameOverride} is provided, a dedicated service is created for that consumer.
     */
    public MessagingService createConsumerService(String consumerId, String clientNameOverride) {
        if (isShutdown.get()) {
            throw new SolaceConnectionException("Connection manager is shutdown");
        }

        if ((clientNameOverride == null || clientNameOverride.isEmpty())
                && (!wrapperProperties.isReconnectRetries() || !isIsolateConsumersEnabled())) {
            // Reuse primary service for stability - avoid connection proliferation
            MessagingService service = getPrimaryService();
            logger.debug("Reusing primary service for consumer: {}", consumerId);
            return service;
        }

        // Isolation enabled: create or return cached per-consumer service
        // Check pool size limit before creating new service
        int maxConsumers = wrapperProperties.getMaxConsumerConnections();
        if (maxConsumers > 0 && !servicePool.containsKey(consumerId) && servicePool.size() >= maxConsumers) {
            throw new SolaceConnectionException(
                    "Maximum consumer connections reached (" + maxConsumers + "). " +
                    "Increase solace.max-consumer-connections or disable consumer isolation.");
        }

        return servicePool.computeIfAbsent(consumerId, id -> {
            try {
                MessagingService svc = createMessagingService(clientNameOverride);
                attachResiliencyListeners("consumer-" + id, svc);
                svc.connect();
                logger.info("Created isolated messaging service for consumer: {}", id);
                return svc;
            } catch (Exception e) {
                logger.error("Failed to create isolated consumer service for {}", id, e);
                throw new SolaceConnectionException("Failed to create isolated consumer service for " + id, e);
            }
        });
    }

    /**
     * Creates or reuses a messaging service for a specific publisher depending on configuration.
     * If {@code solace.isolate-publishers=true}, creates a dedicated service per publisher ID.
     * If a {@code clientNameOverride} is provided, a dedicated service is created for that publisher.
     */
    public MessagingService createPublisherService(String publisherId, String clientNameOverride) {
        if (isShutdown.get()) {
            throw new SolaceConnectionException("Connection manager is shutdown");
        }

        if ((clientNameOverride == null || clientNameOverride.isEmpty())
                && (!wrapperProperties.isReconnectRetries() || !wrapperProperties.isIsolatePublishers())) {
            MessagingService service = getPrimaryService();
            logger.debug("Reusing primary service for publisher: {}", publisherId);
            return service;
        }

        // Check pool size limit before creating new service
        int maxPublishers = wrapperProperties.getMaxPublisherConnections();
        if (maxPublishers > 0 && !publisherServicePool.containsKey(publisherId) && publisherServicePool.size() >= maxPublishers) {
            throw new SolaceConnectionException(
                    "Maximum publisher connections reached (" + maxPublishers + "). " +
                    "Increase solace.max-publisher-connections or disable publisher isolation.");
        }

        return publisherServicePool.computeIfAbsent(publisherId, id -> {
            try {
                MessagingService svc = createMessagingService(clientNameOverride);
                attachResiliencyListeners("publisher-" + id, svc);
                svc.connect();
                logger.info("Created isolated messaging service for publisher: {}", id);
                return svc;
            } catch (Exception e) {
                logger.error("Failed to create isolated publisher service for {}", id, e);
                throw new SolaceConnectionException("Failed to create isolated publisher service for " + id, e);
            }
        });
    }

    /**
     * Attach resiliency listeners to a MessagingService to log events and update health stats.
     */
    private void attachResiliencyListeners(String serviceId, MessagingService service) {
        try {
            service.addReconnectionAttemptListener(event -> {
                int n = reconnectionAttemptCount.incrementAndGet();
                lastReconnectionAttemptTs = System.currentTimeMillis();
                logger.info("[{}] Reconnection attempt #{}: {}", serviceId, n, event);
            });
        } catch (Throwable t) {
            logger.debug("Failed to add ReconnectionAttemptListener (possibly unsupported API version): {}", t.getMessage());
        }

        try {
            service.addReconnectionListener(event -> {
                int n = reconnectedCount.incrementAndGet();
                lastReconnectedTs = System.currentTimeMillis();
                logger.info("[{}] Reconnected #{}: {}", serviceId, n, event);
            });
        } catch (Throwable t) {
            logger.debug("Failed to add ReconnectionListener (possibly unsupported API version): {}", t.getMessage());
        }

        try {
            service.addServiceInterruptionListener(event -> {
                int n = serviceInterruptionCount.incrementAndGet();
                lastServiceInterruptionTs = System.currentTimeMillis();
                lastServiceInterruptionCause = event != null ? String.valueOf(event.getCause()) : null;
                logger.warn("[{}] Non-recoverable service interruption #{}: cause={}, event={}",
                        serviceId, n, lastServiceInterruptionCause, event);
            });
        } catch (Throwable t) {
            logger.debug("Failed to add ServiceInterruptionListener (possibly unsupported API version): {}", t.getMessage());
        }
    }

    private boolean isIsolateConsumersEnabled() {
        return wrapperProperties.isIsolateConsumers();
    }

    /**
     * Removes and disconnects a consumer messaging service.
     */
    public void removeConsumerService(String consumerId) {
        MessagingService service = servicePool.remove(consumerId);
        if (service != null && service.isConnected()) {
            try {
                service.disconnect();
                logger.info("Removed consumer messaging service for: {}", consumerId);
            } catch (Exception e) {
                logger.warn("Error disconnecting consumer service for: {}", consumerId, e);
            }
        }
    }

    public void removePublisherService(String publisherId) {
        MessagingService service = publisherServicePool.remove(publisherId);
        if (service != null && service.isConnected()) {
            try {
                service.disconnect();
                logger.info("Removed publisher messaging service for: {}", publisherId);
            } catch (Exception e) {
                logger.warn("Error disconnecting publisher service for: {}", publisherId, e);
            }
        }
    }

    /**
     * Checks if the primary service is connected.
     */
    public boolean isConnected() {
        return primaryService != null && primaryService.isConnected() && !isShutdown.get();
    }

    /**
     * Reconnects the primary service if it's disconnected.
     */
    public void reconnect() {
        if (!isConnected()) {
            logger.info("Attempting to reconnect primary messaging service...");
            initializePrimaryService();
        }
    }

    /**
     * Gets connection statistics for monitoring.
     */
    public ConnectionInfo getConnectionInfo() {
        if (primaryService != null) {
            return new ConnectionInfo(
                wrapperProperties.getHost(),
                wrapperProperties.getMsgVpn(),
                wrapperProperties.getClientName(),
                primaryService.isConnected(),
                servicePool.size()
            );
        }
        return new ConnectionInfo(
            wrapperProperties.getHost(),
            wrapperProperties.getMsgVpn(),
            wrapperProperties.getClientName(),
            false,
            0
        );
    }

    /**
     * Gets service info for monitoring.
     */
    public String getServiceInfo() {
        ConnectionInfo info = getConnectionInfo();
        return String.format("Host: %s, VPN: %s, Client: %s, Connected: %s, Consumer Services: %d", 
                           info.getHost(), 
                           info.getVpnName(),
                           info.getClientName(),
                           info.isConnected(),
                           info.getConsumerServiceCount());
    }

    /**
     * Gets detailed service statistics.
     */
    public ServiceStats getServiceStats() {
        return new ServiceStats(
                isConnected(),
                servicePool.size(),
                (int) servicePool.values().stream().filter(MessagingService::isConnected).count(),
                wrapperProperties.getHost(),
                wrapperProperties.getMsgVpn(),
                reconnectionAttemptCount.get(),
                reconnectedCount.get(),
                serviceInterruptionCount.get(),
                lastReconnectionAttemptTs,
                lastReconnectedTs,
                lastServiceInterruptionTs,
                lastServiceInterruptionCause
        );
    }

    /**
     * Creates a messaging service with custom properties.
     * Useful for testing or special configurations.
     */
    public MessagingService createCustomMessagingService(Properties customProperties) {
        try {
            // Start with our base properties
            Properties serviceProps = new Properties();
            serviceProps.setProperty(SolaceProperties.TransportLayerProperties.HOST, wrapperProperties.getHost());
            serviceProps.setProperty(SolaceProperties.ServiceProperties.VPN_NAME, wrapperProperties.getMsgVpn());
            serviceProps.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_USER_NAME, wrapperProperties.getClientUsername());
            serviceProps.setProperty(SolaceProperties.AuthenticationProperties.SCHEME_BASIC_PASSWORD, wrapperProperties.getClientPassword());
            serviceProps.setProperty(SolaceProperties.ClientProperties.NAME, wrapperProperties.getClientName());
            
            // Add custom properties (they will override defaults if same keys)
            serviceProps.putAll(customProperties);
            
            MessagingService messagingService = MessagingService.builder(ConfigurationProfile.V1)
                    .fromProperties(serviceProps)
                    .build();
                    
            logger.info("Created custom Solace messaging service");
            return messagingService;
            
        } catch (Exception e) {
            logger.error("Failed to create custom Solace messaging service", e);
            throw new SolaceConnectionException("Failed to create custom messaging service", e);
        }
    }

    private void ensureReconnectRetriesForIsolation() {
        if ((wrapperProperties.isIsolateConsumers() || wrapperProperties.isIsolatePublishers())
                && !wrapperProperties.isReconnectRetries()) {
            wrapperProperties.setReconnectRetries(true);
            logger.warn("reconnectRetries forced to true because isolateConsumers/isolatePublishers is enabled");
        }
    }

    private String resolveClientName(String clientNameOverride) {
        if (clientNameOverride != null && !clientNameOverride.trim().isEmpty()) {
            return clientNameOverride.trim();
        }
        String base = wrapperProperties.getClientName();
        if (base == null || base.trim().isEmpty()) {
            return null;
        }
        return base.trim();
    }

    /**
     * Cleanup method called when the bean is destroyed.
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Solace connection manager...");
        isShutdown.set(true);

        // Disconnect all consumer services - snapshot first to avoid ConcurrentModificationException
        List<MessagingService> consumerServices = new ArrayList<>(servicePool.values());
        for (MessagingService service : consumerServices) {
            if (service != null && service.isConnected()) {
                try {
                    service.disconnect();
                } catch (Exception e) {
                    logger.warn("Error disconnecting consumer service", e);
                }
            }
        }
        servicePool.clear();

        // Disconnect all publisher services - snapshot first to avoid ConcurrentModificationException
        List<MessagingService> publisherServices = new ArrayList<>(publisherServicePool.values());
        for (MessagingService service : publisherServices) {
            if (service != null && service.isConnected()) {
                try {
                    service.disconnect();
                } catch (Exception e) {
                    logger.warn("Error disconnecting publisher service", e);
                }
            }
        }
        publisherServicePool.clear();

        // Disconnect primary service
        if (primaryService != null && primaryService.isConnected()) {
            try {
                primaryService.disconnect();
            } catch (Exception e) {
                logger.warn("Error disconnecting primary service", e);
            }
        }

        logger.info("Solace connection manager shutdown complete");
    }

    /**
     * Gets the current Solace properties.
     */
    public com.solace.wrapper.config.SolaceProperties getProperties() {
        return wrapperProperties;
    }

    /**
     * Connection information class for monitoring.
     */
    public static class ConnectionInfo {
        private final String host;
        private final String vpnName;
        private final String clientName;
        private final boolean connected;
        private final int consumerServiceCount;

        public ConnectionInfo(String host, String vpnName, String clientName, 
                            boolean connected, int consumerServiceCount) {
            this.host = host;
            this.vpnName = vpnName;
            this.clientName = clientName;
            this.connected = connected;
            this.consumerServiceCount = consumerServiceCount;
        }

        public String getHost() { return host; }
        public String getVpnName() { return vpnName; }
        public String getClientName() { return clientName; }
        public boolean isConnected() { return connected; }
        public int getConsumerServiceCount() { return consumerServiceCount; }

        @Override
        public String toString() {
            return String.format("ConnectionInfo{host='%s', vpn='%s', client='%s', connected=%s, consumers=%d}",
                               host, vpnName, clientName, connected, consumerServiceCount);
        }
    }

    /**
     * Service statistics class for monitoring.
     */
    public static class ServiceStats {
        private final boolean primaryConnected;
        private final int totalConsumerServices;
        private final int connectedConsumerServices;
        private final String brokerHost;
        private final String vpnName;
        private final int reconnectionAttemptCount;
        private final int reconnectedCount;
        private final int serviceInterruptionCount;
        private final long lastReconnectionAttemptTs;
        private final long lastReconnectedTs;
        private final long lastServiceInterruptionTs;
        private final String lastServiceInterruptionCause;

        public ServiceStats(boolean primaryConnected, int totalConsumerServices,
                            int connectedConsumerServices, String brokerHost, String vpnName,
                            int reconnectionAttemptCount, int reconnectedCount, int serviceInterruptionCount,
                            long lastReconnectionAttemptTs, long lastReconnectedTs, long lastServiceInterruptionTs,
                            String lastServiceInterruptionCause) {
            this.primaryConnected = primaryConnected;
            this.totalConsumerServices = totalConsumerServices;
            this.connectedConsumerServices = connectedConsumerServices;
            this.brokerHost = brokerHost;
            this.vpnName = vpnName;
            this.reconnectionAttemptCount = reconnectionAttemptCount;
            this.reconnectedCount = reconnectedCount;
            this.serviceInterruptionCount = serviceInterruptionCount;
            this.lastReconnectionAttemptTs = lastReconnectionAttemptTs;
            this.lastReconnectedTs = lastReconnectedTs;
            this.lastServiceInterruptionTs = lastServiceInterruptionTs;
            this.lastServiceInterruptionCause = lastServiceInterruptionCause;
        }

        public boolean isPrimaryConnected() { return primaryConnected; }
        public int getTotalConsumerServices() { return totalConsumerServices; }
        public int getConnectedConsumerServices() { return connectedConsumerServices; }
        public String getBrokerHost() { return brokerHost; }
        public String getVpnName() { return vpnName; }
        public int getReconnectionAttemptCount() { return reconnectionAttemptCount; }
        public int getReconnectedCount() { return reconnectedCount; }
        public int getServiceInterruptionCount() { return serviceInterruptionCount; }
        public long getLastReconnectionAttemptTs() { return lastReconnectionAttemptTs; }
        public long getLastReconnectedTs() { return lastReconnectedTs; }
        public long getLastServiceInterruptionTs() { return lastServiceInterruptionTs; }
        public String getLastServiceInterruptionCause() { return lastServiceInterruptionCause; }

        @Override
        public String toString() {
            return String.format(
                    "ServiceStats{primary=%s, total=%d, connected=%d, host='%s', vpn='%s', reconAttempts=%d, reconnected=%d, interruptions=%d, lastAttemptTs=%d, lastReconnTs=%d, lastInterruptTs=%d}",
                    primaryConnected, totalConsumerServices, connectedConsumerServices,
                    brokerHost, vpnName,
                    reconnectionAttemptCount, reconnectedCount, serviceInterruptionCount,
                    lastReconnectionAttemptTs, lastReconnectedTs, lastServiceInterruptionTs
            );
        }
    }
}
