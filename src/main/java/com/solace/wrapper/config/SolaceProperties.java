package com.solace.wrapper.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for Solace connection settings.
 * Configure these under the prefix {@code solace} in your application.yml/properties.
 */
@Component
@ConfigurationProperties(prefix = "solace")
public class SolaceProperties {

    /**
     * Broker host URL. Required (no default). Example: tcp://localhost:55555 or tcps://host:55443
     */
    private String host = "";
    /** Message VPN name on the Solace broker. */
    private String msgVpn = "default";
    /** Client username for authentication. */
    private String clientUsername = "default";
    /** Client password for authentication. */
    private String clientPassword = "";
    /** Client name used when connecting to the broker. */
    private String clientName = "solace-spring-wrapper";
    /** Optional application description propagated to the API. */
    private String applicationDescription;
    /** Whether to enable conservative reconnect attempts. */
    private boolean reconnectRetries = true;
    /** Wait time between reconnect attempts in milliseconds. */
    private int reconnectRetriesWaitInMillis = 3000;
    /** Number of connection retries on initial connect (null = library default). */
    private Integer connectionRetries = 1;
    /** Number of reconnection attempts after disconnect (null = library default). */
    private Integer reconnectionAttempts = 3;
    /** Wait interval between reconnection attempts in ms (null = library default). */
    private Integer reconnectionAttemptsWaitIntervalInMillis = 5000;
    /** TCP connect timeout in ms (applied when supported by the API). */
    private int connectTimeoutInMillis = 30000;
    /** Read timeout in ms (applied when supported by the API). */
    private int readTimeoutInMillis = 30000;
    /** Keep-alive interval in ms (applied when supported by the API). */
    private int keepAliveIntervalInMillis = 3000;

    // Executor sizing for publisher async operations (optional)
    private Integer publisherExecutorCoreSize;
    private Integer publisherExecutorMaxSize;
    private Integer publisherExecutorQueueCapacity;
    // Backpressure strategy for Direct publisher
    public enum BackpressureStrategy { WAIT, REJECT }
    private BackpressureStrategy directPublisherBackpressure = BackpressureStrategy.WAIT;
    private int directPublisherBackpressureWaitMs = 1;
    // Service-level helpful defaults
    /** Reapply Direct subscriptions after reconnect (recommended). */
    private boolean reapplyDirectSubscriptions = true;
    /** Let API generate sender id automatically (recommended). */
    private boolean generateSenderId = true;
    // Optional transport tuning
    private Integer connectionRetriesPerHost; // null = use library default
    private Integer keepAliveWithoutResponseLimit; // null = default (>=3)
    private Boolean tcpNoDelay; // null = default
    // Timestamps emission
    private Boolean generateSendTimestamps; // null = default
    private Boolean generateReceiveTimestamps; // null = default
    // No-local delivery flags
    private Boolean receiverDirectNoLocal; // null = default
    private Boolean receiverPersistentNoLocal; // null = default
    // Publisher persistent ack behaviour
    private Integer publisherPersistentAckTimeoutInMs; // null = default
    private Integer publisherPersistentAckWindowSize; // null = default
    // TLS / Auth enhancements
    // OAuth2
    private String oauth2AccessToken;
    private String oauth2IssuerIdentifier;
    // TLS trust store settings
    private String tlsTrustStorePath; // file path or URL
    private String tlsTrustStorePassword;
    private String tlsTrustStoreFormat; // JKS or PKCS12
    private boolean tlsIgnoreCertificateExpiration = false;
    // Client certificate (keystore) settings for mTLS
    private String clientCertKeystorePath; // file path or URL
    private String clientCertKeystorePassword;
    private String clientCertKeystoreFormat; // JKS or PKCS12
    private String clientCertPrivateKeyPassword; // optional
    /** If true, create isolated MessagingService per consumer (defaults to false). */
    private boolean isolateConsumers = false;
    /** If true, create isolated MessagingService per publisher (defaults to false). */
    private boolean isolatePublishers = false;

    // Resource limits and timeouts
    /** Maximum number of consumer connections (0 = unlimited). */
    private int maxConsumerConnections = 50;
    /** Maximum number of publisher connections (0 = unlimited). */
    private int maxPublisherConnections = 50;
    /** Timeout in milliseconds for terminating receivers/publishers during shutdown. */
    private int terminationTimeoutMs = 5000;
    /** Timeout in milliseconds for pending publish confirmations before cleanup (default 30s). */
    private long pendingConfirmTimeoutMs = 30000;

    // Getters and setters
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public String getMsgVpn() { return msgVpn; }
    public void setMsgVpn(String msgVpn) { this.msgVpn = msgVpn; }

    public String getClientUsername() { return clientUsername; }
    public void setClientUsername(String clientUsername) { this.clientUsername = clientUsername; }

    public String getClientPassword() { return clientPassword; }
    public void setClientPassword(String clientPassword) { this.clientPassword = clientPassword; }

    public String getClientName() { return clientName; }
    public void setClientName(String clientName) { this.clientName = clientName; }
    public String getApplicationDescription() { return applicationDescription; }
    public void setApplicationDescription(String applicationDescription) { this.applicationDescription = applicationDescription; }

    public boolean isReconnectRetries() { return reconnectRetries; }
    public void setReconnectRetries(boolean reconnectRetries) { this.reconnectRetries = reconnectRetries; }

    public int getReconnectRetriesWaitInMillis() { return reconnectRetriesWaitInMillis; }
    public void setReconnectRetriesWaitInMillis(int reconnectRetriesWaitInMillis) { 
        this.reconnectRetriesWaitInMillis = reconnectRetriesWaitInMillis; 
    }

    public int getConnectTimeoutInMillis() { return connectTimeoutInMillis; }
    public void setConnectTimeoutInMillis(int connectTimeoutInMillis) { 
        this.connectTimeoutInMillis = connectTimeoutInMillis; 
    }

    public int getReadTimeoutInMillis() { return readTimeoutInMillis; }
    public void setReadTimeoutInMillis(int readTimeoutInMillis) { 
        this.readTimeoutInMillis = readTimeoutInMillis; 
    }

    public int getKeepAliveIntervalInMillis() { return keepAliveIntervalInMillis; }
    public void setKeepAliveIntervalInMillis(int keepAliveIntervalInMillis) { 
        this.keepAliveIntervalInMillis = keepAliveIntervalInMillis; 
    }

    public Integer getPublisherExecutorCoreSize() { return publisherExecutorCoreSize; }
    public void setPublisherExecutorCoreSize(Integer publisherExecutorCoreSize) { this.publisherExecutorCoreSize = publisherExecutorCoreSize; }

    public Integer getPublisherExecutorMaxSize() { return publisherExecutorMaxSize; }
    public void setPublisherExecutorMaxSize(Integer publisherExecutorMaxSize) { this.publisherExecutorMaxSize = publisherExecutorMaxSize; }

    public Integer getPublisherExecutorQueueCapacity() { return publisherExecutorQueueCapacity; }
    public void setPublisherExecutorQueueCapacity(Integer publisherExecutorQueueCapacity) { this.publisherExecutorQueueCapacity = publisherExecutorQueueCapacity; }

    public BackpressureStrategy getDirectPublisherBackpressure() { return directPublisherBackpressure; }
    public void setDirectPublisherBackpressure(BackpressureStrategy directPublisherBackpressure) { this.directPublisherBackpressure = directPublisherBackpressure; }

    public int getDirectPublisherBackpressureWaitMs() { return directPublisherBackpressureWaitMs; }
    public void setDirectPublisherBackpressureWaitMs(int directPublisherBackpressureWaitMs) { this.directPublisherBackpressureWaitMs = directPublisherBackpressureWaitMs; }
    public boolean isReapplyDirectSubscriptions() { return reapplyDirectSubscriptions; }
    public void setReapplyDirectSubscriptions(boolean reapplyDirectSubscriptions) { this.reapplyDirectSubscriptions = reapplyDirectSubscriptions; }
    public boolean isGenerateSenderId() { return generateSenderId; }
    public void setGenerateSenderId(boolean generateSenderId) { this.generateSenderId = generateSenderId; }
    public Integer getConnectionRetriesPerHost() { return connectionRetriesPerHost; }
    public void setConnectionRetriesPerHost(Integer connectionRetriesPerHost) { this.connectionRetriesPerHost = connectionRetriesPerHost; }
    public Integer getKeepAliveWithoutResponseLimit() { return keepAliveWithoutResponseLimit; }
    public void setKeepAliveWithoutResponseLimit(Integer keepAliveWithoutResponseLimit) { this.keepAliveWithoutResponseLimit = keepAliveWithoutResponseLimit; }
    public Boolean getTcpNoDelay() { return tcpNoDelay; }
    public void setTcpNoDelay(Boolean tcpNoDelay) { this.tcpNoDelay = tcpNoDelay; }
    public Boolean getGenerateSendTimestamps() { return generateSendTimestamps; }
    public void setGenerateSendTimestamps(Boolean generateSendTimestamps) { this.generateSendTimestamps = generateSendTimestamps; }
    public Boolean getGenerateReceiveTimestamps() { return generateReceiveTimestamps; }
    public void setGenerateReceiveTimestamps(Boolean generateReceiveTimestamps) { this.generateReceiveTimestamps = generateReceiveTimestamps; }
    public Boolean getReceiverDirectNoLocal() { return receiverDirectNoLocal; }
    public void setReceiverDirectNoLocal(Boolean receiverDirectNoLocal) { this.receiverDirectNoLocal = receiverDirectNoLocal; }
    public Boolean getReceiverPersistentNoLocal() { return receiverPersistentNoLocal; }
    public void setReceiverPersistentNoLocal(Boolean receiverPersistentNoLocal) { this.receiverPersistentNoLocal = receiverPersistentNoLocal; }
    public Integer getPublisherPersistentAckTimeoutInMs() { return publisherPersistentAckTimeoutInMs; }
    public void setPublisherPersistentAckTimeoutInMs(Integer publisherPersistentAckTimeoutInMs) { this.publisherPersistentAckTimeoutInMs = publisherPersistentAckTimeoutInMs; }
    public Integer getPublisherPersistentAckWindowSize() { return publisherPersistentAckWindowSize; }
    public void setPublisherPersistentAckWindowSize(Integer publisherPersistentAckWindowSize) { this.publisherPersistentAckWindowSize = publisherPersistentAckWindowSize; }

    // OAuth2
    public String getOauth2AccessToken() { return oauth2AccessToken; }
    public void setOauth2AccessToken(String oauth2AccessToken) { this.oauth2AccessToken = oauth2AccessToken; }
    public String getOauth2IssuerIdentifier() { return oauth2IssuerIdentifier; }
    public void setOauth2IssuerIdentifier(String oauth2IssuerIdentifier) { this.oauth2IssuerIdentifier = oauth2IssuerIdentifier; }

    // Kerberos support removed

    // TLS trust store
    public String getTlsTrustStorePath() { return tlsTrustStorePath; }
    public void setTlsTrustStorePath(String tlsTrustStorePath) { this.tlsTrustStorePath = tlsTrustStorePath; }
    public String getTlsTrustStorePassword() { return tlsTrustStorePassword; }
    public void setTlsTrustStorePassword(String tlsTrustStorePassword) { this.tlsTrustStorePassword = tlsTrustStorePassword; }
    public String getTlsTrustStoreFormat() { return tlsTrustStoreFormat; }
    public void setTlsTrustStoreFormat(String tlsTrustStoreFormat) { this.tlsTrustStoreFormat = tlsTrustStoreFormat; }
    public boolean isTlsIgnoreCertificateExpiration() { return tlsIgnoreCertificateExpiration; }
    public void setTlsIgnoreCertificateExpiration(boolean tlsIgnoreCertificateExpiration) { this.tlsIgnoreCertificateExpiration = tlsIgnoreCertificateExpiration; }

    // Client certificate keystore (mTLS)
    public String getClientCertKeystorePath() { return clientCertKeystorePath; }
    public void setClientCertKeystorePath(String clientCertKeystorePath) { this.clientCertKeystorePath = clientCertKeystorePath; }
    public String getClientCertKeystorePassword() { return clientCertKeystorePassword; }
    public void setClientCertKeystorePassword(String clientCertKeystorePassword) { this.clientCertKeystorePassword = clientCertKeystorePassword; }
    public String getClientCertKeystoreFormat() { return clientCertKeystoreFormat; }
    public void setClientCertKeystoreFormat(String clientCertKeystoreFormat) { this.clientCertKeystoreFormat = clientCertKeystoreFormat; }
    public String getClientCertPrivateKeyPassword() { return clientCertPrivateKeyPassword; }
    public void setClientCertPrivateKeyPassword(String clientCertPrivateKeyPassword) { this.clientCertPrivateKeyPassword = clientCertPrivateKeyPassword; }

    public Integer getConnectionRetries() { return connectionRetries; }
    public void setConnectionRetries(Integer connectionRetries) { this.connectionRetries = connectionRetries; }

    public Integer getReconnectionAttempts() { return reconnectionAttempts; }
    public void setReconnectionAttempts(Integer reconnectionAttempts) { this.reconnectionAttempts = reconnectionAttempts; }

    public Integer getReconnectionAttemptsWaitIntervalInMillis() { return reconnectionAttemptsWaitIntervalInMillis; }
    public void setReconnectionAttemptsWaitIntervalInMillis(Integer reconnectionAttemptsWaitIntervalInMillis) {
        this.reconnectionAttemptsWaitIntervalInMillis = reconnectionAttemptsWaitIntervalInMillis;
    }

    public boolean isIsolateConsumers() { return isolateConsumers; }
    public void setIsolateConsumers(boolean isolateConsumers) {
        this.isolateConsumers = isolateConsumers;
        if (isolateConsumers) {
            this.reconnectRetries = true;
        }
    }

    public boolean isIsolatePublishers() { return isolatePublishers; }
    public void setIsolatePublishers(boolean isolatePublishers) {
        this.isolatePublishers = isolatePublishers;
        if (isolatePublishers) {
            this.reconnectRetries = true;
        }
    }

    public int getMaxConsumerConnections() { return maxConsumerConnections; }
    public void setMaxConsumerConnections(int maxConsumerConnections) { this.maxConsumerConnections = maxConsumerConnections; }

    public int getMaxPublisherConnections() { return maxPublisherConnections; }
    public void setMaxPublisherConnections(int maxPublisherConnections) { this.maxPublisherConnections = maxPublisherConnections; }

    public int getTerminationTimeoutMs() { return terminationTimeoutMs; }
    public void setTerminationTimeoutMs(int terminationTimeoutMs) { this.terminationTimeoutMs = terminationTimeoutMs; }

    public long getPendingConfirmTimeoutMs() { return pendingConfirmTimeoutMs; }
    public void setPendingConfirmTimeoutMs(long pendingConfirmTimeoutMs) { this.pendingConfirmTimeoutMs = pendingConfirmTimeoutMs; }
}
