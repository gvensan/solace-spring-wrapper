# TLS/mTLS Setup Guide

This guide covers how to configure TLS (Transport Layer Security) and mTLS (mutual TLS) for secure connections to Solace brokers.

## Overview

The Solace Spring Wrapper supports encrypted connections using:
- **TLS**: Server authentication (client verifies server certificate)
- **mTLS**: Mutual authentication (both client and server verify each other)

## Prerequisites

1. Solace broker configured for TLS
2. SSL/TLS certificates:
   - **Server certificate** (for TLS)
   - **Client certificate + key** (for mTLS)
   - **CA certificate** (trust store)
3. Java KeyStore (JKS) or PKCS12 files

## Basic TLS Configuration

### Step 1: Prepare Trust Store

Create a trust store containing the CA certificate that signed the broker's certificate:

```bash
# Import CA certificate into trust store
keytool -import -alias solace-ca \
  -file ca-certificate.pem \
  -keystore truststore.jks \
  -storepass changeit \
  -noprompt
```

### Step 2: Configure Application

```yaml
solace:
  host: tcps://your-broker.example.com:55443
  msg-vpn: your-vpn
  client-username: your-username
  client-password: your-password

  # TLS Configuration
  tls:
    enabled: true
    trust-store-path: /path/to/truststore.jks
    trust-store-password: changeit
    trust-store-type: JKS

    # Certificate validation (recommended: true for production)
    validate-certificate: true
    validate-certificate-hostname: true
```

### Step 3: Host URL Format

Use `tcps://` protocol for TLS connections:

| Protocol | Port (typical) | Description |
|----------|----------------|-------------|
| `tcp://`  | 55555 | Plain TCP (unencrypted) |
| `tcps://` | 55443 | TLS encrypted |

## mTLS Configuration (Mutual TLS)

For mTLS, you need both trust store (to verify server) and key store (for client identity).

### Step 1: Prepare Key Store

Create a key store with your client certificate and private key:

```bash
# If you have PEM files, convert to PKCS12
openssl pkcs12 -export \
  -in client-certificate.pem \
  -inkey client-private-key.pem \
  -out keystore.p12 \
  -name client-cert \
  -password pass:changeit

# Or import into JKS
keytool -importkeystore \
  -srckeystore keystore.p12 \
  -srcstoretype PKCS12 \
  -srcstorepass changeit \
  -destkeystore keystore.jks \
  -deststoretype JKS \
  -deststorepass changeit
```

### Step 2: Configure Application for mTLS

```yaml
solace:
  host: tcps://your-broker.example.com:55443
  msg-vpn: your-vpn
  # For mTLS, username is typically derived from certificate CN
  client-username: ""
  client-password: ""

  tls:
    enabled: true

    # Trust store (verify server)
    trust-store-path: /path/to/truststore.jks
    trust-store-password: changeit
    trust-store-type: JKS

    # Key store (client identity for mTLS)
    key-store-path: /path/to/keystore.jks
    key-store-password: changeit
    key-store-type: JKS
    private-key-password: changeit

    # Certificate validation
    validate-certificate: true
    validate-certificate-hostname: true
```

## Configuration Reference

### Full TLS Properties

```yaml
solace:
  tls:
    # Enable/disable TLS (auto-detected from tcps:// if not set)
    enabled: true

    # Trust Store Configuration
    trust-store-path: /path/to/truststore.jks
    trust-store-password: changeit
    trust-store-type: JKS          # JKS, PKCS12, or JCEKS

    # Key Store Configuration (mTLS only)
    key-store-path: /path/to/keystore.jks
    key-store-password: changeit
    key-store-type: JKS
    private-key-password: changeit  # If different from key store password
    private-key-alias: client-cert  # Alias of the private key entry

    # Certificate Validation
    validate-certificate: true            # Verify server certificate
    validate-certificate-hostname: true   # Verify hostname matches cert

    # Protocol and Cipher Configuration
    protocols: TLSv1.2,TLSv1.3          # Allowed TLS versions
    cipher-suites: ""                    # Empty = use defaults

    # Advanced Options
    ssl-context-protocol: TLS            # SSL context protocol
    trust-manager-algorithm: SunX509    # Trust manager algorithm
    key-manager-algorithm: SunX509      # Key manager algorithm
```

## Environment-Specific Examples

### Development (Self-Signed Certificates)

For development with self-signed certificates:

```yaml
solace:
  host: tcps://localhost:55443
  msg-vpn: default
  client-username: admin
  client-password: admin

  tls:
    enabled: true
    trust-store-path: ${user.home}/.solace/dev-truststore.jks
    trust-store-password: ${SOLACE_TRUST_STORE_PASSWORD:changeit}

    # WARNING: Only for development!
    validate-certificate: false
    validate-certificate-hostname: false
```

### Production

For production with proper CA-signed certificates:

```yaml
solace:
  host: tcps://solace.prod.example.com:55443
  msg-vpn: production
  client-username: ${SOLACE_USERNAME}
  client-password: ${SOLACE_PASSWORD}

  tls:
    enabled: true
    trust-store-path: /etc/ssl/solace/truststore.jks
    trust-store-password: ${SOLACE_TRUST_STORE_PASSWORD}
    trust-store-type: JKS

    validate-certificate: true
    validate-certificate-hostname: true
    protocols: TLSv1.2,TLSv1.3
```

### Kubernetes with Secrets

Mount certificates from Kubernetes secrets:

```yaml
# application.yml
solace:
  host: tcps://solace-broker:55443
  msg-vpn: ${SOLACE_VPN}
  client-username: ${SOLACE_USERNAME}
  client-password: ${SOLACE_PASSWORD}

  tls:
    enabled: true
    trust-store-path: /etc/ssl/certs/solace/truststore.jks
    trust-store-password: ${SOLACE_TRUST_STORE_PASSWORD}
```

```yaml
# Kubernetes deployment snippet
spec:
  containers:
  - name: app
    volumeMounts:
    - name: solace-certs
      mountPath: /etc/ssl/certs/solace
      readOnly: true
  volumes:
  - name: solace-certs
    secret:
      secretName: solace-tls-certs
```

## Solace Cloud

For Solace Cloud, download the trust store from the Cloud Console:

1. Navigate to your service in Solace Cloud Console
2. Go to **Connect** tab
3. Download the trust store
4. Configure:

```yaml
solace:
  host: tcps://mr-connection-xxx.messaging.solace.cloud:55443
  msg-vpn: your-vpn
  client-username: solace-cloud-client
  client-password: ${SOLACE_CLOUD_PASSWORD}

  tls:
    enabled: true
    trust-store-path: /path/to/solace-cloud-truststore.jks
    trust-store-password: changeit
    validate-certificate: true
```

## Troubleshooting

### Common Errors

#### PKIX Path Building Failed

```
sun.security.validator.ValidatorException: PKIX path building failed
```

**Cause**: Trust store doesn't contain the CA certificate.

**Fix**: Import the correct CA certificate:
```bash
keytool -import -alias broker-ca -file ca.pem -keystore truststore.jks
```

#### Certificate Hostname Mismatch

```
javax.net.ssl.SSLPeerUnverifiedException: Hostname verification failed
```

**Cause**: Broker hostname doesn't match certificate CN/SAN.

**Fix**: Either:
1. Use the correct hostname matching the certificate
2. Set `validate-certificate-hostname: false` (not recommended for production)

#### Handshake Failure

```
javax.net.ssl.SSLHandshakeException: Received fatal alert: handshake_failure
```

**Cause**: Protocol or cipher mismatch between client and server.

**Fix**: Check allowed protocols and cipher suites:
```yaml
solace:
  tls:
    protocols: TLSv1.2,TLSv1.3
```

#### KeyStore Password Incorrect

```
java.io.IOException: Keystore was tampered with, or password was incorrect
```

**Cause**: Wrong password for trust store or key store.

**Fix**: Verify the password matches what was used when creating the store.

### Debugging TLS Issues

Enable SSL debug logging:

```bash
java -Djavax.net.debug=ssl:handshake -jar your-app.jar
```

Or in Spring Boot:

```yaml
logging:
  level:
    javax.net.ssl: DEBUG
    com.solace: DEBUG
```

### Verify Certificate Chain

```bash
# View trust store contents
keytool -list -v -keystore truststore.jks -storepass changeit

# View key store contents
keytool -list -v -keystore keystore.jks -storepass changeit

# Test TLS connection
openssl s_client -connect broker.example.com:55443 -showcerts
```

## Security Best Practices

1. **Always validate certificates in production** - Never disable `validate-certificate` in production
2. **Use strong TLS versions** - Prefer TLSv1.2 or TLSv1.3
3. **Protect passwords** - Use environment variables or secret management
4. **Rotate certificates** - Plan for certificate expiration and rotation
5. **Least privilege** - Use dedicated credentials per application
6. **Audit access** - Enable Solace audit logging for TLS connections
