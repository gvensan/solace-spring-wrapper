package com.solace.wrapper.connection;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.AuthenticationStrategy;
import com.solace.messaging.config.TransportSecurityStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

public class SolaceConnectionManagerAuthTlsTest {

    private static final Logger log = LoggerFactory.getLogger(SolaceConnectionManagerAuthTlsTest.class);

    static class TestCM extends SolaceConnectionManager {
        TestCM(com.solace.wrapper.config.SolaceProperties p) { super(p); }
        @Override public MessagingService createMessagingService() {
            return (MessagingService) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class[]{MessagingService.class}, (p, m, a) -> {
                        if ("connect".equals(m.getName())) return p;
                        if ("isConnected".equals(m.getName())) return true;
                        return null;
                    });
        }
    }

    private com.solace.wrapper.config.SolaceProperties props;

    @BeforeEach
    void init() {
        props = new com.solace.wrapper.config.SolaceProperties();
        props.setHost("tcp://noop:55555"); props.setMsgVpn("vpn"); props.setClientUsername("u"); props.setClientPassword("p");
    }

    @Test
    void oauth2_strategy_is_built_when_token_present() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: oauth2_strategy_is_built_when_token_present\n" +
                "PURPOSE: Verify OAuth2 authentication strategy is built when access token is configured\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Configuring OAuth2 access token and issuer identifier");
        props.setOauth2AccessToken("tok");
        props.setOauth2IssuerIdentifier("iss");

        log.info("STEP 2: Creating connection manager and invoking buildAuthStrategyIfConfigured()");
        SolaceConnectionManager cm = new TestCM(props);
        Method m = SolaceConnectionManager.class.getDeclaredMethod("buildAuthStrategyIfConfigured");
        m.setAccessible(true);
        Object strat = m.invoke(cm);

        log.info("STEP 3: Verifying OAuth2 strategy was built");
        assertThat(strat).isInstanceOf(AuthenticationStrategy.class);
        assertThat(strat.getClass().getName()).contains("OAuth2");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: OAuth2 strategy built successfully (class=" + strat.getClass().getSimpleName() + ")\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void client_cert_strategy_is_built_when_keystore_present() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: client_cert_strategy_is_built_when_keystore_present\n" +
                "PURPOSE: Verify client certificate authentication strategy is built when keystore is configured\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Configuring client certificate keystore properties");
        props.setOauth2AccessToken(null);
        props.setClientCertKeystorePath("/path/to/ks");
        props.setClientCertKeystorePassword("secret");
        props.setClientCertKeystoreFormat("PKCS12");
        props.setClientCertPrivateKeyPassword("pk");

        log.info("STEP 2: Creating connection manager and invoking buildAuthStrategyIfConfigured()");
        SolaceConnectionManager cm = new TestCM(props);
        Method m = SolaceConnectionManager.class.getDeclaredMethod("buildAuthStrategyIfConfigured");
        m.setAccessible(true);
        Object strat = m.invoke(cm);

        log.info("STEP 3: Verifying ClientCertificateAuthentication strategy was built");
        assertThat(strat).isInstanceOf(AuthenticationStrategy.class);
        assertThat(strat.getClass().getName()).contains("ClientCertificateAuthentication");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: Client certificate strategy built successfully (class=" + strat.getClass().getSimpleName() + ")\n" +
                "───────────────────────────────────────────────────────────────\n");
    }

    @Test
    void tls_strategy_is_built_when_truststore_present() throws Exception {
        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "TEST: tls_strategy_is_built_when_truststore_present\n" +
                "PURPOSE: Verify TLS transport security strategy is built when truststore is configured\n" +
                "───────────────────────────────────────────────────────────────");

        log.info("STEP 1: Configuring TLS truststore properties");
        props.setTlsTrustStorePath("/path/to/trust");
        props.setTlsTrustStorePassword("tpw");
        props.setTlsTrustStoreFormat("JKS");
        props.setTlsIgnoreCertificateExpiration(true);

        log.info("STEP 2: Creating connection manager and invoking buildTlsStrategyIfConfigured()");
        SolaceConnectionManager cm = new TestCM(props);
        Method m = SolaceConnectionManager.class.getDeclaredMethod("buildTlsStrategyIfConfigured");
        m.setAccessible(true);
        Object strat = m.invoke(cm);

        log.info("STEP 3: Verifying TLS strategy was built");
        assertThat(strat).isInstanceOf(TransportSecurityStrategy.class);
        assertThat(strat.getClass().getName()).contains("TLS");

        log.info("\n───────────────────────────────────────────────────────────────\n" +
                "RESULT: TLS strategy built successfully (class=" + strat.getClass().getSimpleName() + ")\n" +
                "───────────────────────────────────────────────────────────────\n");
    }
}
