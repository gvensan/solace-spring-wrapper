package com.solace.wrapper.broker;

import com.solace.wrapper.config.SolaceProperties;

/**
 * Utility to build SolaceProperties from test settings.
 */
public final class BrokerTestSupport {
    private BrokerTestSupport() {}

    public static SolaceProperties toSolaceProperties(BrokerTestSettings settings) {
        SolaceProperties props = new SolaceProperties();
        props.setHost(settings.host);
        props.setMsgVpn(settings.msgVpn);
        props.setClientUsername(settings.username);
        props.setClientPassword(settings.password);
        props.setClientName("solace-wrapper-broker-it");
        props.setReconnectRetries(true);
        props.setReconnectionAttempts(3);
        props.setReconnectionAttemptsWaitIntervalInMillis(3000);
        props.setConnectTimeoutInMillis(10000);
        props.setKeepAliveIntervalInMillis(3000);
        props.setPublisherPersistentAckTimeoutInMs(5000);
        return props;
    }
}
