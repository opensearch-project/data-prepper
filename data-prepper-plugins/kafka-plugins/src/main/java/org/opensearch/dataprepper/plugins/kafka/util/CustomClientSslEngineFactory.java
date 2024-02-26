/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.opensearch.dataprepper.plugins.truststore.TrustStoreProvider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CustomClientSslEngineFactory implements SslEngineFactory {
    String certificateContent = null;

    @Override
    public void configure(Map<String, ?> configs) {
        certificateContent = configs.get("certificateContent").toString();
    }

    private TrustManager[] getTrustManager() {
        final TrustManager[] trustManagers;
        if (Objects.nonNull(certificateContent)) {
            trustManagers = TrustStoreProvider.createTrustManager(certificateContent);
        } else {
            trustManagers = TrustStoreProvider.createTrustAllManager();
        }
        return trustManagers;
    }

    @Override
    public SSLEngine createClientSslEngine(final String peerHost, final int peerPort, final String endpointIdentification) {
        try {
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, getTrustManager(), new SecureRandom());
            SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);
            sslEngine.setUseClientMode(true);
            return sslEngine;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        return null;
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return null;
    }

    @Override
    public KeyStore keystore() {
        return null;
    }

    @Override
    public KeyStore truststore() {
        return null;
    }

    @Override
    public void close() {

    }
}
