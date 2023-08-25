/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.certificate;

import org.apache.hc.client5.http.config.TlsConfig;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

/**
 * This class implements SSL certs authentication
 *
 */
public class HttpClientSSLConnectionManager {

    /**
     * This method creates HttpClientConnectionManager for SSL certs authentication
     * @param sinkConfiguration HttpSinkConfiguration
     * @param providerFactory CertificateProviderFactory
     * @return HttpClientConnectionManager
     */
    public HttpClientConnectionManager createHttpClientConnectionManager(final PrometheusSinkConfiguration sinkConfiguration,
                                                                         final CertificateProviderFactory providerFactory){
        final CertificateProvider certificateProvider = providerFactory.getCertificateProvider();
        final org.opensearch.dataprepper.plugins.certificate.model.Certificate certificate = certificateProvider.getCertificate();
        final SSLContext sslContext = sinkConfiguration.getSslCertificateFile() != null ?
                getCAStrategy(new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8))) : getTrustAllStrategy();
        SSLConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactoryBuilder.create()
                .setSslContext(sslContext)
                .build();
       return PoolingHttpClientConnectionManagerBuilder.create()
                .setSSLSocketFactory(sslSocketFactory)
                .setDefaultTlsConfig(TlsConfig.custom()
                        .setHandshakeTimeout(Timeout.ofSeconds(30))
                        .setSupportedProtocols(TLS.V_1_3)
                        .build())
                .build();
    }

    private SSLContext getCAStrategy(final InputStream certificate) {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate trustedCa = factory.generateCertificate(certificate);
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null);
            return sslContextBuilder.build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private SSLContext getTrustAllStrategy() {
        final TrustStrategy trustStrategy = new TrustAllStrategy();
        try {
            return SSLContexts.custom().loadTrustMaterial(null, trustStrategy).build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
