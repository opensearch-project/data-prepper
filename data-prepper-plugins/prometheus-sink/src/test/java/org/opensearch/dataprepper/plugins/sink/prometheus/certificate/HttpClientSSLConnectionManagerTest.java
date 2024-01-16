/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.certificate;

import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpClientSSLConnectionManagerTest {

    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    HttpClientSSLConnectionManager httpClientSSLConnectionManager;

    private CertificateProviderFactory certificateProviderFactory;

    private PrometheusSinkConfiguration prometheusSinkConfiguration;

    @BeforeEach
    void setup() throws IOException {
        this.prometheusSinkConfiguration = mock(PrometheusSinkConfiguration.class);
        this.certificateProviderFactory = mock(CertificateProviderFactory.class);
    }

    @Test
    public void create_httpClientConnectionManager_with_ssl_file_test() {
        when(prometheusSinkConfiguration.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(prometheusSinkConfiguration.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        CertificateProvider provider = new FileCertificateProvider(prometheusSinkConfiguration.getSslCertificateFile(), prometheusSinkConfiguration.getSslKeyFile());
        when(certificateProviderFactory.getCertificateProvider()).thenReturn(provider);

        httpClientSSLConnectionManager = new HttpClientSSLConnectionManager();
        HttpClientConnectionManager httpClientConnectionManager = httpClientSSLConnectionManager
                .createHttpClientConnectionManager(prometheusSinkConfiguration, certificateProviderFactory);
        assertNotNull(httpClientConnectionManager);
    }
}
