/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.certificate;

import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpClientSSLConnectionManagerTest {

    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    HttpClientSSLConnectionManager httpClientSSLConnectionManager;

    private CertificateProviderFactory certificateProviderFactory;

    private HttpSinkConfiguration httpSinkConfiguration;

    @BeforeEach
    void setup() throws IOException {
        this.httpSinkConfiguration = mock(HttpSinkConfiguration.class);
        this.certificateProviderFactory = mock(CertificateProviderFactory.class);
    }

    @Test
    public void create_httpClientConnectionManager_with_ssl_file_test() {
        when(httpSinkConfiguration.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(httpSinkConfiguration.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        CertificateProvider provider = new FileCertificateProvider(httpSinkConfiguration.getSslCertificateFile(), httpSinkConfiguration.getSslKeyFile());
        when(certificateProviderFactory.getCertificateProvider()).thenReturn(provider);

        CertificateProviderFactory providerFactory = new CertificateProviderFactory(httpSinkConfiguration);
        httpClientSSLConnectionManager = new HttpClientSSLConnectionManager();
        HttpClientConnectionManager httpClientConnectionManager = httpClientSSLConnectionManager
                .createHttpClientConnectionManager(httpSinkConfiguration, providerFactory);
        assertNotNull(httpClientConnectionManager);
    }
}
