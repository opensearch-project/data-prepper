/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http.common.certificate;

import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import org.opensearch.dataprepper.http.common.HttpServerConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CertificateProviderFactoryTest {
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    private HttpServerConfig httpServerConfig;
    private CertificateProviderFactory certificateProviderFactory;

    @BeforeEach
    void setUp() {
        httpServerConfig = mock(HttpServerConfig.class);
    }

    @Test
    void getCertificateProviderFileCertificateProviderSuccess() {
        when(httpServerConfig.isSsl()).thenReturn(true);
        when(httpServerConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(httpServerConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);

        certificateProviderFactory = new CertificateProviderFactory(httpServerConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(FileCertificateProvider.class));
    }

    @Test
    void getCertificateProviderS3ProviderSuccess() {
        when(httpServerConfig.isSslCertAndKeyFileInS3()).thenReturn(true);
        when(httpServerConfig.getAwsRegion()).thenReturn("us-east-1");
        when(httpServerConfig.getSslCertificateFile()).thenReturn("s3://data/certificate/test_cert.crt");
        when(httpServerConfig.getSslKeyFile()).thenReturn("s3://data/certificate/test_decrypted_key.key");

        certificateProviderFactory = new CertificateProviderFactory(httpServerConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(S3CertificateProvider.class));
    }

    @Test
    void getCertificateProviderAcmProviderSuccess() {
        when(httpServerConfig.isUseAcmCertificateForSsl()).thenReturn(true);
        when(httpServerConfig.getAwsRegion()).thenReturn("us-east-1");
        when(httpServerConfig.getAcmCertificateArn()).thenReturn("arn:aws:acm:us-east-1:account:certificate/1234-567-856456");

        certificateProviderFactory = new CertificateProviderFactory(httpServerConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(ACMCertificateProvider.class));
    }
}