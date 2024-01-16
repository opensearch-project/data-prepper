/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.s3;

import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;

class CertificateProviderFactoryTest {
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    private CertificateProviderFactory certificateProviderFactory;

    @Test
    void getCertificateProviderFileCertificateProviderSuccess() {
        certificateProviderFactory = new CertificateProviderFactory(false, Region.of("us-east-1"),
                "arn:aws:acm:us-east-1:account:certificate/1234-567-856456", 5L, "test", false, TEST_SSL_CERTIFICATE_FILE, TEST_SSL_KEY_FILE);

        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(FileCertificateProvider.class));
    }

    @Test
    void getCertificateProviderS3ProviderSuccess() {

        certificateProviderFactory = new CertificateProviderFactory(false, Region.of("us-east-1"),
                "arn:aws:acm:us-east-1:account:certificate/1234-567-856456", 5L, "test", true, TEST_SSL_CERTIFICATE_FILE, TEST_SSL_KEY_FILE);

        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(S3CertificateProvider.class));
    }

    @Test
    void getCertificateProviderAcmProviderSuccess() {
        certificateProviderFactory = new CertificateProviderFactory(true, Region.of("us-east-1"),
                "arn:aws:acm:us-east-1:account:certificate/1234-567-856456", 5L, "test", false, TEST_SSL_CERTIFICATE_FILE, TEST_SSL_KEY_FILE);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(ACMCertificateProvider.class));
    }
}