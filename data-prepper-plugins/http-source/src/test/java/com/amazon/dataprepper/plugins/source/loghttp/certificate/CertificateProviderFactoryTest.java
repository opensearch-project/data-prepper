/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loghttp.certificate;

import com.amazon.dataprepper.plugins.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.certificate.file.FileCertificateProvider;
import com.amazon.dataprepper.plugins.source.loghttp.HTTPSourceConfig;
import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CertificateProviderFactoryTest {
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    @Test
    public void getFileCertificateProviderSuccess() {
        final HTTPSourceConfig sourceConfig = mock(HTTPSourceConfig.class);
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);

        final CertificateProviderFactory certificateProviderFactory = new CertificateProviderFactory(sourceConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(FileCertificateProvider.class));
    }
}