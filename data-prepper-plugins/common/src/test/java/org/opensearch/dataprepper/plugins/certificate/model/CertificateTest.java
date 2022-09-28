/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.exception.CertificateFingerprintParsingException;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class CertificateTest {
    private static final String EXPECTED_FINGERPRINT = "fb6d45a36864f6673fa04b5601f54a1c96d3cb45";

    @Test
    void testGetFingerprint_Success() {
        final String certificateFilePath = "data/certificate/test_cert.crt";
        final String privateKeyFilePath = "data/certificate/test_decrypted_key.key";
        final FileCertificateProvider fileCertificateProvider = new FileCertificateProvider(certificateFilePath, privateKeyFilePath);
        final Certificate certificate = fileCertificateProvider.getCertificate();

        final String fingerprint = certificate.getFingerprint();
        assertThat(fingerprint, is(EXPECTED_FINGERPRINT));
    }

    @Test
    void testGetFingerprint_InvalidCert() {
        final Certificate certificate = new Certificate(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        assertThrows(CertificateFingerprintParsingException.class, () -> certificate.getFingerprint());
    }
}