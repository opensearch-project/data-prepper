/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class FileCertificateProviderTest {

    private FileCertificateProvider fileCertificateProvider;

    @Test
    public void getCertificateValidPathSuccess() throws IOException {
        final String certificateFilePath = "data/certificate/test_cert.crt";
        final String privateKeyFilePath = "data/certificate/test_decrypted_key.key";

        fileCertificateProvider = new FileCertificateProvider(certificateFilePath, privateKeyFilePath);

        final Certificate certificate = fileCertificateProvider.getCertificate();

        final Path certFilePath = Path.of(certificateFilePath);
        final Path keyFilePath = Path.of(privateKeyFilePath);
        final String certAsString = Files.readString(certFilePath);
        final String keyAsString = Files.readString(keyFilePath);

        assertThat(certificate.getCertificate(), is(certAsString));
        assertThat(certificate.getPrivateKey(), is(keyAsString));
    }

    @Test
    public void getCertificateInvalidPathSuccess() {
        final String certificateFilePath = "path_does_not_exit/test_cert.crt";
        final String privateKeyFilePath = "path_does_not_exit/test_decrypted_key.key";

        fileCertificateProvider = new FileCertificateProvider(certificateFilePath, privateKeyFilePath);

        Assertions.assertThrows(RuntimeException.class, () -> fileCertificateProvider.getCertificate());
    }
}
