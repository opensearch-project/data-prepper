/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.file;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class FileCertificateProviderTest {

    private FileCertificateProvider fileCertificateProvider;

    @Test
    public void getCertificateValidPathSuccess() throws IOException {
        final String certificateFilePath = FileCertificateProviderTest.class.getClassLoader().getResource("test-crt.crt").getPath();

        fileCertificateProvider = new FileCertificateProvider(certificateFilePath);

        final Certificate certificate = fileCertificateProvider.getCertificate();

        final Path certFilePath = Path.of(certificateFilePath);
        final String certAsString = Files.readString(certFilePath);

        assertThat(certificate.getCertificate(), is(certAsString));
    }

    @Test(expected = RuntimeException.class)
    public void getCertificateInvalidPathSuccess() {
        final String certificateFilePath = "path_does_not_exit/test_cert.crt";

        fileCertificateProvider = new FileCertificateProvider(certificateFilePath);

        fileCertificateProvider.getCertificate();
    }
}
