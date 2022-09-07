/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.file;

import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class FileCertificateProvider implements CertificateProvider {
    private final String certificateFilePath;
    private final String privateKeyFilePath;

    public FileCertificateProvider(final String certificateFilePath,
            final String privateKeyFilePath) {
        this.certificateFilePath = Objects.requireNonNull(certificateFilePath);
        this.privateKeyFilePath = Objects.requireNonNull(privateKeyFilePath);
    }

    private static final Logger LOG = LoggerFactory.getLogger(FileCertificateProvider.class);

    public Certificate getCertificate() {
        try {
            final Path certFilePath = new File(certificateFilePath).toPath();
            final Path pkFilePath = new File(privateKeyFilePath).toPath();

            final byte[] certFileBytes = Files.readAllBytes(certFilePath);
            final byte[] pkFileBytes = Files.readAllBytes(pkFilePath);

            final String certAsString = new String(certFileBytes);
            final String privateKeyAsString = new String(pkFileBytes);

            return new Certificate(certAsString, privateKeyAsString);
        } catch (final Exception ex) {
            LOG.error("Error encountered while reading the certificate.", ex);
            throw new RuntimeException(ex);
        }
    }
}
