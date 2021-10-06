package com.amazon.dataprepper.plugins.certificate.file;

import com.amazon.dataprepper.plugins.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.certificate.model.Certificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            final Path certFilePath = Path.of(certificateFilePath);
            final Path pkFilePath = Path.of(privateKeyFilePath);

            final String certAsString = Files.readString(certFilePath);
            final String privateKeyAsString = Files.readString(pkFilePath);

            return new Certificate(certAsString, privateKeyAsString);
        } catch (final Exception ex) {
            LOG.error("Error encountered while reading the certificate.", ex);
            throw new RuntimeException(ex);
        }
    }
}
