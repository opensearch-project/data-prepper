package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.file;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class FileCertificateProvider implements CertificateProvider {
    private final String certificateFilePath;

    public FileCertificateProvider(final String certificateFilePath) {
        this.certificateFilePath = Objects.requireNonNull(certificateFilePath);
    }

    private static final Logger LOG = LoggerFactory.getLogger(FileCertificateProvider.class);

    public Certificate getCertificate() {
        try {
            final Path certFilePath = Paths.get(certificateFilePath);

            final byte[] bytes = Files.readAllBytes(certFilePath);

            final String certAsString = new String(bytes);

            return new Certificate(certAsString);
        } catch (final Exception ex) {
            LOG.error("Error encountered while reading the certificate.", ex);
            throw new RuntimeException(ex);
        }
    }
}
