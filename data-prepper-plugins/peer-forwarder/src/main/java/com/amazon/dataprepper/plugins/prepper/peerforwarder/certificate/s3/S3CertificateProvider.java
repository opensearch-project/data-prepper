/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.s3;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class S3CertificateProvider implements CertificateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(S3CertificateProvider.class);
    private final AmazonS3 s3Client;
    private final String certificateFilePath;

    public S3CertificateProvider(final AmazonS3 s3Client,
                                 final String certificateFilePath) {
        this.s3Client = Objects.requireNonNull(s3Client);
        this.certificateFilePath = Objects.requireNonNull(certificateFilePath);
    }

    public Certificate getCertificate() {
        final AmazonS3URI certificateS3URI = new AmazonS3URI(certificateFilePath);
        final String certificate = getObjectWithKey(certificateS3URI.getBucket(), certificateS3URI.getKey());

        return new Certificate(certificate);
    }

    private String getObjectWithKey(final String bucketName, final String key) {

        // Download the object
        try (final S3Object s3Object = s3Client.getObject(bucketName, key)) {
            LOG.info("Object with key \"{}\" downloaded.", key);
            return IOUtils.toString(s3Object.getObjectContent(), StandardCharsets.UTF_8);
        } catch (final Exception ex) {
            LOG.error("Error encountered while processing the response from Amazon S3.", ex);
            throw new RuntimeException(ex);
        }
    }
}
