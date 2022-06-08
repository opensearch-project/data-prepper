/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.s3;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class S3CertificateProvider implements CertificateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(S3CertificateProvider.class);
    private final S3Client s3Client;
    private final String certificateFilePath;

    public S3CertificateProvider(final S3Client s3Client,
                                 final String certificateFilePath) {
        this.s3Client = Objects.requireNonNull(s3Client);
        this.certificateFilePath = Objects.requireNonNull(certificateFilePath);
    }

    public Certificate getCertificate() {

        try {

            final URI certificateFileUri = new URI(certificateFilePath);
            final String certificate = getObjectWithKey(certificateFileUri.getHost(), certificateFileUri.getPath().substring(1));

            return new Certificate(certificate);

        } catch (URISyntaxException ex) {
            LOG.error("Error encountered while parsing the certificate's Amazon S3 URI.", ex);
            throw new RuntimeException(ex);
        }

    }

    private String getObjectWithKey(final String bucketName, final String key) {

        // Download the object
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
        try (final ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest)) {
            LOG.info("Object with key \"{}\" downloaded.", key);
            return IOUtils.toString(s3Object, StandardCharsets.UTF_8);
        } catch (final Exception ex) {
            LOG.error("Error encountered while processing the response from Amazon S3.", ex);
            throw new RuntimeException(ex);
        }
    }
}
