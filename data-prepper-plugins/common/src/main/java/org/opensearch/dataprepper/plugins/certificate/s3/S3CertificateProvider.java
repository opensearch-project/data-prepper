/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.s3;

import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class S3CertificateProvider implements CertificateProvider {
    private static final Logger LOG = LoggerFactory.getLogger(S3CertificateProvider.class);
    private final S3Client s3Client;
    private final String certificateFile;
    private final String privateKeyFile;

    public S3CertificateProvider(final S3Client s3Client,
                                 final String certificateFile,
                                 final String privateKeyFile) {
        this.s3Client = Objects.requireNonNull(s3Client);
        this.certificateFile = Objects.requireNonNull(certificateFile);
        this.privateKeyFile = Objects.requireNonNull(privateKeyFile);
    }

    public Certificate getCertificate() {

        try {

            final URI certificateFileUri = new URI(certificateFile);
            final URI privateKeyFileUri = new URI(privateKeyFile);

            final String certificate = getObjectWithKey(certificateFileUri.getHost(), certificateFileUri.getPath().substring(1));
            final String privateKey = getObjectWithKey(privateKeyFileUri.getHost(), privateKeyFileUri.getPath().substring(1));

            return new Certificate(certificate, privateKey);

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
