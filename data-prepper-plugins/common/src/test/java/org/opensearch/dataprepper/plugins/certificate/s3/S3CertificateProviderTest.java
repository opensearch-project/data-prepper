/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.s3;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3CertificateProviderTest {
    @Mock
    private S3Client s3Client;

    private S3CertificateProvider s3CertificateProvider;

    @Test
    public void getCertificateValidKeyPathSuccess() {
        final String certificateContent = UUID.randomUUID().toString();
        final String privateKeyContent = UUID.randomUUID().toString();
        final String bucketName = UUID.randomUUID().toString();
        final String certificatePath = UUID.randomUUID().toString();
        final String privateKeyPath = UUID.randomUUID().toString();

        final String s3SslKeyCertChainFile = String.format("s3://%s/%s",bucketName, certificatePath);
        final String s3SslKeyFile = String.format("s3://%s/%s",bucketName, privateKeyPath);

        final InputStream certObjectStream = IOUtils.toInputStream(certificateContent, StandardCharsets.UTF_8);
        final ResponseInputStream certResponseInputStream = new ResponseInputStream<>(GetObjectResponse.builder().build(), AbortableInputStream.create(certObjectStream));

        final InputStream privateKeyObjectStream = IOUtils.toInputStream(privateKeyContent, StandardCharsets.UTF_8);
        final ResponseInputStream<GetObjectResponse> privateKeyResponseInputStream = new ResponseInputStream<>(GetObjectResponse.builder().build(), AbortableInputStream.create(privateKeyObjectStream));

        final GetObjectRequest certRequest = GetObjectRequest.builder().bucket(bucketName).key(certificatePath).build();
        when(s3Client.getObject(certRequest)).thenReturn(certResponseInputStream);

        final GetObjectRequest keyRequest = GetObjectRequest.builder().bucket(bucketName).key(privateKeyPath).build();
        when(s3Client.getObject(keyRequest)).thenReturn(privateKeyResponseInputStream);

        s3CertificateProvider = new S3CertificateProvider(s3Client, s3SslKeyCertChainFile, s3SslKeyFile);

        final Certificate certificate = s3CertificateProvider.getCertificate();

        assertThat(certificate.getCertificate(), is(certificateContent));
        assertThat(certificate.getPrivateKey(), is(privateKeyContent));
    }

    @Test
    public void getCertificateValidKeyPathS3Exception() {
        final String certificatePath = UUID.randomUUID().toString();
        final String privateKeyPath = UUID.randomUUID().toString();
        final String bucketName = UUID.randomUUID().toString();

        final String s3SslKeyCertChainFile = String.format("s3://%s/%s",bucketName, certificatePath);
        final String s3SslKeyFile = String.format("s3://%s/%s",bucketName, privateKeyPath);

        s3CertificateProvider = new S3CertificateProvider(s3Client, s3SslKeyCertChainFile, s3SslKeyFile);

        when(s3Client.getObject(Mockito.any(GetObjectRequest.class))).thenThrow(new RuntimeException("S3 exception"));

        Assertions.assertThrows(RuntimeException.class, () -> s3CertificateProvider.getCertificate());
    }
}
