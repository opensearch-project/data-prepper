/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.s3;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3CertificateProviderTest {

    @Mock
    private S3Client s3Client;

    private S3CertificateProvider s3CertificateProvider;

    @Test
    public void getCertificateValidKeyPathSuccess() {
        final String certificateContent = UUID.randomUUID().toString();
        final String bucketName = UUID.randomUUID().toString();
        final String certificatePath = UUID.randomUUID().toString();

        final String s3SslKeyCertChainFile = String.format("s3://%s/%s",bucketName, certificatePath);

        final InputStream certObjectStream = IOUtils.toInputStream(certificateContent, StandardCharsets.UTF_8);
        final ResponseInputStream certResponseInputStream = new ResponseInputStream<>(GetObjectResponse.builder().build(), AbortableInputStream.create(certObjectStream));

        final GetObjectRequest certRequest = GetObjectRequest.builder().bucket(bucketName).key(certificatePath).build();
        when(s3Client.getObject(certRequest)).thenReturn(certResponseInputStream);

        s3CertificateProvider = new S3CertificateProvider(s3Client, s3SslKeyCertChainFile);

        final Certificate certificate = s3CertificateProvider.getCertificate();

        assertThat(certificate.getCertificate(), is(certificateContent));
    }

    @Test(expected = RuntimeException.class)
    public void getCertificateValidKeyPathS3Exception() {
        final String certificatePath = UUID.randomUUID().toString();
        final String bucketName = UUID.randomUUID().toString();

        final String s3SslKeyCertChainFile = String.format("s3://%s/%s",bucketName, certificatePath);

        s3CertificateProvider = new S3CertificateProvider(s3Client, s3SslKeyCertChainFile);
        when(s3Client.getObject(any(GetObjectRequest.class))).thenThrow(new RuntimeException("S3 exception"));

        s3CertificateProvider.getCertificate();
    }
}
