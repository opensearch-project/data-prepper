/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.s3;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3CertificateProviderTest {
    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private S3Object certS3Object;

    private S3CertificateProvider s3CertificateProvider;

    @Test
    public void getCertificateValidKeyPathSuccess() {
        final String certificateContent = UUID.randomUUID().toString();
        final String bucketName = UUID.randomUUID().toString();
        final String certificatePath = UUID.randomUUID().toString();

        final String s3SslKeyCertChainFile = String.format("s3://%s/%s",bucketName, certificatePath);

        final InputStream certObjectStream = IOUtils.toInputStream(certificateContent, StandardCharsets.UTF_8);

        when(certS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(certObjectStream,null));

        when(amazonS3.getObject(bucketName, certificatePath)).thenReturn(certS3Object);

        s3CertificateProvider = new S3CertificateProvider(amazonS3, s3SslKeyCertChainFile);

        final Certificate certificate = s3CertificateProvider.getCertificate();

        assertThat(certificate.getCertificate(), is(certificateContent));
    }

    @Test(expected = RuntimeException.class)
    public void getCertificateValidKeyPathS3Exception() {
        final String certificatePath = UUID.randomUUID().toString();
        final String bucketName = UUID.randomUUID().toString();

        final String s3SslKeyCertChainFile = String.format("s3://%s/%s",bucketName, certificatePath);

        s3CertificateProvider = new S3CertificateProvider(amazonS3, s3SslKeyCertChainFile);
        when(amazonS3.getObject(anyString(), anyString())).thenThrow(new RuntimeException("S3 exception"));

        s3CertificateProvider.getCertificate();
    }
}
