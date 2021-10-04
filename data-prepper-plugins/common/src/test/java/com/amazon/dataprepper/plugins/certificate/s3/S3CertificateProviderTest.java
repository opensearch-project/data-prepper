package com.amazon.dataprepper.plugins.certificate.s3;

import com.amazon.dataprepper.plugins.certificate.model.Certificate;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3CertificateProviderTest {
    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private S3Object certS3Object;

    @Mock
    private S3Object privateKeyS3Object;

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
        final InputStream privateKeyObjectStream = IOUtils.toInputStream(privateKeyContent, StandardCharsets.UTF_8);

        when(certS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(certObjectStream,null));
        when(privateKeyS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(privateKeyObjectStream,null));

        when(amazonS3.getObject(bucketName, certificatePath)).thenReturn(certS3Object);
        when(amazonS3.getObject(bucketName, privateKeyPath)).thenReturn(privateKeyS3Object);

        s3CertificateProvider = new S3CertificateProvider(amazonS3, s3SslKeyCertChainFile, s3SslKeyFile);

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

        s3CertificateProvider = new S3CertificateProvider(amazonS3, s3SslKeyCertChainFile, s3SslKeyFile);
        when(amazonS3.getObject(anyString(), anyString())).thenThrow(new RuntimeException("S3 exception"));

        Assertions.assertThrows(RuntimeException.class, () -> s3CertificateProvider.getCertificate());
    }
}
