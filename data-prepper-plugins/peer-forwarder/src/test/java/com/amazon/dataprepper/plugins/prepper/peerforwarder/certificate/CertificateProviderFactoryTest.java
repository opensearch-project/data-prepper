package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.acm.ACMCertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.file.FileCertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.s3.S3CertificateProvider;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class CertificateProviderFactoryTest {
    private CertificateProviderFactory certificateProviderFactory;

    @Test
    public void getCertificateProviderAcmProviderSuccess() {
        final boolean useAcmCertForSSL = true;
        final String awsRegion = "us-east-1";
        final String acmCertificateArn = "arn:aws:acm:us-east-1:account:certificate/1234-567-856456";
        final String sslKeyCertChainFile = null;
        final long acmCertIssueTimeOutMillis = 1000L;

        final CertificateProviderConfig certificateProviderConfig = new CertificateProviderConfig(
                useAcmCertForSSL,
                acmCertificateArn,
                awsRegion,
                acmCertIssueTimeOutMillis,
                sslKeyCertChainFile
        );

        certificateProviderFactory = new CertificateProviderFactory(certificateProviderConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(ACMCertificateProvider.class));
    }

    @Test
    public void getCertificateProviderS3ProviderSuccess() {
        final boolean useAcmCertForSSL = false;
        final String awsRegion = "us-east-1";
        final String acmCertificateArn = null;
        final String sslKeyCertChainFile = "s3://some_s3_bucket/certificate/test_cert.crt";
        final long acmCertIssueTimeOutMillis = 1000L;

        final CertificateProviderConfig certificateProviderConfig = new CertificateProviderConfig(
                useAcmCertForSSL,
                acmCertificateArn,
                awsRegion,
                acmCertIssueTimeOutMillis,
                sslKeyCertChainFile
        );

        certificateProviderFactory = new CertificateProviderFactory(certificateProviderConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();;

        assertThat(certificateProvider, IsInstanceOf.instanceOf(S3CertificateProvider.class));
    }

    @Test
    public void getCertificateProviderFileProviderSuccess() {
        final boolean useAcmCertForSSL = false;
        final String awsRegion = null;
        final String acmCertificateArn = null;
        final String sslKeyCertChainFile = "path_to_certificate/test_cert.crt";
        final long acmCertIssueTimeOutMillis = 1000L;

        final CertificateProviderConfig certificateProviderConfig = new CertificateProviderConfig(
                useAcmCertForSSL,
                acmCertificateArn,
                awsRegion,
                acmCertIssueTimeOutMillis,
                sslKeyCertChainFile
        );

        certificateProviderFactory = new CertificateProviderFactory(certificateProviderConfig);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(FileCertificateProvider.class));
    }
}
