package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.acm.ACMCertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.file.FileCertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.s3.S3CertificateProvider;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.certificatemanager.AWSCertificateManager;
import com.amazonaws.services.certificatemanager.AWSCertificateManagerClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CertificateProviderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CertificateProviderFactory.class);

    final CertificateProviderConfig certificateProviderConfig;
    public CertificateProviderFactory(final CertificateProviderConfig certificateProviderConfig) {
        this.certificateProviderConfig = certificateProviderConfig;
    }

    public CertificateProvider getCertificateProvider() {
        // ACM Cert for SSL takes preference
        if (certificateProviderConfig.useAcmCertForSSL()) {
            LOG.info("Using ACM certificate for SSL/TLS to setup trust store.");
            final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
            final ClientConfiguration clientConfig = new ClientConfiguration()
                    .withThrottledRetries(true);
            final AWSCertificateManager awsCertificateManager = AWSCertificateManagerClientBuilder.standard()
                    .withRegion(certificateProviderConfig.getAwsRegion())
                    .withCredentials(credentialsProvider)
                    .withClientConfiguration(clientConfig)
                    .build();
            return new ACMCertificateProvider(awsCertificateManager, certificateProviderConfig.getAcmCertificateArn(),
                    certificateProviderConfig.getAcmCertIssueTimeOutMillis());
        } else if (certificateProviderConfig.isSslCertFileInS3()) {
            LOG.info("Using S3 to fetch certificate for SSL/TLS to setup trust store.");
            final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
            final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(certificateProviderConfig.getAwsRegion())
                    .withCredentials(credentialsProvider)
                    .build();
            return new S3CertificateProvider(s3Client, certificateProviderConfig.getSslKeyCertChainFile());
        } else {
            LOG.info("Using local file system to get certificate for SSL/TLS to setup trust store.");
            return new FileCertificateProvider(certificateProviderConfig.getSslKeyCertChainFile());
        }
    }
}
