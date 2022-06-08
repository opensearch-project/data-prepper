/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.acm.ACMCertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.file.FileCertificateProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.s3.S3CertificateProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.acm.AcmClient;
import software.amazon.awssdk.services.s3.S3Client;

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
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
            final ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryMode.STANDARD)
                    .build();
            final AcmClient awsCertificateManager = AcmClient.builder()
                    .region(Region.of(certificateProviderConfig.getAwsRegion()))
                    .credentialsProvider(credentialsProvider)
                    .overrideConfiguration(clientConfig)
                    .build();
            return new ACMCertificateProvider(awsCertificateManager, certificateProviderConfig.getAcmCertificateArn(),
                    certificateProviderConfig.getAcmCertIssueTimeOutMillis());
        } else if (certificateProviderConfig.isSslCertFileInS3()) {
            LOG.info("Using S3 to fetch certificate for SSL/TLS to setup trust store.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
            final S3Client s3Client = S3Client.builder()
                    .region(Region.of(certificateProviderConfig.getAwsRegion()))
                    .credentialsProvider(credentialsProvider)
                    .build();
            return new S3CertificateProvider(s3Client, certificateProviderConfig.getSslKeyCertChainFile());
        } else {
            LOG.info("Using local file system to get certificate for SSL/TLS to setup trust store.");
            return new FileCertificateProvider(certificateProviderConfig.getSslKeyCertChainFile());
        }
    }
}
