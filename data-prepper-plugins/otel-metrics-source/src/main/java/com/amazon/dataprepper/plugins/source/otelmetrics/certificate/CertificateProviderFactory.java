/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otelmetrics.certificate;

import com.amazon.dataprepper.plugins.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import com.amazon.dataprepper.plugins.certificate.file.FileCertificateProvider;
import com.amazon.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import com.amazon.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig;
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

    final OTelMetricsSourceConfig oTelMetricsSourceConfig;
    public CertificateProviderFactory(final OTelMetricsSourceConfig oTelMetricsSourceConfig) {
        this.oTelMetricsSourceConfig = oTelMetricsSourceConfig;
    }

    public CertificateProvider getCertificateProvider() {
        // ACM Cert for SSL takes preference
        if (oTelMetricsSourceConfig.useAcmCertForSSL()) {
            LOG.info("Using ACM certificate and private key for SSL/TLS.");
            final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
            final ClientConfiguration clientConfig = new ClientConfiguration()
                    .withThrottledRetries(true);
            final AWSCertificateManager awsCertificateManager = AWSCertificateManagerClientBuilder.standard()
                    .withRegion(oTelMetricsSourceConfig.getAwsRegion())
                    .withCredentials(credentialsProvider)
                    .withClientConfiguration(clientConfig)
                    .build();
            return new ACMCertificateProvider(awsCertificateManager, oTelMetricsSourceConfig.getAcmCertificateArn(),
                    oTelMetricsSourceConfig.getAcmCertIssueTimeOutMillis(), oTelMetricsSourceConfig.getAcmPrivateKeyPassword());
        } else if (oTelMetricsSourceConfig.isSslCertAndKeyFileInS3()) {
            LOG.info("Using S3 to fetch certificate and private key for SSL/TLS.");
            final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
            final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(oTelMetricsSourceConfig.getAwsRegion())
                    .withCredentials(credentialsProvider)
                    .build();
            return new S3CertificateProvider(s3Client, oTelMetricsSourceConfig.getSslKeyCertChainFile(), oTelMetricsSourceConfig.getSslKeyFile());
        } else {
            LOG.info("Using local file system to get certificate and private key for SSL/TLS.");
            return new FileCertificateProvider(oTelMetricsSourceConfig.getSslKeyCertChainFile(), oTelMetricsSourceConfig.getSslKeyFile());
        }
    }
}
