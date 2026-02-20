/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs.certificate;

import org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import org.opensearch.dataprepper.plugins.metricpublisher.MicrometerMetricPublisher;
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

    final OTelLogsSourceConfig oTelLogsSourceConfig;
    public CertificateProviderFactory(final OTelLogsSourceConfig oTelLogsSourceConfig) {
        this.oTelLogsSourceConfig = oTelLogsSourceConfig;
    }

    public CertificateProvider getCertificateProvider() {
        // ACM Cert for SSL takes preference
        if (oTelLogsSourceConfig.useAcmCertForSSL()) {
            LOG.info("Using ACM certificate and private key for SSL/TLS.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
            final ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryMode.STANDARD)
                    .build();

            final PluginMetrics awsSdkMetrics = PluginMetrics.fromNames("sdk", "aws");

            final AcmClient awsCertificateManager = AcmClient.builder()
                    .region(Region.of(oTelLogsSourceConfig.getAwsRegion()))
                    .credentialsProvider(credentialsProvider)
                    .overrideConfiguration(clientConfig)
                    .overrideConfiguration(metricPublisher -> metricPublisher.addMetricPublisher(new MicrometerMetricPublisher(awsSdkMetrics)))
                    .build();

            return new ACMCertificateProvider(awsCertificateManager,
                    oTelLogsSourceConfig.getAcmCertificateArn(),
                    oTelLogsSourceConfig.getAcmCertIssueTimeOutMillis(),
                    oTelLogsSourceConfig.getAcmPrivateKeyPassword());
        } else if (oTelLogsSourceConfig.isSslCertAndKeyFileInS3()) {
            LOG.info("Using S3 to fetch certificate and private key for SSL/TLS.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
            final S3Client s3Client = S3Client.builder()
                    .region(Region.of(oTelLogsSourceConfig.getAwsRegion()))
                    .credentialsProvider(credentialsProvider)
                    .build();
            return new S3CertificateProvider(s3Client,
                    oTelLogsSourceConfig.getSslKeyCertChainFile(),
                    oTelLogsSourceConfig.getSslKeyFile());
        } else {
            LOG.info("Using local file system to get certificate and private key for SSL/TLS.");
            return new FileCertificateProvider(oTelLogsSourceConfig.getSslKeyCertChainFile(), oTelLogsSourceConfig.getSslKeyFile());
        }
    }
}
