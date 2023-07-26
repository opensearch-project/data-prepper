/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.certificate;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import org.opensearch.dataprepper.plugins.metricpublisher.MicrometerMetricPublisher;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.services.acm.AcmClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * This class consist logic for downloading the SSL certificates from S3/ACM/Local file.
 *
 */
public class CertificateProviderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CertificateProviderFactory.class);

    final HttpSinkConfiguration httpSinkConfiguration;
    public CertificateProviderFactory(final HttpSinkConfiguration httpSinkConfiguration) {
        this.httpSinkConfiguration = httpSinkConfiguration;
    }

    /**
     * This method consist logic for downloading the SSL certificates from S3/ACM/Local file.
     * @return CertificateProvider
     */
    public CertificateProvider getCertificateProvider() {
        if (httpSinkConfiguration.useAcmCertForSSL()) {
            LOG.info("Using ACM certificate and private key for SSL/TLS.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
            final ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryMode.STANDARD)
                    .build();

            final PluginMetrics awsSdkMetrics = PluginMetrics.fromNames("sdk", "aws");

            final AcmClient awsCertificateManager = AcmClient.builder()
                    .region(httpSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion())
                    .credentialsProvider(credentialsProvider)
                    .overrideConfiguration(clientConfig)
                    .overrideConfiguration(metricPublisher -> metricPublisher.addMetricPublisher(new MicrometerMetricPublisher(awsSdkMetrics)))
                    .build();

            return new ACMCertificateProvider(awsCertificateManager, httpSinkConfiguration.getAcmCertificateArn(),
                    httpSinkConfiguration.getAcmCertIssueTimeOutMillis(), httpSinkConfiguration.getAcmPrivateKeyPassword());
        } else if (httpSinkConfiguration.isSslCertAndKeyFileInS3()) {
            LOG.info("Using S3 to fetch certificate and private key for SSL/TLS.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
            final S3Client s3Client = S3Client.builder()
                    .region(httpSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion())
                    .credentialsProvider(credentialsProvider)
                    .build();
            return new S3CertificateProvider(s3Client,
                    httpSinkConfiguration.getSslCertificateFile(),
                    httpSinkConfiguration.getSslKeyFile());
        } else {
            LOG.info("Using local file system to get certificate and private key for SSL/TLS.");
            return new FileCertificateProvider(httpSinkConfiguration.getSslCertificateFile(), httpSinkConfiguration.getSslKeyFile());
        }
    }
}
