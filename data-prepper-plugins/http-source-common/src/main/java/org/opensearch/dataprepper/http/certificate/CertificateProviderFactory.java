/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http.certificate;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import org.opensearch.dataprepper.plugins.metricpublisher.MicrometerMetricPublisher;
import org.opensearch.dataprepper.http.HttpServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.acm.AcmClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;

public class CertificateProviderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CertificateProviderFactory.class);

    private static final int ACM_CLIENT_RETRIES = 10;
    private static final long ACM_CLIENT_BASE_BACKOFF_MILLIS = 1000l;
    private static final long ACM_CLIENT_MAX_BACKOFF_MILLIS = 60000l;

    final HttpServerConfig httpServerConfig;
    public CertificateProviderFactory(final HttpServerConfig httpServerConfig) {
        this.httpServerConfig = httpServerConfig;
    }

    public CertificateProvider getCertificateProvider() {
        if (httpServerConfig.isUseAcmCertificateForSsl()) {
            LOG.info("Using ACM certificate and private key for SSL/TLS.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create())
                    .build();

            final BackoffStrategy backoffStrategy = EqualJitterBackoffStrategy.builder()
                    .baseDelay(Duration.ofMillis(ACM_CLIENT_BASE_BACKOFF_MILLIS))
                    .maxBackoffTime(Duration.ofMillis(ACM_CLIENT_MAX_BACKOFF_MILLIS))
                    .build();
            final RetryPolicy retryPolicy = RetryPolicy.builder()
                    .numRetries(ACM_CLIENT_RETRIES)
                    .retryCondition(RetryCondition.defaultRetryCondition())
                    .backoffStrategy(backoffStrategy)
                    .throttlingBackoffStrategy(backoffStrategy)
                    .build();
            final ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(retryPolicy)
                    .build();

            final PluginMetrics awsSdkMetrics = PluginMetrics.fromNames("sdk", "aws");

            final AcmClient awsCertificateManager = AcmClient.builder()
                    .region(Region.of(httpServerConfig.getAwsRegion()))
                    .credentialsProvider(credentialsProvider)
                    .overrideConfiguration(clientConfig)
                    .httpClientBuilder(ApacheHttpClient.builder())
                    .overrideConfiguration(metricPublisher -> metricPublisher.addMetricPublisher(new MicrometerMetricPublisher(awsSdkMetrics)))
                    .build();

            return new ACMCertificateProvider(awsCertificateManager,
                    httpServerConfig.getAcmCertificateArn(),
                    httpServerConfig.getAcmCertificateTimeoutMillis(),
                    httpServerConfig.getAcmPrivateKeyPassword());
        } else if (httpServerConfig.isSslCertAndKeyFileInS3()) {
            LOG.info("Using S3 to fetch certificate and private key for SSL/TLS.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create()).build();

            final S3Client s3Client = S3Client.builder()
                    .region(Region.of(httpServerConfig.getAwsRegion()))
                    .credentialsProvider(credentialsProvider)
                    .httpClientBuilder(ApacheHttpClient.builder())
                    .build();

            return new S3CertificateProvider(
                    s3Client,
                    httpServerConfig.getSslCertificateFile(),
                    httpServerConfig.getSslKeyFile()
            );
        } else {
            LOG.info("Using local file system to get certificate and private key for SSL/TLS.");
            return new FileCertificateProvider(
                    httpServerConfig.getSslCertificateFile(),
                    httpServerConfig.getSslKeyFile()
            );
        }
    }
}
