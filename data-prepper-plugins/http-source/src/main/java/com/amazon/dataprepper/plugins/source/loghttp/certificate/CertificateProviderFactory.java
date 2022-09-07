/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loghttp.certificate;

import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import com.amazon.dataprepper.plugins.source.loghttp.HTTPSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.acm.AcmClient;
import software.amazon.awssdk.services.s3.S3Client;

public class CertificateProviderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CertificateProviderFactory.class);

    final HTTPSourceConfig httpSourceConfig;
    public CertificateProviderFactory(final HTTPSourceConfig httpSourceConfig) {
        this.httpSourceConfig = httpSourceConfig;
    }

    public CertificateProvider getCertificateProvider() {
        if (httpSourceConfig.isUseAcmCertificateForSsl()) {
            LOG.info("Using ACM certificate and private key for SSL/TLS.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create())
                    .build();

            final ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryMode.STANDARD)
                    .build();

            final AcmClient awsCertificateManager = AcmClient.builder()
                    .region(Region.of(httpSourceConfig.getAwsRegion()))
                    .credentialsProvider(credentialsProvider)
                    .overrideConfiguration(clientConfig)
                    .httpClientBuilder(ApacheHttpClient.builder())
                    .build();

            return new ACMCertificateProvider(awsCertificateManager,
                    httpSourceConfig.getAcmCertificateArn(),
                    httpSourceConfig.getAcmCertificateTimeoutMillis(),
                    httpSourceConfig.getAcmPrivateKeyPassword());
        } else if (httpSourceConfig.isSslCertAndKeyFileInS3()) {
            LOG.info("Using S3 to fetch certificate and private key for SSL/TLS.");
            final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                    .addCredentialsProvider(DefaultCredentialsProvider.create()).build();

            final S3Client s3Client = S3Client.builder()
                    .region(Region.of(httpSourceConfig.getAwsRegion()))
                    .credentialsProvider(credentialsProvider)
                    .httpClientBuilder(ApacheHttpClient.builder())
                    .build();

            return new S3CertificateProvider(
                    s3Client,
                    httpSourceConfig.getSslCertificateFile(),
                    httpSourceConfig.getSslKeyFile()
            );
        } else {
            LOG.info("Using local file system to get certificate and private key for SSL/TLS.");
            return new FileCertificateProvider(
                    httpSourceConfig.getSslCertificateFile(),
                    httpSourceConfig.getSslKeyFile()
            );
        }
    }
}
