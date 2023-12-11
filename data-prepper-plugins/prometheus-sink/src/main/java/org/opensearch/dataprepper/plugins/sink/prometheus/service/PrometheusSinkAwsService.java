/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsRequestSigningApacheInterceptor;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;

public class PrometheusSinkAwsService {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkAwsService.class);
    public static final String AWS_SIGV4 = "aws_sigv4";
    private static final String AOS_SERVICE_NAME = "aps";

    public static void  attachSigV4(final PrometheusSinkConfiguration prometheusSinkConfiguration, final HttpClientBuilder httpClientBuilder, final AwsCredentialsSupplier awsCredentialsSupplier) {
        LOG.info("{} is set, will sign requests using AWSRequestSigningApacheInterceptor", AWS_SIGV4);
        final Aws4Signer aws4Signer = Aws4Signer.create();
        final AwsCredentialsOptions awsCredentialsOptions = createAwsCredentialsOptions(prometheusSinkConfiguration);
        final AwsCredentialsProvider credentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        httpClientBuilder.addRequestInterceptorLast(new AwsRequestSigningApacheInterceptor(AOS_SERVICE_NAME, aws4Signer,
                credentialsProvider, prometheusSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion()));
    }

    private static AwsCredentialsOptions createAwsCredentialsOptions(final PrometheusSinkConfiguration prometheusSinkConfiguration) {
        return AwsCredentialsOptions.builder()
                .withStsRoleArn(prometheusSinkConfiguration.getAwsAuthenticationOptions().getAwsStsRoleArn())
                .withStsExternalId(prometheusSinkConfiguration.getAwsAuthenticationOptions().getAwsStsExternalId())
                .withRegion(prometheusSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion())
                .withStsHeaderOverrides(prometheusSinkConfiguration.getAwsAuthenticationOptions().getAwsStsHeaderOverrides())
                .build();
    }
}
