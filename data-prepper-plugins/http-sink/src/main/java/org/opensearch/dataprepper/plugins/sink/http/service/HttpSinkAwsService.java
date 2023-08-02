/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.http.AwsRequestSigningApacheInterceptor;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;

public class HttpSinkAwsService {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkAwsService.class);
    public static final String AWS_SIGV4 = "aws_sigv4";
    private static final String AOS_SERVICE_NAME = "http-endpoint";

    public static void  attachSigV4(final HttpSinkConfiguration httpSinkConfiguration, final HttpClientBuilder httpClientBuilder, final AwsCredentialsSupplier awsCredentialsSupplier) {
        LOG.info("{} is set, will sign requests using AWSRequestSigningApacheInterceptor", AWS_SIGV4);
        final Aws4Signer aws4Signer = Aws4Signer.create();
        final AwsCredentialsOptions awsCredentialsOptions = createAwsCredentialsOptions(httpSinkConfiguration);
        final AwsCredentialsProvider credentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        httpClientBuilder.addRequestInterceptorLast(new AwsRequestSigningApacheInterceptor(AOS_SERVICE_NAME, aws4Signer,
                credentialsProvider, httpSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion()));
    }

    private static AwsCredentialsOptions createAwsCredentialsOptions(final HttpSinkConfiguration httpSinkConfiguration) {
        return AwsCredentialsOptions.builder()
                .withStsRoleArn(httpSinkConfiguration.getAwsAuthenticationOptions().getAwsStsRoleArn())
                .withStsExternalId(httpSinkConfiguration.getAwsAuthenticationOptions().getAwsStsExternalId())
                .withRegion(httpSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion())
                .withStsHeaderOverrides(httpSinkConfiguration.getAwsAuthenticationOptions().getAwsStsHeaderOverrides())
                .build();
    }
}
