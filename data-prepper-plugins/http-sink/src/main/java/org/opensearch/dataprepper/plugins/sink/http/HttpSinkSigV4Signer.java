/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.regions.Region;

import javax.annotation.Nonnull;
import java.net.URI;

class HttpSinkSigV4Signer {
    private static final String SERVICE_NAME = "execute-api";
    private final Aws4Signer signer = Aws4Signer.create();
    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;
    private final URI endpointUri;

    HttpSinkSigV4Signer(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final HttpSinkConfiguration config) {
        this.region = config.getAwsConfig().getAwsRegion();
        this.credentialsProvider = awsCredentialsSupplier.getProvider(convertToCredentialOptions(config.getAwsConfig()));
        this.endpointUri = URI.create(config.getUrl());
    }

    private static AwsCredentialsOptions convertToCredentialOptions(final AwsConfig awsConfig) {
        if (awsConfig == null) {
            return AwsCredentialsOptions.builder().build();
        }
        return AwsCredentialsOptions.builder()
                .withRegion(awsConfig.getAwsRegion())
                .withStsRoleArn(awsConfig.getAwsStsRoleArn())
                .withStsExternalId(awsConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsConfig.getAwsStsHeaderOverrides())
                .build();
    }

    SdkHttpFullRequest signRequest(final SdkHttpFullRequest unsignedRequest) {
        if (credentialsProvider == null || credentialsProvider.resolveCredentials() == null) {
            return null;
        }
        return signer.sign(unsignedRequest, Aws4SignerParams.builder()
                .signingRegion(region)
                .signingName(SERVICE_NAME)
                .awsCredentials(credentialsProvider.resolveCredentials())
                .build());
    }
}
