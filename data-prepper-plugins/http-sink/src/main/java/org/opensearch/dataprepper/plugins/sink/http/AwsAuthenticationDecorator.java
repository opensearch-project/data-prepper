/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.regions.Region;

import javax.annotation.Nonnull;

public class AwsAuthenticationDecorator implements AuthenticationDecorator {
    private static final String SERVICE_NAME = "execute-api";
    private static final String CONTENT_SHA256_HEADER = "x-amz-content-sha256";
    private static final String CONTENT_SHA256_VALUE = "required";

    private final Aws4Signer signer = Aws4Signer.create();
    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;

    public AwsAuthenticationDecorator(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier,
                                      @Nonnull final AwsConfig awsConfig) {
        this.region = awsConfig.getAwsRegion();
        this.credentialsProvider = awsCredentialsSupplier.getProvider(convertToCredentialOptions(awsConfig));
    }

    private static AwsCredentialsOptions convertToCredentialOptions(final AwsConfig awsConfig) {
        return AwsCredentialsOptions.builder()
                .withRegion(awsConfig.getAwsRegion())
                .withStsRoleArn(awsConfig.getAwsStsRoleArn())
                .withStsExternalId(awsConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsConfig.getAwsStsHeaderOverrides())
                .build();
    }

    @Override
    public SdkHttpFullRequest authenticate(final SdkHttpFullRequest request) {
        final SdkHttpFullRequest requestWithHeader = request.toBuilder()
                .putHeader(CONTENT_SHA256_HEADER, CONTENT_SHA256_VALUE)
                .build();

        return signer.sign(requestWithHeader, Aws4SignerParams.builder()
                .signingRegion(region)
                .signingName(SERVICE_NAME)
                .awsCredentials(credentialsProvider.resolveCredentials())
                .build());
    }
}
