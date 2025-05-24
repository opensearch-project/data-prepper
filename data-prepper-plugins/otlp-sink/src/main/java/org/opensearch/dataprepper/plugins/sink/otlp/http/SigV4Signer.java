/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Helper class to apply AWS SigV4 signing to outgoing HTTP requests
 * before sending them to the AWS OTLP endpoint.
 */
class SigV4Signer {
    private static final String SERVICE_NAME = "xray";
    private static final String OTLP_PATH = "/v1/traces";
    private final Aws4Signer signer = Aws4Signer.create();

    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;
    private final URI endpointUri;

    /**
     * Constructs a SigV4 signer helper.
     *
     * @param awsCredentialsSupplier the AWS credentials supplier
     * @param config Configuration for region and optional STS role
     */
    SigV4Signer(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final OtlpSinkConfig config) {
        this.region = config.getAwsRegion();

        this.credentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(region)
                .withStsRoleArn(config.getStsRoleArn())
                .withStsExternalId(config.getStsExternalId())
                .build());

        this.endpointUri = config.getEndpoint() != null
                ? URI.create(config.getEndpoint())
                : URI.create(String.format("https://xray.%s.amazonaws.com%s", region.id(), OTLP_PATH));
    }

    /**
     * Signs a request payload using AWS SigV4 and returns a fully signed request.
     *
     * @param payload The OTLP Protobuf-encoded request body to be sent
     * @return A signed {@link SdkHttpFullRequest} ready for transmission to the AWS OTLP endpoint
     */
    SdkHttpFullRequest signRequest(@Nonnull final byte[] payload) {
        final SdkHttpFullRequest unsignedRequest = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST)
                .uri(endpointUri)
                .putHeader("Content-Type", "application/x-protobuf")
                .putHeader("Content-Encoding", "gzip")
                .contentStreamProvider(() -> SdkBytes.fromByteArray(payload).asInputStream())
                .build();

        return signer.sign(unsignedRequest, Aws4SignerParams.builder()
                .signingRegion(region)
                .signingName(SERVICE_NAME)
                .awsCredentials(credentialsProvider.resolveCredentials())
                .build());
    }
}
