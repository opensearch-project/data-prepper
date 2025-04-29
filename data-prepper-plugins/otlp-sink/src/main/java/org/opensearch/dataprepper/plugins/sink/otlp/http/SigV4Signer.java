/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Helper class to apply AWS SigV4 signing to outgoing HTTP requests
 * before sending them to the AWS OTLP endpoint.
 */
class SigV4Signer {
    private static final String SERVICE_NAME = "xray";
    private static final String OTLP_PATH = "/v1/traces";
    private static final String OTLP_SINK_SESSION = "otlp-sink-session";
    private final Aws4Signer signer = Aws4Signer.create();

    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;
    private final URI endpointUri;

    /**
     * Constructs a SigV4 signer helper based on the AWS authentication configuration.
     * Supports optional STS role assumption.
     *
     * @param config Configuration for region and optional STS role
     */
    SigV4Signer(@Nonnull final OtlpSinkConfig config) {
        this(config, null);
    }

    /**
     * Package-private constructor for unit testing with mocked STS client.
     */
    @VisibleForTesting
    SigV4Signer(@Nonnull final OtlpSinkConfig config, final StsClient stsClient) {
        this.region = resolveRegion(config.getAwsRegion());
        this.credentialsProvider = initCredentialsProvider(region, config.getStsRoleArn(), config.getStsExternalId(), stsClient);
        this.endpointUri = config.getEndpoint() != null
                ? URI.create(config.getEndpoint())
                : URI.create(String.format("https://xray.%s.amazonaws.com%s", region.id(), OTLP_PATH));

    }

    private static Region resolveRegion(final Region region) {
        return region != null ? region : DefaultAwsRegionProviderChain.builder().build().getRegion();
    }

    private static AwsCredentialsProvider initCredentialsProvider(
            @Nonnull final Region region,
            final String stsRoleArn,
            final String stsExternalId,
            final StsClient stsClient
    ) {
        if (stsRoleArn != null) {
            return StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(r -> {
                        r.roleArn(stsRoleArn);
                        r.roleSessionName(OTLP_SINK_SESSION);
                        if (stsExternalId != null) {
                            r.externalId(stsExternalId);
                        }
                    })
                    .stsClient(stsClient != null ? stsClient : StsClient.builder().region(region).build())
                    .build();
        }

        return DefaultCredentialsProvider.create();
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
                .contentStreamProvider(() -> SdkBytes.fromByteArray(payload).asInputStream())
                .build();

        return signer.sign(unsignedRequest, Aws4SignerParams.builder()
                .signingRegion(region)
                .signingName(SERVICE_NAME)
                .awsCredentials(credentialsProvider.resolveCredentials())
                .build());
    }
}
