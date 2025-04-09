/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.xrayotlp.http;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.plugins.sink.xrayotlp.configuration.XRayOTLPSinkConfig;
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
import java.net.URI;

/**
 * Helper class to apply AWS SigV4 signing to outgoing HTTP requests
 * before sending them to the AWS X-Ray OTLP endpoint.
 */
public class SigV4Signer {
    private static final String SERVICE_NAME = "xray";
    private static final String OTLP_PATH = "/v1/traces";
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
    public SigV4Signer(final XRayOTLPSinkConfig config) {
        this(config, null);
    }

    /**
     * Package-private constructor for unit testing with mocked STS client.
     */
    @VisibleForTesting
    SigV4Signer(final XRayOTLPSinkConfig config, final StsClient stsClient) {
        this.region = resolveRegion(config.getAwsRegion());
        this.credentialsProvider = initCredentialsProvider(config.getAwsRegion(), config.getStsRoleArn(), config.getStsExternalId(), stsClient);
        this.endpointUri = URI.create(String.format("https://xray.%s.amazonaws.com%s", region.id(), OTLP_PATH));
    }

    private Region resolveRegion(final Region region) {
        return region != null ? region : DefaultAwsRegionProviderChain.builder().build().getRegion();
    }

    private AwsCredentialsProvider initCredentialsProvider(
            final Region region,
            final String stsRoleArn,
            final String stsExternalId,
            final StsClient stsClient
    ) {
        if (region != null && stsRoleArn != null) {
            return StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(r -> {
                        r.roleArn(stsRoleArn);
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
    public SdkHttpFullRequest signRequest(final byte[] payload) {
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
