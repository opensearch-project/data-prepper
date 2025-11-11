 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */
package org.opensearch.dataprepper.plugins.sink.prometheus;

import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import javax.annotation.Nonnull;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Helper class to apply AWS SigV4 signing to outgoing HTTP requests
 * before sending them to the AWS OTLP endpoint.
 */
class PrometheusSigV4Signer {
    private static final String SERVICE_NAME = "aps";
    private final Aws4Signer signer = Aws4Signer.create();

    private final AwsCredentialsProvider credentialsProvider;
    private final PrometheusSinkConfiguration config;
    private final Region region;
    private final URI endpointUri;

    PrometheusSigV4Signer(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final PrometheusSinkConfiguration config, final String url) {
        this.region = config.getAwsConfig().getAwsRegion();

        this.config = config;
        this.credentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(region)
                .withStsRoleArn(config.getAwsConfig().getAwsStsRoleArn())
                .withStsExternalId(config.getAwsConfig().getAwsStsExternalId())
                .build());

        this.endpointUri = URI.create(url);
    }

    /**
     * Constructs a SigV4 signer helper.
     *
     * @param awsCredentialsSupplier the AWS credentials supplier
     * @param config Configuration for region and optional STS role
     */
    PrometheusSigV4Signer(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final PrometheusSinkConfiguration config) {

        this(awsCredentialsSupplier, config, config.getUrl());
    }

    SdkHttpFullRequest signQueryRequest(final String query) {
        SdkHttpFullRequest unsignedRequest = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST)
                .uri(endpointUri)
                .putHeader("Content-type", "application/x-www-form-urlencoded")
                .putHeader("x-amz-content-sha256","required")
                .contentStreamProvider(() -> SdkBytes.fromString(query, StandardCharsets.US_ASCII).asInputStream())
                .build();
        
        
        return signer.sign(unsignedRequest, Aws4SignerParams.builder()
                .signingRegion(region)
                .signingName(SERVICE_NAME)
                .awsCredentials(credentialsProvider.resolveCredentials())
                .build());
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
                .putHeader("Content-Encoding", config.getEncoding().toString())
                .putHeader("Content-Type", config.getContentType())
                .putHeader("X-Prometheus-Remote-Write-Version", config.getRemoteWriteVersion())
                .putHeader("x-amz-content-sha256","required")
                .contentStreamProvider(() -> SdkBytes.fromByteArray(payload).asInputStream())
                .build();

        return signer.sign(unsignedRequest, Aws4SignerParams.builder()
                .signingRegion(region)
                .signingName(SERVICE_NAME)
                .awsCredentials(credentialsProvider.resolveCredentials())
                .build());
    }
}
