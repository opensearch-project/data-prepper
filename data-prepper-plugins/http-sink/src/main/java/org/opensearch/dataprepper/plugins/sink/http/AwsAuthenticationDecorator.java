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

import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class AwsAuthenticationDecorator implements AuthenticationDecorator {
    private static final String CONTENT_SHA256_HEADER = "x-amz-content-sha256";
    private static final String CONTENT_SHA256_VALUE = "required";

    private final Aws4Signer signer = Aws4Signer.create();
    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;
    private final String serviceName;

    public AwsAuthenticationDecorator(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier,
                                      @Nonnull final AwsConfig awsConfig,
                                      @Nonnull final String serviceName) {
        this.region = awsConfig.getAwsRegion();
        this.serviceName = serviceName;
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
    public HttpRequest buildRequest(final String url, final byte[] payload, final Map<String, List<String>> customHeaders) {
        final SdkHttpFullRequest sdkRequest = createSdkHttpRequest(url, payload, customHeaders);
        final SdkHttpFullRequest signedRequest = sign(sdkRequest);
        return toArmeriaRequest(signedRequest, payload);
    }

    private SdkHttpFullRequest createSdkHttpRequest(final String url, final byte[] payload,
                                                     final Map<String, List<String>> customHeaders) {
        final SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST)
                .uri(URI.create(url))
                .contentStreamProvider(() -> SdkBytes.fromByteArray(payload).asInputStream());

        if (customHeaders != null) {
            customHeaders.forEach((key, values) ->
                    values.forEach(value -> builder.appendHeader(key, value))
            );
        }
        return builder.build();
    }

    private SdkHttpFullRequest sign(final SdkHttpFullRequest request) {
        final SdkHttpFullRequest requestWithHeader = request.toBuilder()
                .putHeader(CONTENT_SHA256_HEADER, CONTENT_SHA256_VALUE)
                .build();

        return signer.sign(requestWithHeader, Aws4SignerParams.builder()
                .signingRegion(region)
                .signingName(serviceName)
                .awsCredentials(credentialsProvider.resolveCredentials())
                .build());
    }

    private static HttpRequest toArmeriaRequest(final SdkHttpFullRequest sdkRequest, final byte[] payload) {
        final RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .scheme(sdkRequest.getUri().getScheme())
                .path(sdkRequest.getUri().getRawPath())
                .authority(sdkRequest.getUri().getAuthority());

        sdkRequest.headers().forEach((k, vList) ->
                vList.forEach(v -> headersBuilder.add(k, v))
        );

        return HttpRequest.of(headersBuilder.build(), HttpData.wrap(payload));
    }
}
