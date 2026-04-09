/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientOptions;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Set;

public class HttpSinkSender {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkSender.class);
    private static final String HTTP_METHOD = "post";
    private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(408, 429, 500, 502, 503, 504);
    private static final Set<Integer> AUTH_ERROR_CODES = Set.of(401, 403);

    private final HttpSinkSigV4Signer signer;
    private final WebClient webClient;
    private final HttpSinkConfiguration config;
    private final int maxRetries;
    private final long retryIntervalMs;
    private final SinkMetrics sinkMetrics;

    public HttpSinkSender(final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final HttpSinkConfiguration config, final SinkMetrics sinkMetrics) {
        this.signer = awsCredentialsSupplier != null ? new HttpSinkSigV4Signer(awsCredentialsSupplier, config) : null;
        this.webClient = buildWebClient(config);
        this.config = config;
        this.sinkMetrics = sinkMetrics;
        this.maxRetries = config.getMaxUploadRetries();
        this.retryIntervalMs = config.getHttpRetryInterval().toMillis();
    }

    private static WebClient buildWebClient(final HttpSinkConfiguration config) {
        return WebClient.builder()
                .factory(ClientFactory.builder()
                        .connectTimeout(config.getConnectionTimeout())
                        .build())
                .options(ClientOptions.builder().build())
                .build();
    }

    public HttpEndPointResponse send(final byte[] payload) {
        HttpEndPointResponse response = null;
        int attempt = 0;
        
        while (attempt <= maxRetries) {
            try {
                final HttpRequest request = buildHttpRequest(payload);
                if (request == null) {
                    return new HttpEndPointResponse(config.getUrl(), 0, "Failed to build request");
                }

                response = webClient.execute(request)
                        .aggregate()
                        .thenApply(resp -> {
                            int statusCode = resp.status().code();
                            String responseBody = resp.content().toStringUtf8();
                            return new HttpEndPointResponse(config.getUrl(), statusCode, responseBody);
                        })
                        .exceptionally(throwable -> {
                            LOG.error("Request failed", throwable);
                            return new HttpEndPointResponse(config.getUrl(), 0, throwable.getMessage());
                        })
                        .join();

                // Success - no retry needed
                if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                    return response;
                }

                // Auth errors - do not retry
                if (AUTH_ERROR_CODES.contains(response.getStatusCode())) {
                    LOG.error("Authentication error ({}), not retrying", response.getStatusCode());
                    return response;
                }

                // Non-retryable error - do not retry
                if (!RETRYABLE_STATUS_CODES.contains(response.getStatusCode())) {
                    LOG.error("Non-retryable error ({}), message({}) not retrying", response.getStatusCode(), response.getErrMessage());
                    return response;
                }

                // Retryable error - retry if attempts remain
                if (attempt < maxRetries) {
                    LOG.warn("Retryable error ({}), attempt {}/{}, retrying after {}ms", 
                            response.getStatusCode(), attempt + 1, maxRetries, retryIntervalMs);
                    Thread.sleep(retryIntervalMs);
                    sinkMetrics.incrementRetries(1);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Retry interrupted", e);
                return new HttpEndPointResponse(config.getUrl(), 0, "Retry interrupted: " + e.getMessage());
            } catch (Exception e) {
                LOG.error("Failed to execute request, attempt {}/{}", attempt + 1, maxRetries + 1, e);
                if (attempt >= maxRetries) {
                    return new HttpEndPointResponse(config.getUrl(), 0, e.getMessage());
                }
                try {
                    Thread.sleep(retryIntervalMs);
                    sinkMetrics.incrementRetries(1);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return new HttpEndPointResponse(config.getUrl(), 0, "Retry interrupted: " + ie.getMessage());
                }
            }
            attempt++;
        }
        
        return response != null ? response : new HttpEndPointResponse(config.getUrl(), 0, "Max retries exceeded");
    }

    private SdkHttpFullRequest createSdkHttpRequest(final String url, @Nonnull final byte[] payload) {
        final SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST)
                .uri(URI.create(url))
                .contentStreamProvider(() -> SdkBytes.fromByteArray(payload).asInputStream());

        if (signer != null) {
            builder.putHeader("x-amz-content-sha256", "required");
        }


        if (config.getCustomHeaderOptions() != null) {
            config.getCustomHeaderOptions().forEach((key, values) -> 
                values.forEach(value -> builder.appendHeader(key, value))
            );
        }
        return builder.build();
    }

    private HttpRequest buildHttpRequest(final byte[] payload) {
        SdkHttpFullRequest sdkHttpRequest = createSdkHttpRequest(config.getUrl(), payload);

        if (signer != null) {
            sdkHttpRequest = signer.signRequest(sdkHttpRequest);
            if (sdkHttpRequest == null) {
                return null;
            }
        }

        final RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .scheme(sdkHttpRequest.getUri().getScheme())
                .path(sdkHttpRequest.getUri().getRawPath())
                .authority(sdkHttpRequest.getUri().getAuthority());

        sdkHttpRequest.headers().forEach((k, vList) -> {
                vList.forEach(v -> headersBuilder.add(k, v));
            }
        );

        return HttpRequest.of(headersBuilder.build(), HttpData.wrap(payload));
    }
}
