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
import com.google.common.annotations.VisibleForTesting;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.retry.Backoff;
import com.linecorp.armeria.client.retry.RetryRuleWithContent;
import com.linecorp.armeria.client.retry.RetryingClient;
import com.linecorp.armeria.client.retry.RetryingClientBuilder;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpFullRequest;

import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.model.codec.CompressionEngine;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientOptions;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Responsible for sending signed OTLP Protobuf requests to OTLP endpoint using an Ameria client.
 */
public class PrometheusHttpSender {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusHttpSender.class);
    private static final int DEFAULT_MAX_REQUEST_SIZE = 1024*1024; // 1MB 
    private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(429, 502, 503, 504);

    private final PrometheusSigV4Signer signer;
    private final WebClient webClient;
    private final SinkMetrics sinkMetrics;
    private final long connectionTimeoutMillis;
    private final long idleTimeoutMillis;
    private final CompressionEngine compressionEngine;

    /**
     * Constructor for the PrometheusHttpSender.
     *
     * @param awsCredentialsSupplier the AWS credentials supplier
     * @param config The configuration for the Prometheus sink plugin.
     */
    public PrometheusHttpSender(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final PrometheusSinkConfiguration config, @Nonnull final SinkMetrics sinkMetrics, final long connectionTimeoutMillis, final long idleTimeoutMillis) {
        this(new PrometheusSigV4Signer(awsCredentialsSupplier, config), config.getEncoding(), buildWebClient(config), sinkMetrics, connectionTimeoutMillis, idleTimeoutMillis);
    }

    /**
     * Constructor for unit testing with injected dependencies.
     */
    @VisibleForTesting
    PrometheusHttpSender(final PrometheusSigV4Signer signer, final CompressionOption compressionOption, final WebClient webClient, final SinkMetrics sinkMetrics, final long connectionTimeoutMillis, final long idleTimeoutMillis) {
        this.signer = signer;
        this.webClient = webClient;
        this.compressionEngine = compressionOption.getCompressionEngine();
        this.sinkMetrics = sinkMetrics;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
    }

    /**
     * Builds a WebClient with retry logic for known OTLP retryable status codes.
     * <p>
     * Retries on 429, 502, 503, and 504 per OTEL spec.
     * See: <a href="https://opentelemetry.io/docs/specs/otlp/#otlphttp-response">OTLP/HTTP Response</a>
     * <p>
     * We are not using the Retry-After header for dynamic backoff because:
     * - Armeria’s retry rule API expects a boolean decision or fixed Backoff.
     * - Applying Retry-After semantics would require a custom Backoff implementation,
     * adding complexity with minimal benefit for most OTLP endpoints.
     * - Our exponential backoff already handles typical retry intervals gracefully.
     */
    private static WebClient buildWebClient(final PrometheusSinkConfiguration config) {
        final RetryRuleWithContent<HttpResponse> retryRule = RetryRuleWithContent.<HttpResponse>builder()
                .onStatus((ctx, status) -> RETRYABLE_STATUS_CODES.contains(status.code()))
                .thenBackoff(Backoff.exponential(100, 10_000).withJitter(0.2));

        final long estimatedContentLimit = Math.max(1, DEFAULT_MAX_REQUEST_SIZE) * (config.getMaxRetries() + 1);
        final int safeContentLimit = (int) Math.min(estimatedContentLimit, Integer.MAX_VALUE);

        final RetryingClientBuilder retryingClientBuilder = RetryingClient.builder(retryRule, safeContentLimit)
                .maxTotalAttempts(config.getMaxRetries() + 1);

        return WebClient.builder()
                .factory(ClientFactory.builder()
                    .connectTimeout(config.getConnectionTimeout())
                    .idleTimeout(config.getIdleTimeout())
                    .build())
                .decorator(retryingClientBuilder.newDecorator())
                .responseTimeoutMillis(config.getRequestTimeout().toMillis())
                .maxResponseLength(safeContentLimit)
                .options(ClientOptions.builder().build())
                .build();
    }

    /**
     * Sends the provided OTLP Protobuf payload to the OTLP endpoint asynchronously.
     *
     * @param payload - batch the batch of spans to send
     */
    public PrometheusPushResult pushToEndpoint(final byte[] payload) {
        PrometheusPushResult result;
        try {
            final byte[] compressedBufferData = compressionEngine.compress(payload);

            final HttpRequest request = buildHttpRequest(compressedBufferData);
            final long startTime = System.currentTimeMillis();
            
            // Execute request and wait for completion
            result = webClient.execute(request)
                .aggregate()
                .thenApply(response -> {
                    final long latency = System.currentTimeMillis() - startTime;
                    sinkMetrics.recordRequestSize(compressedBufferData.length);
                    LOG.error("Response received in {}ms. Status: {}", latency, response.status());
                    
                    int statusCode = response.status().code();
                    final byte[] responseBytes = response.content().array();
                    return new PrometheusPushResult(handleResponse(statusCode, responseBytes), statusCode);
                })
                .exceptionally(throwable -> {
                    LOG.error("Request failed", throwable);
                    return new PrometheusPushResult(false, 0);
                })
                .join();  // Wait for completion
        } catch (Exception e) {
            LOG.error("Failed to execute request", e);
            result = new PrometheusPushResult(false, 0);
        }
        return result;
    }

    private HttpRequest buildHttpRequest(final byte[] payload) {
        final SdkHttpFullRequest signedRequest = signer.signRequest(payload);
        
        final RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .scheme(signedRequest.getUri().getScheme())
                .path(signedRequest.getUri().getRawPath())
                .authority(signedRequest.getUri().getAuthority());

        // Preserve all original headers from the signed request without modification
        signedRequest.headers().forEach((k, vList) -> {
            // Add each header value individually to preserve exact format
            vList.forEach(v -> {
                LOG.debug("Adding header [{}] = [{}]", k, v);
                headersBuilder.add(k, v);
            });
        });

        // Create request with raw headers and payload
        HttpRequest request = HttpRequest.of(headersBuilder.build(), HttpData.wrap(payload));
        LOG.debug("Final request URI: {}", request.uri());
        LOG.debug("Final request headers: {}", request.headers());
        return request;
    }

    private boolean handleResponse(final int statusCode, final byte[] responseBytes) {
        if (statusCode >= 200 && statusCode < 300) {
            return true;
        }

        final String responseBody = responseBytes != null
                ? new String(responseBytes, StandardCharsets.UTF_8)
                : "<no body>";

        LOG.error("Non-successful Prometheus server response. Status: {}, Response: {}", statusCode, responseBody);
        return false;
    }

}
