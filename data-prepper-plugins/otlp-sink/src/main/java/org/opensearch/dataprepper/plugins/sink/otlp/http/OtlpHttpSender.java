/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.http;

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
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpFullRequest;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.function.Function;

/**
 * Responsible for sending signed OTLP Protobuf requests to OTLP endpoint using an Ameria client.
 */
public class OtlpHttpSender {
    private static final Logger LOG = LoggerFactory.getLogger(OtlpHttpSender.class);
    private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(429, 502, 503, 504);
    private static final MediaType PROTOBUF = MediaType.parse("application/x-protobuf");

    private final SigV4Signer signer;
    private final WebClient webClient;
    private final OtlpSinkMetrics sinkMetrics;
    private final Function<byte[], byte[]> gzipCompressor;

    /**
     * Constructor for the OtlpHttpSender.
     *
     * @param config The configuration for the OTLP sink plugin.
     * @param sinkMetrics The metrics for the OTLP sink plugin.
     */
    public OtlpHttpSender(@Nonnull final OtlpSinkConfig config, @Nonnull final OtlpSinkMetrics sinkMetrics) {
        this(sinkMetrics, new GzipCompressor(sinkMetrics), new SigV4Signer(config), buildWebClient(config));
    }

    /**
     * Constructor for unit testing with injected dependencies.
     */
    @VisibleForTesting
    OtlpHttpSender(@Nonnull final OtlpSinkMetrics sinkMetrics, @Nonnull final Function<byte[], byte[]> gzipCompressor,
                   final SigV4Signer signer, final WebClient webClient) {

        this.sinkMetrics = sinkMetrics;
        this.gzipCompressor = gzipCompressor;
        this.signer = signer;
        this.webClient = webClient;
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

    private static WebClient buildWebClient(final OtlpSinkConfig config) {
        final RetryRuleWithContent<HttpResponse> retryRule = RetryRuleWithContent.<HttpResponse>builder()
                .onStatus((ctx, status) -> RETRYABLE_STATUS_CODES.contains(status.code()))
                .thenBackoff(Backoff.exponential(100, 10_000).withJitter(0.2));

        final long estimatedContentLimit = Math.max(1, config.getMaxBatchSize()) * (config.getMaxRetries() + 1);
        final int safeContentLimit = (int) Math.min(estimatedContentLimit, Integer.MAX_VALUE);

        final RetryingClientBuilder retryingClientBuilder = RetryingClient.builder(retryRule, safeContentLimit)
                .maxTotalAttempts(config.getMaxRetries() + 1);

        final long httpTimeoutMs = Math.min(
                Math.max(config.getFlushTimeoutMillis() * 2, 3_000), 10_000
        );

        return WebClient.builder()
                .decorator(retryingClientBuilder.newDecorator())
                .responseTimeoutMillis(httpTimeoutMs)
                .maxResponseLength(safeContentLimit)
                .build();
    }

    /**
     * Sends the provided OTLP Protobuf payload to the OTLP endpoint asynchronously.
     *
     * @param payload The OTLP Protobuf-encoded data to be sent.
     * @param spans The number of spans in the payload.
     */
    public void send(@Nonnull final byte[] payload, final int spans) {
        final byte[] compressedPayload = gzipCompressor.apply(payload);
        if (compressedPayload.length == 0) {
            sinkMetrics.incrementFailedSpansCount(spans);
            return;
        }

        final HttpRequest request = buildHttpRequest(compressedPayload);

        final long startTime = System.currentTimeMillis();
        webClient.execute(request)
                .aggregate()
                .thenAccept(response -> {
                    final long latency = System.currentTimeMillis() - startTime;
                    sinkMetrics.recordHttpLatency(latency);
                    sinkMetrics.incrementPayloadSize(payload.length);
                    sinkMetrics.incrementPayloadGzipSize(compressedPayload.length);

                    final int statusCode = response.status().code();
                    final byte[] responseBytes = response.content().array();
                    handleResponse(statusCode, responseBytes, spans);
                })
                .exceptionally(e -> {
                    LOG.error("Failed to send {} spans.", spans, e);
                    sinkMetrics.incrementRejectedSpansCount(spans);
                    return null;
                });
    }

    private HttpRequest buildHttpRequest(final byte[] compressedPayload) {
        final SdkHttpFullRequest signedRequest = signer.signRequest(compressedPayload);
        final RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .path(signedRequest.getUri().getPath())
                .contentType(PROTOBUF)
                .add("Content-Encoding", "gzip");

        signedRequest.headers().forEach((k, vList) -> vList.forEach(v -> headersBuilder.add(k, v)));
        return HttpRequest.of(headersBuilder.build(), HttpData.wrap(compressedPayload));
    }

    private void handleResponse(final int statusCode, final byte[] responseBytes, final int spans) {
        sinkMetrics.recordResponseCode(statusCode);

        if (statusCode >= 200 && statusCode < 300) {
            handleSuccessfulResponse(responseBytes, spans);
            return;
        }

        final String responseBody = responseBytes != null
                ? new String(responseBytes, StandardCharsets.UTF_8)
                : "<no body>";

        LOG.error("Non-successful OTLP response. Status: {}, Response: {}", statusCode, responseBody);
        sinkMetrics.incrementRejectedSpansCount(spans);
    }

    /**
     * Handles a successful OTLP response with partial success.
     */
    private void handleSuccessfulResponse(final byte[] responseBytes, final int spans) {
        if (responseBytes == null) {
            sinkMetrics.incrementRecordsOut(spans);
            return;
        }

        try {
            final ExportTraceServiceResponse otlpResponse = ExportTraceServiceResponse.parseFrom(responseBytes);

            if (otlpResponse.hasPartialSuccess()) {
                final var partial = otlpResponse.getPartialSuccess();
                final long rejectedSpans = partial.getRejectedSpans();
                final String errorMessage = partial.getErrorMessage();
                if (rejectedSpans > 0) {
                    LOG.error("OTLP Partial Success: rejectedSpans={}, message={}", rejectedSpans, errorMessage);
                    sinkMetrics.incrementRejectedSpansCount(rejectedSpans);
                }

                final long deliveredSpans = spans - rejectedSpans;
                sinkMetrics.incrementRecordsOut(deliveredSpans);
            } else {
                sinkMetrics.incrementRecordsOut(spans);
            }
        } catch (final Exception e) {
            LOG.error("Could not parse OTLP response as ExportTraceServiceResponse: {}", e.getMessage());
            sinkMetrics.incrementErrorsCount();
            sinkMetrics.incrementRecordsOut(spans);
        }
    }
}