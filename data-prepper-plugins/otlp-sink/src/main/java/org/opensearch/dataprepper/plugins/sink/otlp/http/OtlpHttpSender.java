/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.otlp.OtlpSignalHandler;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.utils.Pair;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Responsible for sending signed OTLP Protobuf requests to OTLP endpoint using an Armeria client.
 * Supports traces, metrics, and logs.
 */
public class OtlpHttpSender {
    private static final Logger LOG = LoggerFactory.getLogger(OtlpHttpSender.class);
    private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(429, 502, 503, 504);

    private final SigV4Signer signer;
    private final WebClient webClient;
    private final OtlpSinkMetrics sinkMetrics;
    private final Function<byte[], byte[]> gzipCompressor;

    /**
     * Constructor for the OtlpHttpSender.
     *
     * @param awsCredentialsSupplier the AWS credentials supplier
     * @param config The configuration for the OTLP sink plugin.
     * @param sinkMetrics The metrics for the OTLP sink plugin.
     */
    public OtlpHttpSender(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final OtlpSinkConfig config, @Nonnull final OtlpSinkMetrics sinkMetrics) {
        this(sinkMetrics, new GzipCompressor(sinkMetrics), new SigV4Signer(awsCredentialsSupplier, config), buildWebClient(config));
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
     * @param batch the batch of events to send
     * @param signalHandler the signal handler for building the request and parsing the response
     */
    public void send(@Nonnull final List<Pair<Object, EventHandle>> batch, @Nonnull final OtlpSignalHandler signalHandler) {
        if (batch.isEmpty()) {
            return;
        }

        // Defensive copy to avoid ConcurrentModificationException
        final List<Pair<Object, EventHandle>> immutableBatch = List.copyOf(batch);

        final Pair<byte[], byte[]> payloadAndCompressedPayload = getPayloadAndCompressedPayload(immutableBatch, signalHandler);
        final int events = immutableBatch.size();
        if (payloadAndCompressedPayload.right().length == 0) {
            sinkMetrics.incrementFailedRecordsCount(events);
            releaseAllEventHandle(immutableBatch, false);
            return;
        }

        final HttpRequest request = buildHttpRequest(payloadAndCompressedPayload.right());
        final long startTime = System.currentTimeMillis();

        webClient.execute(request)
                .aggregate()
                .thenAccept(response -> {
                    final long latency = System.currentTimeMillis() - startTime;
                    sinkMetrics.recordHttpLatency(latency);
                    sinkMetrics.incrementPayloadSize(payloadAndCompressedPayload.left().length);
                    sinkMetrics.incrementPayloadGzipSize(payloadAndCompressedPayload.right().length);

                    final int statusCode = response.status().code();
                    final byte[] responseBytes = response.content().array();
                    handleResponse(statusCode, responseBytes, immutableBatch, signalHandler);
                })
                .exceptionally(e -> {
                    LOG.error("Failed to send {} events.", events, e);
                    sinkMetrics.incrementRejectedRecordsCount(events);
                    releaseAllEventHandle(immutableBatch, false);
                    return null;
                });
    }

    private Pair<byte[], byte[]> getPayloadAndCompressedPayload(final List<Pair<Object, EventHandle>> batch, final OtlpSignalHandler signalHandler) {
        final byte[] payload = signalHandler.buildRequestPayload(batch);
        final byte[] compressedPayload = gzipCompressor.apply(payload);
        return Pair.of(payload, compressedPayload);
    }

    private HttpRequest buildHttpRequest(final byte[] compressedPayload) {
        final SdkHttpFullRequest signedRequest = signer.signRequest(compressedPayload);

        final RequestHeadersBuilder headersBuilder = RequestHeaders.builder()
                .method(HttpMethod.POST)
                .scheme(signedRequest.getUri().getScheme())
                .path(signedRequest.getUri().getRawPath())
                .authority(signedRequest.getUri().getAuthority());

        // ONLY use the signed headers
        signedRequest.headers().forEach((k, vList) -> vList.forEach(v -> headersBuilder.add(k, v)));
        return HttpRequest.of(headersBuilder.build(), HttpData.wrap(compressedPayload));
    }

    private void handleResponse(final int statusCode, final byte[] responseBytes, final List<Pair<Object, EventHandle>> batch, final OtlpSignalHandler signalHandler) {
        sinkMetrics.recordResponseCode(statusCode);

        if (statusCode >= 200 && statusCode < 300) {
            handleSuccessfulResponse(responseBytes, batch, signalHandler);
            return;
        }

        final String responseBody = responseBytes != null
                ? new String(responseBytes, StandardCharsets.UTF_8)
                : "<no body>";

        LOG.error("Non-successful OTLP response. Status: {}, Response: {}", statusCode, responseBody);
        sinkMetrics.incrementRejectedRecordsCount(batch.size());
        releaseAllEventHandle(batch, false);
    }

    /**
     * Handles a successful OTLP response with partial success.
     */
    private void handleSuccessfulResponse(final byte[] responseBytes, final List<Pair<Object, EventHandle>> batch, final OtlpSignalHandler signalHandler) {
        final int events = batch.size();
        if (responseBytes == null) {
            sinkMetrics.incrementRecordsOut(events);
            releaseAllEventHandle(batch, true);
            return;
        }

        try {
            final Pair<Long, String> partialSuccess = signalHandler.parsePartialSuccess(responseBytes);
            final long rejectedCount = partialSuccess.left();
            final String errorMessage = partialSuccess.right();

            if (rejectedCount > 0) {
                LOG.error("OTLP Partial Success: rejected={}, message={}", rejectedCount, errorMessage);
                sinkMetrics.incrementRejectedRecordsCount(rejectedCount);
            }

            final long deliveredEvents = events - rejectedCount;
            sinkMetrics.incrementRecordsOut(deliveredEvents);

            // Optimistically release all as true, no per-event granularity
            releaseAllEventHandle(batch, true);
        } catch (final Exception e) {
            LOG.error("Could not parse OTLP response: {}", e.getMessage());
            sinkMetrics.incrementErrorsCount();
            sinkMetrics.incrementRecordsOut(events);
            releaseAllEventHandle(batch, true);
        }
    }

    private void releaseAllEventHandle(@Nonnull final List<Pair<Object, EventHandle>> batch, final boolean success) {
        batch.forEach(pair -> pair.right().release(success));
    }
}