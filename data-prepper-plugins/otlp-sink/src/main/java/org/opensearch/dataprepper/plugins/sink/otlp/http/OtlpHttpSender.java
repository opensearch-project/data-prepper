/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.http;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpFullRequest;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Responsible for sending signed OTLP Protobuf requests to OTLP endpoint using OkHttp.
 */
public class OtlpHttpSender implements AutoCloseable {
    private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(429, 502, 503, 504);
    private static final int BASE_RETRY_DELAY_MS = 100;
    private static final Logger LOG = LoggerFactory.getLogger(OtlpHttpSender.class);
    private static final MediaType PROTOBUF = MediaType.get("application/x-protobuf");
    private final SecureRandom random = new SecureRandom();

    private final int maxRetries;
    private final SigV4Signer signer;
    private final OkHttpClient httpClient;
    private final Consumer<Integer> sleeper;
    private final List<Integer> retryDelaysMs;
    private final OtlpSinkMetrics sinkMetrics;
    private final Function<byte[], Optional<byte[]>> gzipCompressor;

    /**
     * Constructor for the OtlpHttpSender.
     * Initializes the signer and HTTP client.
     *
     * @param config The configuration for the OTLP sink plugin.
     * @param sinkMetrics The metrics for the OTLP sink plugin.
     */
    public OtlpHttpSender(@Nonnull final OtlpSinkConfig config, @Nonnull final OtlpSinkMetrics sinkMetrics) {
        this(config, sinkMetrics, new GzipCompressor(sinkMetrics), null, null, null);
    }

    /**
     * Constructor for unit testing with injected dependencies.
     */
    @VisibleForTesting
    OtlpHttpSender(@Nonnull final OtlpSinkConfig config, @Nonnull final OtlpSinkMetrics sinkMetrics, @Nonnull final Function<byte[], Optional<byte[]>> gzipCompressor,
                   final SigV4Signer signer, final OkHttpClient httpClient, final Consumer<Integer> sleeper) {

        this.sinkMetrics = sinkMetrics;
        this.gzipCompressor = gzipCompressor;
        this.signer = signer != null ? signer : new SigV4Signer(config);
        this.sleeper = sleeper != null ? sleeper : new ThreadSleeper();
        this.httpClient = httpClient != null ? httpClient : buildOkHttpClient(config.getFlushTimeoutMillis());

        this.retryDelaysMs = generateExponentialBackoffDelays(config.getMaxRetries());
        this.maxRetries = config.getMaxRetries();
    }

    private static OkHttpClient buildOkHttpClient(final long flushTimeoutMs) {
        final long httpTimeoutMs = Math.min(
                Math.max(flushTimeoutMs * 2, 3_000),
                10_000
        );

        return new OkHttpClient.Builder()
                .callTimeout(httpTimeoutMs, TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * Generates exponential backoff delays with jitter.
     *
     * @param retries Number of retries.
     * @return List of delay durations in milliseconds.
     */
    private static List<Integer> generateExponentialBackoffDelays(final int retries) {
        final List<Integer> delays = new ArrayList<>();
        for (int i = 0; i < retries; i++) {
            // Exponential backoff: 100ms, 200ms, 400ms, ...
            delays.add(BASE_RETRY_DELAY_MS * (1 << i));
        }

        return delays;
    }

    /**
     * Sends the provided OTLP Protobuf payload to the OTLP endpoint.
     * Retries with exponential backoff and jitter on failure.
     *
     * @param payload       The OTLP Protobuf-encoded data to be sent.
     * @throws IOException  when failed to send the payload.
     */
    public void send(@Nonnull final byte[] payload) throws IOException {
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                final Optional<byte[]> compressedPayload = gzipCompressor.apply(payload);
                if (compressedPayload.isEmpty()) {
                    return;
                }

                final SdkHttpFullRequest signedRequest = signer.signRequest(compressedPayload.get());
                final Request.Builder requestBuilder = new Request.Builder()
                        .url(signedRequest.getUri().toString())
                        .post(RequestBody.create(compressedPayload.get(), PROTOBUF))
                        .addHeader("Content-Encoding", "gzip");

                signedRequest.headers().forEach((key, values) -> {
                    for (final String value : values) {
                        requestBuilder.addHeader(key, value);
                    }
                });

                final Request request = requestBuilder.build();

                final long startTime = System.currentTimeMillis();
                try (final Response response = httpClient.newCall(request).execute()) {
                    final long duration = System.currentTimeMillis() - startTime;
                    sinkMetrics.recordHttpLatency(duration);

                    handleResponse(response);

                    sinkMetrics.incrementPayloadSize(payload.length);
                    sinkMetrics.incrementPayloadGzipSize(compressedPayload.get().length);
                    return;
                }
            } catch (final IOException ioException) {
                if (attempt < maxRetries) {
                    final int jitter = random.nextInt(100);
                    final int retryIndex = Math.min(attempt, retryDelaysMs.size() - 1);
                    final int delay = retryDelaysMs.get(retryIndex) + jitter;
                    try {
                        sleeper.accept(delay);
                        sinkMetrics.incrementRetriesCount();
                    } catch (final RuntimeException runtimeException) {
                        throw new IOException("Sender failed to sleep before retrying.", runtimeException);
                    }
                } else {
                    throw new IOException("Max retries reached", ioException);
                }
            }
        }
    }

    /**
     * Handles the OTLP export response.
     * Retries on 429, 502, 503, and 504 per OTEL spec. Logs other errors without retry.
     * See: <a href="https://opentelemetry.io/docs/specs/otlp/#otlphttp-response">OTLP/HTTP Response</a>
     *
     * @param response The HTTP response
     * @throws IOException For retryable errors
     */
    private void handleResponse(@Nonnull final Response response) throws IOException {
        final int status = response.code();
        sinkMetrics.recordResponseCode(status);

        final byte[] responseBytes = response.body() != null
                ? response.body().bytes()
                : null;

        if (status >= 200 && status < 300) {
            handleSuccessfulResponse(responseBytes);
            return;
        }

        final String responseBody = responseBytes != null ? new String(responseBytes, StandardCharsets.UTF_8) : "<no body>";
        if (RETRYABLE_STATUS_CODES.contains(status)) {
            throw new IOException(String.format("Retryable error. Status: %d, Response: %s", status, responseBody));
        }

        LOG.error("Non-retryable error. Status: {}, Response: {}", status, responseBody);
    }

    private void handleSuccessfulResponse(final byte[] responseBytes) {
        if (responseBytes == null || responseBytes.length == 0) {
            return;
        }

        try {
            final ExportTraceServiceResponse otlpResponse = ExportTraceServiceResponse.parseFrom(responseBytes);

            if (otlpResponse.hasPartialSuccess()) {
                final var partial = otlpResponse.getPartialSuccess();
                final long rejectedSpans = partial.getRejectedSpans();
                final String errorMessage = partial.getErrorMessage();
                if (rejectedSpans > 0 || !errorMessage.isEmpty()) {
                    LOG.error("OTLP Partial Success: rejectedSpans={}, message={}", rejectedSpans, errorMessage);
                    sinkMetrics.incrementRejectedSpansCount(rejectedSpans);
                    sinkMetrics.incrementErrorsCount();
                }
            }
        } catch (final Exception e) {
            LOG.error("Could not parse OTLP response as ExportTraceServiceResponse: {}", e.getMessage());
            sinkMetrics.incrementErrorsCount();
        }
    }

    @Override
    public void close() {
        httpClient.connectionPool().evictAll();
        httpClient.dispatcher().executorService().shutdown();
    }
}