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
import java.util.Set;

/**
 * Responsible for sending signed OTLP Protobuf trace data to AWS OTLP endpoint using OkHttp.
 */
public class OtlpHttpSender implements AutoCloseable {
    @VisibleForTesting
    static final Set<Integer> NON_RETRYABLE_STATUS_CODES = Set.of(400, 401, 403, 422);

    private static final int BASE_RETRY_DELAY_MS = 100;
    private static final Logger LOG = LoggerFactory.getLogger(OtlpHttpSender.class);
    private static final MediaType PROTOBUF = MediaType.get("application/x-protobuf");
    private final SecureRandom random = new SecureRandom();

    private final SigV4Signer signer;
    private final OkHttpClient httpClient;
    private final Sleeper sleeper;
    private final int maxRetries;
    private final List<Integer> retryDelaysMs;
    private final OtlpSinkMetrics sinkMetrics;

    /**
     * Constructor for the OtlpHttpSender.
     * Initializes the signer and HTTP client.
     * @param config The configuration for the OTLP sink plugin.
     */
    public OtlpHttpSender(@Nonnull final OtlpSinkConfig config, @Nonnull final OtlpSinkMetrics sinkMetrics) {
        this(config, sinkMetrics, null, null, null);
    }

    /**
     * Constructor for unit testing with injected dependencies.
     *
     * @param config     The configuration for the OTLP sink plugin.
     * @param signer     The SigV4Signer instance for signing requests.
     * @param httpClient The OkHttpClient instance for making HTTP requests.
     */
    @VisibleForTesting
    OtlpHttpSender(@Nonnull final OtlpSinkConfig config, @Nonnull final OtlpSinkMetrics sinkMetrics, final SigV4Signer signer, final OkHttpClient httpClient, final Sleeper sleeper) {
        this.sinkMetrics = sinkMetrics;
        this.signer = signer != null ? signer : new SigV4Signer(config);
        this.httpClient = httpClient != null ? httpClient : new OkHttpClient();
        this.sleeper = sleeper != null ? sleeper : new ThreadSleeper();

        this.retryDelaysMs = generateExponentialBackoffDelays(config.getMaxRetries());
        this.maxRetries = config.getMaxRetries();
    }

    /**
     * Generates exponential backoff delays with jitter.
     *
     * @param retries Number of retries.
     * @return List of delay durations in milliseconds.
     */
    private List<Integer> generateExponentialBackoffDelays(final int retries) {
        List<Integer> delays = new ArrayList<>();

        for (int i = 0; i < retries; i++) {
            // Exponential backoff: 100ms, 200ms, 400ms, ...
            delays.add(BASE_RETRY_DELAY_MS * (1 << i));
        }

        return delays;
    }

    /**
     * Sends the provided OTLP Protobuf trace data to the OTLP endpoint.
     * Retries with exponential backoff and jitter on failure.
     *
     * @param payload The OTLP Protobuf-encoded trace data to be sent.
     */
    public void send(@Nonnull final byte[] payload) {
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                final SdkHttpFullRequest signedRequest = signer.signRequest(payload);

                final Request.Builder requestBuilder = new Request.Builder()
                        .url(signedRequest.getUri().toString())
                        .post(RequestBody.create(payload, PROTOBUF));

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
                    return;
                }
            } catch (final Exception e) {
                if (attempt < maxRetries) {
                    final int jitter = random.nextInt(100);
                    final int retryIndex = Math.min(attempt, retryDelaysMs.size() - 1);
                    final int delay = retryDelaysMs.get(retryIndex) + jitter;
                    LOG.warn("Retrying after failure in attempt {}. Sleeping {}ms.", attempt + 1, delay, e);
                    sinkMetrics.incrementRetriesCount();
                    try {
                        sleeper.sleep(delay);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted while sending OTLP data", ie);
                    }
                } else {
                    LOG.error("All retry attempts failed while signing or sending OTLP data", e);
                    throw new RuntimeException("Failed to sign/send data after retries", e);
                }
            }
        }
    }

    /**
     * Handles the response from the OTLP endpoint.
     * Logs the response status and body, and throws an exception for retryable errors.
     *
     * @param response The HTTP response from the OTLP endpoint.
     * @throws IOException If the response status is not successful and retryable.
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
        if (NON_RETRYABLE_STATUS_CODES.contains(status)) {
            LOG.error("Non-retryable client error. Status: {}, Response: {}", status, responseBody);
            return;
        }

        final String errorMsg = String.format("Failed to send OTLP data. Status: %d, Response: %s", status, responseBody);
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
    }

    private void handleSuccessfulResponse(final byte[] responseBytes) {
        if (responseBytes == null || responseBytes.length == 0) {
            LOG.info("OTLP export successful. No response body.");
            return;
        }

        try {
            final ExportTraceServiceResponse otlpResponse = ExportTraceServiceResponse.parseFrom(responseBytes);

            if (otlpResponse.hasPartialSuccess()) {
                final var partial = otlpResponse.getPartialSuccess();
                final long rejectedSpans = partial.getRejectedSpans();
                sinkMetrics.incrementRejectedSpansCount(rejectedSpans);

                final String errorMessage = partial.getErrorMessage();

                if (rejectedSpans > 0 || !errorMessage.isEmpty()) {
                    LOG.warn("OTLP Partial Success: rejectedSpans={}, message={}", rejectedSpans, errorMessage);
                } else {
                    LOG.info("OTLP export successful with no rejections.");
                }
            } else {
                LOG.info("OTLP export successful with no partial success field.");
            }
        } catch (final Exception e) {
            LOG.error("Could not parse OTLP response as ExportTraceServiceResponse: {}", e.getMessage());
            sinkMetrics.incrementErrorsCount();
        }
    }

    @Override
    public void close() {
        // No explicit shutdown required for OkHttpClient unless using dispatcher or connection pool tuning.
        // https://square.github.io/okhttp/4.x/okhttp/okhttp3/-ok-http-client/dispatchers/
    }
}