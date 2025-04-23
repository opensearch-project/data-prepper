/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

class GzipCompressor implements Function<byte[], Optional<byte[]>> {
    private static final Logger LOG = LoggerFactory.getLogger(GzipCompressor.class);
    private final OtlpSinkMetrics sinkMetrics;

    /**
     * Constructor for the GzipCompressor.
     *
     * @param sinkMetrics The metrics for the OTLP sink plugin.
     */
    GzipCompressor(final OtlpSinkMetrics sinkMetrics) {
        this.sinkMetrics = sinkMetrics;
    }

    /**
     * Compresses the provided payload using GZIP compression.
     * Logs an error if compression fails.
     *
     * @param payload The payload to be compressed.
     * @return Optional containing the compressed payload, or empty if compression failed.
     */
    @Override
    public Optional<byte[]> apply(final byte[] payload) {
        try {
            return Optional.of(compressInternal(payload));
        } catch (final IOException e) {
            LOG.error("Failed to compress payload", e);
            sinkMetrics.incrementErrorsCount();
            return Optional.empty();
        }
    }

    /**
     * Internal method to enable mocked-testing.
     */
    @VisibleForTesting
    byte[] compressInternal(final byte[] payload) throws IOException {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
             final GZIPOutputStream gzip = new GZIPOutputStream(out)) {

            gzip.write(payload);
            gzip.finish();
            return out.toByteArray();
        }
    }
}
