/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class GzipCompressorTest {

    private OtlpSinkMetrics sinkMetrics;

    @BeforeEach
    void setUp() {
        sinkMetrics = mock(OtlpSinkMetrics.class);
    }

    @Test
    void apply_returnsCompressedPayload() throws IOException {
        byte[] input = "test-payload".getBytes();
        GzipCompressor gzipCompressor = new GzipCompressor(sinkMetrics);
        Optional<byte[]> compressed = gzipCompressor.apply(input);

        assertTrue(compressed.isPresent(), "Expected compressed payload to be present");

        // Validate decompression gives original input
        byte[] decompressed = decompress(compressed.get());
        assertArrayEquals(input, decompressed);
    }

    @Test
    void apply_handlesIOException_andIncrementsErrorMetric() throws IOException {
        GzipCompressor gzipCompressor = spy(new GzipCompressor(sinkMetrics));
        doThrow(new IOException("boom")).when(gzipCompressor).compressInternal(any());

        Optional<byte[]> result = gzipCompressor.apply("payload".getBytes(StandardCharsets.UTF_8));

        assertTrue(result.isEmpty());
        verify(sinkMetrics).incrementErrorsCount();
        verify(sinkMetrics).incrementRejectedSpansCount(1);
    }

    private byte[] decompress(byte[] compressed) throws IOException {
        try (GZIPInputStream gzipStream = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            return gzipStream.readAllBytes();
        }
    }
}