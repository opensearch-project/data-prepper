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
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

        final byte[] compressed = gzipCompressor.apply(input);

        // Validate decompression gives original input
        assertNotNull(compressed);
        final byte[] decompressed = decompress(compressed);
        assertArrayEquals(input, decompressed);
    }

    @Test
    void apply_handlesIOException_andIncrementsErrorMetric() throws IOException {
        GzipCompressor gzipCompressor = spy(new GzipCompressor(sinkMetrics));
        doThrow(new IOException("boom")).when(gzipCompressor).compressInternal(any());

        final byte[] result = gzipCompressor.apply("payload".getBytes(StandardCharsets.UTF_8));

        assertEquals(0, result.length);
        verify(sinkMetrics).incrementErrorsCount();
    }

    private byte[] decompress(byte[] compressed) throws IOException {
        try (GZIPInputStream gzipStream = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            return gzipStream.readAllBytes();
        }
    }
}