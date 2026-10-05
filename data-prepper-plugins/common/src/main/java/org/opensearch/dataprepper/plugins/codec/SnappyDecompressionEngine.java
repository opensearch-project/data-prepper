/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.xerial.snappy.SnappyInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SnappyDecompressionEngine implements DecompressionEngine {

    private static final int JSON_HEADER_MAX_SIZE = 1024;
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Override
    public InputStream createInputStream(final InputStream responseInputStream) throws IOException {
        final byte[] buffer = CompressionMagicDetector.bufferLookAhead(
                responseInputStream, CompressionMagicDetector.SCAN_BUFFER_SIZE);

        if (buffer.length == 0) {
            throw new IOException("Unable to decompress Snappy data: stream is empty");
        }

        final int payloadOffset = detectJsonHeaderEnd(buffer);

        return new SnappyInputStream(
                CompressionMagicDetector.reconstructStream(buffer, payloadOffset, buffer.length, responseInputStream));
    }

    /**
     * Detects if the buffer starts with a JSON object/array header.
     * Returns the offset of the first byte after the header (skipping trailing whitespace),
     * or 0 if no JSON header is detected.
     */
    static int detectJsonHeaderEnd(final byte[] buffer) {
        int i = 0;
        while (i < buffer.length && isWhitespace(buffer[i])) {
            i++;
        }

        if (i >= buffer.length || (buffer[i] != '{' && buffer[i] != '[')) {
            return 0;
        }

        final int parseLimit = Math.min(buffer.length, JSON_HEADER_MAX_SIZE);
        final int jsonEnd = parseJsonEnd(buffer, i, parseLimit);

        if (jsonEnd < 0) {
            // Jackson failed to parse — not valid JSON, assume raw Snappy from offset 0
            return 0;
        }

        // Skip trailing whitespace/newline after JSON
        int offset = jsonEnd;
        while (offset < buffer.length && isWhitespace(buffer[offset])) {
            offset++;
        }

        return offset;
    }

    /**
     * Uses Jackson to parse a JSON value starting at the given offset.
     * Returns the byte offset immediately after the JSON value ends, or -1 if parsing fails.
     */
    private static int parseJsonEnd(final byte[] buffer, final int startOffset, final int limit) {
        try (JsonParser parser = JSON_FACTORY.createParser(
                new ByteArrayInputStream(buffer, startOffset, limit - startOffset))) {
            parser.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);

            final JsonToken firstToken = parser.nextToken();
            if (firstToken != JsonToken.START_OBJECT && firstToken != JsonToken.START_ARRAY) {
                return -1;
            }

            // Skip the entire JSON structure
            parser.skipChildren();

            // getTokenLocation gives the start of the current (END) token,
            // we need the position after it — use currentLocation after reading past it
            final long endOffset = parser.currentLocation().getByteOffset();
            return startOffset + (int) endOffset;
        } catch (IOException e) {
            return -1;
        }
    }

    private static boolean isWhitespace(final byte b) {
        return b == ' ' || b == '\n' || b == '\r' || b == '\t';
    }
}
