/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.formatdetection;

import org.apache.tika.Tika;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Detects data format and compression from raw bytes using Apache Tika.
 *
 * <p>Detection proceeds in two layers:
 * <ol>
 *   <li>Compression: Tika identifies compression wrappers (gzip, zstd, snappy) from magic bytes.
 *       If compressed, the sample is decompressed before format detection.</li>
 *   <li>Format: Tika detects the MIME type of the (decompressed) content, which is mapped to a
 *       {@link DetectedFormat}. Additional text heuristics disambiguate JSON arrays, ND-JSON,
 *       CSV, and TSV, which Tika reports generically.</li>
 * </ol>
 */
public class FormatDetector {
    private static final Logger LOG = LoggerFactory.getLogger(FormatDetector.class);

    private static final int DEFAULT_SAMPLE_SIZE = 65536; // 64 KB
    private static final double NDJSON_LINE_THRESHOLD = 0.8;
    private static final double CSV_CONSISTENCY_THRESHOLD = 0.8;
    private static final int CSV_MIN_LINES = 2;
    private static final int CSV_MAX_SAMPLE_LINES = 10;

    private final Tika tika;

    public FormatDetector() {
        this.tika = new Tika();
    }

    /**
     * Detect format and compression from a byte array sample.
     *
     * @param sample raw bytes from the beginning of the data source
     * @return detection result with compression, format, and confidence
     */
    public FormatDetectionResult detect(final byte[] sample) {
        if (sample == null || sample.length < 4) {
            return new FormatDetectionResult(DetectedCompression.NONE, DetectedFormat.UNKNOWN, Confidence.LOW);
        }

        final DetectedCompression compression = detectCompression(sample);
        final byte[] decompressed = decompress(sample, compression);

        if (decompressed.length < 4) {
            return new FormatDetectionResult(compression, DetectedFormat.UNKNOWN, Confidence.LOW);
        }

        return detectFormat(decompressed, compression);
    }

    /**
     * Detect compression by asking Tika for the MIME type of the raw bytes.
     */
    DetectedCompression detectCompression(final byte[] data) {
        final String mimeType = tikaDetect(data);
        if (mimeType == null) {
            return DetectedCompression.NONE;
        }
        if (mimeType.contains("gzip")) {
            return DetectedCompression.GZIP;
        }
        if (mimeType.contains("zstd") || mimeType.contains("zstandard")) {
            return DetectedCompression.ZSTD;
        }
        if (mimeType.contains("snappy") || mimeType.contains("x-snappy")) {
            return DetectedCompression.SNAPPY;
        }
        return DetectedCompression.NONE;
    }

    private FormatDetectionResult detectFormat(final byte[] decompressed, final DetectedCompression compression) {
        final String mimeType = tikaDetect(decompressed);

        // Binary and document formats — Tika identifies these definitively from magic bytes.
        final DetectedFormat binaryFormat = mapBinaryMimeType(mimeType);
        if (binaryFormat != null) {
            return new FormatDetectionResult(compression, binaryFormat, Confidence.HIGH);
        }

        // Text-based content — refine with heuristics since Tika reports generic text/plain,
        // application/json, etc. without distinguishing JSON array vs ND-JSON vs CSV.
        String text = new String(decompressed, StandardCharsets.UTF_8).trim();
        if (text.startsWith("\uFEFF")) {
            text = text.substring(1);
        }
        if (text.isEmpty()) {
            return new FormatDetectionResult(compression, DetectedFormat.UNKNOWN, Confidence.LOW);
        }

        return detectTextFormat(text, mimeType, compression);
    }

    /**
     * Map Tika binary/document MIME types to {@link DetectedFormat}.
     * Returns null for text-based types that require further heuristics.
     */
    private DetectedFormat mapBinaryMimeType(final String mimeType) {
        if (mimeType == null) {
            return null;
        }
        if (mimeType.contains("parquet")) {
            return DetectedFormat.PARQUET;
        }
        if (mimeType.contains("avro")) {
            return DetectedFormat.AVRO;
        }
        if (mimeType.contains("orc")) {
            return DetectedFormat.ORC;
        }
        if (mimeType.equals("application/pdf")) {
            return DetectedFormat.PDF;
        }
        if (mimeType.startsWith("image/")) {
            return DetectedFormat.IMAGE;
        }
        return null;
    }

    FormatDetectionResult detectTextFormat(final String text, final String mimeType,
                                           final DetectedCompression compression) {
        // XML — trust Tika's MIME detection plus a structural check.
        if ((mimeType != null && mimeType.contains("xml"))
                || text.startsWith("<?xml")
                || (text.startsWith("<") && text.contains(">"))) {
            return new FormatDetectionResult(compression, DetectedFormat.XML, Confidence.HIGH);
        }

        // JSON array vs ND-JSON vs single object.
        if (text.startsWith("{") || text.startsWith("[")) {
            if (isCompleteJson(text)) {
                if (text.startsWith("[")) {
                    return new FormatDetectionResult(compression, DetectedFormat.JSON, Confidence.HIGH);
                }
                // A single JSON object maps to ND-JSON (the json codec only handles arrays).
                return new FormatDetectionResult(compression, DetectedFormat.NDJSON, Confidence.HIGH);
            }
            // Truncated JSON array (sample cut mid-array).
            if (text.startsWith("[") && text.length() > 2 && text.substring(1).trim().startsWith("{")) {
                return new FormatDetectionResult(compression, DetectedFormat.JSON, Confidence.MEDIUM);
            }
            // Truncated single JSON object (single line only).
            if (text.startsWith("{") && text.length() > 10 && !text.contains("\n")
                    && (text.contains("\":") || text.contains("\": "))) {
                return new FormatDetectionResult(compression, DetectedFormat.NDJSON, Confidence.MEDIUM);
            }
        }

        // ND-JSON: most lines parse as JSON objects.
        final String[] lines = text.split("\\r?\\n");
        final String[] nonEmptyLines = filterNonEmpty(lines);
        if (nonEmptyLines.length > 0) {
            final long jsonLineCount = countJsonLines(nonEmptyLines);
            final double ratio = (double) jsonLineCount / nonEmptyLines.length;
            if (ratio >= NDJSON_LINE_THRESHOLD) {
                final Confidence confidence = ratio >= 0.95 ? Confidence.HIGH : Confidence.MEDIUM;
                return new FormatDetectionResult(compression, DetectedFormat.NDJSON, confidence);
            }
        }

        // CSV / TSV: consistent delimiter count across lines.
        if (nonEmptyLines.length >= CSV_MIN_LINES) {
            final DetectedFormat delimiterFormat = detectDelimitedFormat(nonEmptyLines);
            if (delimiterFormat != null) {
                return new FormatDetectionResult(compression, delimiterFormat, Confidence.MEDIUM);
            }
        }

        // Verify it is genuinely readable text before falling back.
        if (!isReadableText(text)) {
            return new FormatDetectionResult(compression, DetectedFormat.UNKNOWN, Confidence.LOW);
        }

        return new FormatDetectionResult(compression, DetectedFormat.TEXT, Confidence.LOW);
    }

    private String tikaDetect(final byte[] data) {
        try {
            final MediaType mediaType = MediaType.parse(tika.detect(data));
            return mediaType == null ? null : mediaType.toString();
        } catch (final Exception e) {
            LOG.debug("Tika detection failed: {}", e.getMessage());
            return null;
        }
    }

    private DetectedFormat detectDelimitedFormat(final String[] lines) {
        if (isConsistentDelimiter(lines, ',')) {
            return DetectedFormat.CSV;
        }
        if (isConsistentDelimiter(lines, '\t')) {
            return DetectedFormat.TSV;
        }
        if (isConsistentDelimiter(lines, '|')) {
            return DetectedFormat.CSV;
        }
        return null;
    }

    private boolean isConsistentDelimiter(final String[] lines, final char delimiter) {
        final int sampleSize = Math.min(lines.length, CSV_MAX_SAMPLE_LINES);
        final long firstLineCount = countDelimiterOutsideQuotes(lines[0], delimiter);
        if (firstLineCount == 0) {
            return false;
        }
        int matches = 0;
        for (int i = 1; i < sampleSize; i++) {
            if (countDelimiterOutsideQuotes(lines[i], delimiter) == firstLineCount) {
                matches++;
            }
        }
        return (double) matches / (sampleSize - 1) >= CSV_CONSISTENCY_THRESHOLD;
    }

    private boolean isCompleteJson(final String text) {
        if (text.startsWith("{") && text.endsWith("}")) {
            return bracketsBalanced(text, '{', '}');
        }
        if (text.startsWith("[") && text.endsWith("]")) {
            return bracketsBalanced(text, '[', ']');
        }
        return false;
    }

    private boolean bracketsBalanced(final String text, final char open, final char close) {
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < text.length(); i++) {
            final char c = text.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\' && inString) {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (inString) {
                continue;
            }
            if (c == open) {
                depth++;
            } else if (c == close) {
                depth--;
                if (depth == 0 && i < text.length() - 1) {
                    return false;
                }
            }
        }
        return depth == 0;
    }

    private long countJsonLines(final String[] lines) {
        long count = 0;
        for (final String line : lines) {
            final String trimmed = line.trim();
            if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                count++;
            }
        }
        return count;
    }

    private String[] filterNonEmpty(final String[] lines) {
        return java.util.Arrays.stream(lines)
                .filter(l -> !l.trim().isEmpty())
                .toArray(String[]::new);
    }

    private long countDelimiterOutsideQuotes(final String line, final char delimiter) {
        long count = 0;
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            final char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == delimiter && !inQuotes) {
                count++;
            }
        }
        return count;
    }

    /**
     * Checks if the text content is readable (mostly printable characters).
     * Binary data disguised as text will have a high ratio of non-printable chars.
     */
    private boolean isReadableText(final String text) {
        if (text.isEmpty()) {
            return false;
        }
        final int sampleLength = Math.min(text.length(), 1024);
        int nonPrintable = 0;
        for (int i = 0; i < sampleLength; i++) {
            final char c = text.charAt(i);
            if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
                nonPrintable++;
            } else if (c >= 0x7F && c <= 0x9F) {
                nonPrintable++;
            }
        }
        return (double) nonPrintable / sampleLength < 0.10;
    }

    private byte[] decompress(final byte[] data, final DetectedCompression compression) {
        if (compression == DetectedCompression.GZIP) {
            return decompressStream(data, true);
        }
        if (compression == DetectedCompression.SNAPPY) {
            return decompressStream(data, false);
        }
        // Zstd decompression not yet implemented.
        return data;
    }

    private byte[] decompressStream(final byte[] data, final boolean gzip) {
        try (final InputStream is = gzip
                ? new GZIPInputStream(new ByteArrayInputStream(data))
                : new SnappyInputStream(new ByteArrayInputStream(data))) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final byte[] buffer = new byte[4096];
            int bytesRead;
            int totalRead = 0;
            while ((bytesRead = is.read(buffer)) != -1 && totalRead < DEFAULT_SAMPLE_SIZE) {
                out.write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }
            return out.toByteArray();
        } catch (final IOException e) {
            // Corrupt compression — return empty so format comes back UNKNOWN.
            return new byte[0];
        }
    }
}
