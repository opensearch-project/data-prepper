/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.formatdetection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import org.xerial.snappy.SnappyInputStream;

/**
 * Detects data format and compression from raw bytes.
 *
 * <p>Detection pipeline:
 * <ol>
 *   <li>Detect compression via magic bytes</li>
 *   <li>Decompress sample if needed</li>
 *   <li>Detect binary formats (Parquet, Avro, ORC) via magic bytes</li>
 *   <li>Detect text formats (JSON, NDJSON, XML, CSV, TSV) via heuristics</li>
 * </ol>
 */
public class FormatDetector {

    private static final int DEFAULT_SAMPLE_SIZE = 65536; // 64 KB

    // Compression magic bytes
    private static final byte[] GZIP_MAGIC = {(byte) 0x1F, (byte) 0x8B};
    private static final byte[] ZSTD_MAGIC = {(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD};
    private static final byte[] SNAPPY_FRAMED_MAGIC = {(byte) 0xFF, 0x06, 0x00, 0x00, 0x73, 0x4E, 0x61, 0x50, 0x70, 0x59};
    private static final byte[] SNAPPY_STREAM_MAGIC = {(byte) 0x82, 0x53, 0x4E, 0x41, 0x50, 0x50, 0x59}; // snappy-java SnappyOutputStream

    // Binary format magic bytes
    private static final byte[] PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] AVRO_MAGIC = {0x4F, 0x62, 0x6A, 0x01}; // "Obj\x01"
    private static final byte[] ORC_MAGIC = "ORC".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PDF_MAGIC = "%PDF".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] JPEG_MAGIC = {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF};
    private static final byte[] PNG_MAGIC = {(byte) 0x89, 0x50, 0x4E, 0x47};

    private static final double NDJSON_LINE_THRESHOLD = 0.8;
    private static final double CSV_CONSISTENCY_THRESHOLD = 0.8;
    private static final int CSV_MIN_LINES = 2;
    private static final int CSV_MAX_SAMPLE_LINES = 10;

    /**
     * Detect format from a byte array sample.
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

        // Check binary formats first
        final DetectedFormat binaryFormat = detectBinaryFormat(decompressed);
        if (binaryFormat != null) {
            return new FormatDetectionResult(compression, binaryFormat, Confidence.HIGH);
        }

        // Text-based detection
        String text = new String(decompressed, StandardCharsets.UTF_8).trim();
        // Strip UTF-8 BOM if present
        if (text.startsWith("\uFEFF")) {
            text = text.substring(1);
        }
        if (text.isEmpty()) {
            return new FormatDetectionResult(compression, DetectedFormat.UNKNOWN, Confidence.LOW);
        }

        return detectTextFormat(text, compression);
    }

    DetectedCompression detectCompression(final byte[] data) {
        if (startsWith(data, GZIP_MAGIC)) {
            return DetectedCompression.GZIP;
        }
        if (startsWith(data, ZSTD_MAGIC)) {
            return DetectedCompression.ZSTD;
        }
        if (startsWith(data, SNAPPY_FRAMED_MAGIC) || startsWith(data, SNAPPY_STREAM_MAGIC)) {
            return DetectedCompression.SNAPPY;
        }
        return DetectedCompression.NONE;
    }

    DetectedFormat detectBinaryFormat(final byte[] data) {
        if (startsWith(data, PARQUET_MAGIC)) {
            return DetectedFormat.PARQUET;
        }
        if (startsWith(data, AVRO_MAGIC)) {
            return DetectedFormat.AVRO;
        }
        if (startsWith(data, ORC_MAGIC)) {
            return DetectedFormat.ORC;
        }
        if (startsWith(data, PDF_MAGIC)) {
            return DetectedFormat.PDF;
        }
        if (startsWith(data, JPEG_MAGIC) || startsWith(data, PNG_MAGIC)) {
            return DetectedFormat.IMAGE;
        }
        return null;
    }

    FormatDetectionResult detectTextFormat(final String text, final DetectedCompression compression) {
        // XML: starts with <?xml or a tag
        if (text.startsWith("<?xml") || (text.startsWith("<") && text.contains(">"))) {
            return new FormatDetectionResult(compression, DetectedFormat.XML, Confidence.HIGH);
        }

        // JSON: try full parse as object or array
        if (text.startsWith("{") || text.startsWith("[")) {
            if (isCompleteJson(text)) {
                if (text.startsWith("[")) {
                    return new FormatDetectionResult(compression, DetectedFormat.JSON, Confidence.HIGH);
                }
                // Single JSON object — maps to NDJSON since json codec only handles arrays
                return new FormatDetectionResult(compression, DetectedFormat.NDJSON, Confidence.HIGH);
            }
            // Could be truncated JSON or ND-JSON — try line-by-line
        }

        // ND-JSON: each line is a JSON object
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

        // CSV: consistent comma count across lines
        if (nonEmptyLines.length >= CSV_MIN_LINES) {
            final DetectedFormat delimiterFormat = detectDelimitedFormat(nonEmptyLines);
            if (delimiterFormat != null) {
                return new FormatDetectionResult(compression, delimiterFormat, Confidence.MEDIUM);
            }
        }

        // Check if content is actually readable text (not binary garbage)
        if (!isReadableText(text)) {
            return new FormatDetectionResult(compression, DetectedFormat.UNKNOWN, Confidence.LOW);
        }

        // Fallback: plain text
        return new FormatDetectionResult(compression, DetectedFormat.TEXT, Confidence.LOW);
    }

    private DetectedFormat detectDelimitedFormat(final String[] lines) {
        // Try comma, tab, pipe
        if (isConsistentDelimiter(lines, ',')) {
            return DetectedFormat.CSV;
        }
        if (isConsistentDelimiter(lines, '\t')) {
            return DetectedFormat.TSV;
        }
        if (isConsistentDelimiter(lines, '|')) {
            return DetectedFormat.CSV; // pipe-delimited is a CSV variant
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
        // Simple bracket-matching heuristic for POC
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
                    // Closed early — there's content after the closing bracket
                    // Could be ND-JSON (multiple objects on separate lines)
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

    private long countChar(final String s, final char c) {
        return s.chars().filter(ch -> ch == c).count();
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
     * Returns false if more than 10% of characters are non-printable (excluding common whitespace).
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

    private boolean startsWith(final byte[] data, final byte[] prefix) {
        if (data.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private byte[] decompress(final byte[] data, final DetectedCompression compression) {
        if (compression == DetectedCompression.GZIP) {
            return decompressGzip(data);
        }
        if (compression == DetectedCompression.SNAPPY) {
            return decompressSnappy(data);
        }
        // Zstd decompression not yet implemented
        return data;
    }

    private byte[] decompressGzip(final byte[] data) {
        try (final InputStream is = new GZIPInputStream(new ByteArrayInputStream(data))) {
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
            // Corrupt gzip — return original bytes so format comes back UNKNOWN
            return new byte[0];
        }
    }

    private byte[] decompressSnappy(final byte[] data) {
        try (final InputStream is = new SnappyInputStream(new ByteArrayInputStream(data))) {
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
            // Corrupt snappy — return empty so format comes back UNKNOWN
            return new byte[0];
        }
    }
}
