/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.formatdetection;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Simulates the Intelligent Ingestion coordinator flow:
 *   1. Scan a directory (like S3 prefix)
 *   2. For each file, read first 64KB
 *   3. Detect format
 *   4. Select codec based on detection
 *   5. Report what would happen
 *
 * Usage:
 *   java -cp <classpath> FormatDetectorPipelineSimulator /path/to/data/directory
 */
public class FormatDetectorPipelineSimulator {

    private static final int SAMPLE_SIZE = 65536;

    public static void main(final String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: FormatDetectorPipelineSimulator <directory>");
            System.out.println("  Simulates the coordinator detecting format for each file in a directory.");
            System.exit(1);
        }

        final Path dataDir = Paths.get(args[0]);
        if (!Files.isDirectory(dataDir)) {
            System.err.println("Not a directory: " + dataDir);
            System.exit(1);
        }

        final FormatDetector detector = new FormatDetector();

        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Format Detection Pipeline Simulator                                       ║");
        System.out.println("║  Simulating: Discovery scan → Format detection → Codec selection           ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝\n");

        // Phase 1: Discovery — list all files (simulates ListObjectsV2)
        System.out.println("▶ Phase 1: Discovery (listing files in prefix)");
        System.out.println("  Directory: " + dataDir.toAbsolutePath());

        final Path[] files;
        try (Stream<Path> stream = Files.walk(dataDir).filter(Files::isRegularFile).sorted()) {
            files = stream.toArray(Path[]::new);
        }
        System.out.println("  Objects found: " + files.length);
        System.out.println();

        // Phase 2: Format probe (simulates ranged GetObject on first N files)
        System.out.println("▶ Phase 2: Format probe (sampling first " + Math.min(5, files.length) + " files)");
        System.out.println(String.format("  %-35s %-10s %-8s %-8s %-15s", "File", "Format", "Compr.", "Conf.", "Codec"));
        System.out.println("  " + "-".repeat(76));

        DetectedFormat dominantFormat = null;
        int sampleCount = 0;
        int[] formatCounts = new int[DetectedFormat.values().length];

        for (int i = 0; i < Math.min(5, files.length); i++) {
            final Path file = files[i];
            final byte[] sample = readSample(file);
            final FormatDetectionResult result = detector.detect(sample);

            formatCounts[result.getFormat().ordinal()]++;
            sampleCount++;

            System.out.println(String.format("  %-35s %-10s %-8s %-8s %-15s",
                    file.getFileName(),
                    result.getFormat(),
                    result.getCompression(),
                    result.getConfidence(),
                    codecForFormat(result.getFormat())));
        }

        // Find dominant format
        int maxCount = 0;
        for (int i = 0; i < formatCounts.length; i++) {
            if (formatCounts[i] > maxCount) {
                maxCount = formatCounts[i];
                dominantFormat = DetectedFormat.values()[i];
            }
        }

        System.out.println();
        System.out.println("  Dominant format: " + dominantFormat + " (" + maxCount + "/" + sampleCount + " files)");
        boolean consistent = maxCount == sampleCount;
        System.out.println("  Consistent: " + consistent);
        System.out.println("  → Cache result: {format=" + dominantFormat + ", consistent=" + consistent + "}");
        System.out.println();

        // Phase 3: Coordinator processing (simulates per-object ingestion)
        System.out.println("▶ Phase 3: Coordinator processing (per-object detection + codec selection)");
        System.out.println(String.format("  %-35s %-10s %-8s %-10s %-15s", "File", "Format", "Compr.", "Match?", "Action"));
        System.out.println("  " + "-".repeat(78));

        int matched = 0, mismatched = 0, failures = 0;

        for (final Path file : files) {
            final byte[] sample = readSample(file);
            final FormatDetectionResult result = detector.detect(sample);

            final boolean matchesCache = result.getFormat() == dominantFormat;
            final String action;

            if (result.getFormat() == DetectedFormat.UNKNOWN) {
                action = "→ FAILURE INDEX";
                failures++;
            } else if (matchesCache) {
                action = "→ parse with " + codecForFormat(result.getFormat());
                matched++;
            } else {
                action = "→ MISMATCH, use " + codecForFormat(result.getFormat());
                mismatched++;
            }

            System.out.println(String.format("  %-35s %-10s %-8s %-10s %-15s",
                    file.getFileName(),
                    result.getFormat(),
                    result.getCompression(),
                    matchesCache ? "✓" : "✗",
                    action));
        }

        // Summary
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Summary                                                                   ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Total files:    %-59d║%n", files.length);
        System.out.printf("║  Cache hits:     %-59d║%n", matched);
        System.out.printf("║  Mismatches:     %-59d║%n", mismatched);
        System.out.printf("║  Failures (DLQ): %-59d║%n", failures);
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");
    }

    private static String codecForFormat(final DetectedFormat format) {
        if (format == DetectedFormat.PARQUET) return "parquet";
        if (format == DetectedFormat.AVRO) return "avro";
        if (format == DetectedFormat.ORC) return "orc";
        if (format == DetectedFormat.JSON) return "json (array)";
        if (format == DetectedFormat.NDJSON) return "ndjson";
        if (format == DetectedFormat.CSV) return "csv";
        if (format == DetectedFormat.TSV) return "csv (tab)";
        if (format == DetectedFormat.XML) return "UNSUPPORTED";
        if (format == DetectedFormat.TEXT) return "newline";
        if (format == DetectedFormat.PDF || format == DetectedFormat.IMAGE) return "UNSUPPORTED";
        return "NONE";
    }

    private static byte[] readSample(final Path path) throws IOException {
        try (final InputStream is = Files.newInputStream(path)) {
            return is.readNBytes(SAMPLE_SIZE);
        }
    }
}
