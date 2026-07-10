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

/**
 * Standalone runner to test format detection against real files.
 *
 * Usage:
 *   ./gradlew :data-prepper-plugins:format-detection:test --tests "*FormatDetectorRunner*" \
 *       -DtestFiles="/path/to/file1.json,/path/to/file2.csv.gz"
 *
 * Or run directly:
 *   java -cp ... FormatDetectorRunner /path/to/file1 /path/to/file2
 */
public class FormatDetectorRunner {

    private static final int SAMPLE_SIZE = 65536; // 64 KB

    public static void main(final String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: FormatDetectorRunner <file1> [file2] [file3] ...");
            System.out.println("  Detects format and compression of each file using first 64KB.");
            System.exit(1);
        }

        final FormatDetector detector = new FormatDetector();

        System.out.println("=".repeat(80));
        System.out.printf("%-40s %-12s %-10s %-8s%n", "File", "Format", "Compress", "Conf");
        System.out.println("-".repeat(80));

        for (final String filePath : args) {
            final Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                System.out.printf("%-40s %-12s%n", truncatePath(filePath), "NOT_FOUND");
                continue;
            }

            final byte[] sample = readSample(path);
            final FormatDetectionResult result = detector.detect(sample);

            System.out.printf("%-40s %-12s %-10s %-8s%n",
                    truncatePath(filePath),
                    result.getFormat(),
                    result.getCompression(),
                    result.getConfidence());
        }

        System.out.println("=".repeat(80));
    }

    private static byte[] readSample(final Path path) throws IOException {
        try (final InputStream is = Files.newInputStream(path)) {
            return is.readNBytes(SAMPLE_SIZE);
        }
    }

    private static String truncatePath(final String path) {
        if (path.length() <= 40) {
            return path;
        }
        return "..." + path.substring(path.length() - 37);
    }
}
