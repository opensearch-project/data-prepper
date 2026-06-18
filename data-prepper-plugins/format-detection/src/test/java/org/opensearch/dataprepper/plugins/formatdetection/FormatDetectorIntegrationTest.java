/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.formatdetection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-scenario integration test.
 *
 * Run against custom files:
 *   ./gradlew :data-prepper-plugins:format-detection:test \
 *       --tests "*FormatDetectorIntegrationTest.detectCustomFile" \
 *       -DtestFile="/path/to/your/file.json.gz"
 *
 * Run the sample directory scan:
 *   ./gradlew :data-prepper-plugins:format-detection:test \
 *       --tests "*FormatDetectorIntegrationTest.detectAllSamples"
 */
class FormatDetectorIntegrationTest {

    private static final int SAMPLE_SIZE = 65536;
    private FormatDetector detector;

    @BeforeEach
    void setUp() {
        detector = new FormatDetector();
    }

    @Test
    void detectAllSamples() throws IOException {
        final Path samplesDir = Paths.get("src/test/resources/samples");
        assertThat(Files.exists(samplesDir)).isTrue();

        System.out.println("\n" + "=".repeat(80));
        System.out.printf("%-30s %-12s %-10s %-8s %-6s%n", "File", "Format", "Compress", "Conf", "Bytes");
        System.out.println("-".repeat(80));

        Files.list(samplesDir).sorted().forEach(path -> {
            try {
                final byte[] sample = readSample(path);
                final FormatDetectionResult result = detector.detect(sample);
                System.out.printf("%-30s %-12s %-10s %-8s %-6d%n",
                        path.getFileName(),
                        result.getFormat(),
                        result.getCompression(),
                        result.getConfidence(),
                        sample.length);

                // Every file should detect as something other than UNKNOWN
                assertThat(result.getFormat())
                        .as("format for %s", path.getFileName())
                        .isNotEqualTo(DetectedFormat.UNKNOWN);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        System.out.println("=".repeat(80) + "\n");
    }

    @Test
    @EnabledIfSystemProperty(named = "testFile", matches = ".+")
    void detectCustomFile() throws IOException {
        final String filePath = System.getProperty("testFile");
        final Path path = Paths.get(filePath);
        assertThat(Files.exists(path)).as("File must exist: %s", filePath).isTrue();

        final byte[] sample = readSample(path);
        final FormatDetectionResult result = detector.detect(sample);

        System.out.printf("%nDetection result for: %s%n", filePath);
        System.out.printf("  Format:      %s%n", result.getFormat());
        System.out.printf("  Compression: %s%n", result.getCompression());
        System.out.printf("  Confidence:  %s%n", result.getConfidence());
        System.out.printf("  Sample size: %d bytes%n%n", sample.length);

        assertThat(result.getFormat()).isNotNull();
        assertThat(result.getCompression()).isNotNull();
        assertThat(result.getConfidence()).isNotNull();
    }

    @Test
    @EnabledIfSystemProperty(named = "testDir", matches = ".+")
    void detectAllFilesInDirectory() throws IOException {
        final String dirPath = System.getProperty("testDir");
        final Path dir = Paths.get(dirPath);
        assertThat(Files.isDirectory(dir)).as("Must be directory: %s", dirPath).isTrue();

        System.out.println("\n" + "=".repeat(80));
        System.out.printf("%-40s %-12s %-10s %-8s%n", "File", "Format", "Compress", "Conf");
        System.out.println("-".repeat(80));

        Files.walk(dir).filter(Files::isRegularFile).sorted().forEach(path -> {
            try {
                final byte[] sample = readSample(path);
                final FormatDetectionResult result = detector.detect(sample);
                System.out.printf("%-40s %-12s %-10s %-8s%n",
                        dir.relativize(path),
                        result.getFormat(),
                        result.getCompression(),
                        result.getConfidence());
            } catch (IOException e) {
                System.out.printf("%-40s ERROR: %s%n", dir.relativize(path), e.getMessage());
            }
        });
        System.out.println("=".repeat(80) + "\n");
    }

    private byte[] readSample(final Path path) throws IOException {
        try (final InputStream is = Files.newInputStream(path)) {
            return is.readNBytes(SAMPLE_SIZE);
        }
    }
}
