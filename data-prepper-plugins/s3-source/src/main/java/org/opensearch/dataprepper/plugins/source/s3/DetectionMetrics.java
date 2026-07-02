/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.plugins.formatdetection.Confidence;
import org.opensearch.dataprepper.plugins.formatdetection.DetectedCompression;
import org.opensearch.dataprepper.plugins.formatdetection.DetectedFormat;
import org.opensearch.dataprepper.plugins.formatdetection.FormatDetectionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks format detection statistics for dashboard/logging purposes.
 * Logs a summary periodically showing format distribution and confidence breakdown.
 */
class DetectionMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(DetectionMetrics.class);

    private final Map<DetectedFormat, AtomicInteger> formatCounts = new ConcurrentHashMap<>();
    private final Map<DetectedCompression, AtomicInteger> compressionCounts = new ConcurrentHashMap<>();
    private final Map<Confidence, AtomicInteger> confidenceCounts = new ConcurrentHashMap<>();
    private final AtomicInteger totalDetections = new AtomicInteger(0);
    private final AtomicInteger failedDetections = new AtomicInteger(0);

    void record(final FormatDetectionResult result) {
        totalDetections.incrementAndGet();
        formatCounts.computeIfAbsent(result.getFormat(), k -> new AtomicInteger()).incrementAndGet();
        compressionCounts.computeIfAbsent(result.getCompression(), k -> new AtomicInteger()).incrementAndGet();
        confidenceCounts.computeIfAbsent(result.getConfidence(), k -> new AtomicInteger()).incrementAndGet();

        if (result.getFormat() == DetectedFormat.UNKNOWN) {
            failedDetections.incrementAndGet();
        }
    }

    void logSummary() {
        final int total = totalDetections.get();
        if (total == 0) {
            return;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("\n╔══════════════════════════════════════════════════╗\n");
        sb.append("║  Format Detection Summary                        ║\n");
        sb.append("╠══════════════════════════════════════════════════╣\n");
        sb.append(String.format("║  Total objects processed: %-23d║%n", total));
        sb.append(String.format("║  Failed detections:       %-23d║%n", failedDetections.get()));
        sb.append("╠══════════════════════════════════════════════════╣\n");
        sb.append("║  Format Distribution:                            ║\n");

        formatCounts.entrySet().stream()
                .sorted((a, b) -> b.getValue().get() - a.getValue().get())
                .forEach(entry -> {
                    final int count = entry.getValue().get();
                    final double pct = (count * 100.0) / total;
                    sb.append(String.format("║    %-12s %4d  (%5.1f%%)                  ║%n",
                            entry.getKey(), count, pct));
                });

        sb.append("╠══════════════════════════════════════════════════╣\n");
        sb.append("║  Compression:                                    ║\n");

        compressionCounts.entrySet().stream()
                .sorted((a, b) -> b.getValue().get() - a.getValue().get())
                .forEach(entry -> {
                    final int count = entry.getValue().get();
                    final double pct = (count * 100.0) / total;
                    sb.append(String.format("║    %-12s %4d  (%5.1f%%)                  ║%n",
                            entry.getKey(), count, pct));
                });

        sb.append("╠══════════════════════════════════════════════════╣\n");
        sb.append("║  Confidence:                                     ║\n");

        confidenceCounts.entrySet().stream()
                .sorted((a, b) -> a.getKey().ordinal() - b.getKey().ordinal())
                .forEach(entry -> {
                    final int count = entry.getValue().get();
                    final double pct = (count * 100.0) / total;
                    sb.append(String.format("║    %-12s %4d  (%5.1f%%)                  ║%n",
                            entry.getKey(), count, pct));
                });

        sb.append("╚══════════════════════════════════════════════════╝");

        LOG.info(sb.toString());
    }
}
