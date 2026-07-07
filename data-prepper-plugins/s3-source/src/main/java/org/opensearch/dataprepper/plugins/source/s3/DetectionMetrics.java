/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.formatdetection.Confidence;
import org.opensearch.dataprepper.plugins.formatdetection.DetectedCompression;
import org.opensearch.dataprepper.plugins.formatdetection.DetectedFormat;
import org.opensearch.dataprepper.plugins.formatdetection.FormatDetectionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks format detection metrics using Micrometer (exposed via Prometheus/CloudWatch).
 *
 * Metrics emitted:
 *   - formatDetection.total           (counter) — total objects detected
 *   - formatDetection.failed          (counter) — objects that could not be detected (UNKNOWN)
 *   - formatDetection.format.{name}   (counter) — count per format type (JSON, NDJSON, CSV, etc.)
 *   - formatDetection.compression.{name} (counter) — count per compression type
 *   - formatDetection.confidence.high (counter) — HIGH confidence detections
 *   - formatDetection.confidence.medium (counter) — MEDIUM confidence detections
 *   - formatDetection.confidence.low  (counter) — LOW confidence detections
 *   - formatDetection.duration        (timer)   — time spent detecting format
 */
class DetectionMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(DetectionMetrics.class);

    static final String DETECTION_TOTAL = "formatDetection.total";
    static final String DETECTION_FAILED = "formatDetection.failed";
    static final String DETECTION_DURATION = "formatDetection.duration";
    static final String DETECTION_TYPE_PREFIX = "formatDetection.";
    static final String DETECTION_CONFIDENCE_PREFIX = "formatDetection.confidence.";

    private final Counter totalCounter;
    private final Counter failedCounter;
    private final Timer detectionTimer;
    private final Map<String, Counter> typeCounters;
    private final Map<String, Timer> typeTimers;
    private final Map<Confidence, Counter> confidenceCounters;
    private final PluginMetrics pluginMetrics;

    // In-memory tracking for log summary
    private final Map<String, AtomicInteger> typeCounts = new ConcurrentHashMap<>();
    private final Map<Confidence, AtomicInteger> confidenceCounts = new ConcurrentHashMap<>();
    private final AtomicInteger totalDetections = new AtomicInteger(0);
    private final AtomicInteger failedDetections = new AtomicInteger(0);
    private volatile long lastSummaryTime = 0;
    private volatile int lastSummaryTotal = 0;
    private static final long SUMMARY_INTERVAL_MS = 60_000; // Log summary every 60 seconds

    DetectionMetrics(final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.totalCounter = pluginMetrics.counter(DETECTION_TOTAL);
        this.failedCounter = pluginMetrics.counter(DETECTION_FAILED);
        this.detectionTimer = pluginMetrics.timer(DETECTION_DURATION);
        this.typeCounters = new ConcurrentHashMap<>();
        this.typeTimers = new ConcurrentHashMap<>();
        this.confidenceCounters = new ConcurrentHashMap<>();
    }

    /**
     * Record a detection result — updates both Micrometer metrics and in-memory summary.
     * Emits combined format+compression metrics like:
     *   formatDetection.ndjson        (uncompressed NDJSON)
     *   formatDetection.ndjson.gzip   (gzipped NDJSON)
     *   formatDetection.csv.snappy    (snappy-compressed CSV)
     */
    void record(final FormatDetectionResult result, final Duration duration) {
        totalCounter.increment();
        totalDetections.incrementAndGet();
        detectionTimer.record(duration);

        // Combined type key: format or format.compression
        final String typeKey = buildTypeKey(result);
        typeCounters.computeIfAbsent(typeKey,
                k -> pluginMetrics.counter(DETECTION_TYPE_PREFIX + k)).increment();
        // Per-type duration timer
        typeTimers.computeIfAbsent(typeKey,
                k -> pluginMetrics.timer(DETECTION_TYPE_PREFIX + k + ".duration")).record(duration);
        typeCounts.computeIfAbsent(typeKey, k -> new AtomicInteger()).incrementAndGet();

        // Confidence counter
        final Confidence confidence = result.getConfidence();
        confidenceCounters.computeIfAbsent(confidence,
                c -> pluginMetrics.counter(DETECTION_CONFIDENCE_PREFIX + c.name().toLowerCase())).increment();
        confidenceCounts.computeIfAbsent(confidence, k -> new AtomicInteger()).incrementAndGet();

        // Failed counter
        if (result.getFormat() == DetectedFormat.UNKNOWN) {
            failedCounter.increment();
            failedDetections.incrementAndGet();
        }
    }

    private String buildTypeKey(final FormatDetectionResult result) {
        final String format = result.getFormat().name().toLowerCase();
        if (result.getCompression() == DetectedCompression.NONE) {
            return format;
        }
        return format + "." + result.getCompression().name().toLowerCase();
    }

    /**
     * Log a summary of detection statistics if enough time has passed and there's new activity.
     * Only logs every 60 seconds, and only if new detections occurred since last summary.
     */
    void logSummary() {
        final int total = totalDetections.get();
        if (total == 0) {
            return;
        }

        final long now = System.currentTimeMillis();
        if (now - lastSummaryTime < SUMMARY_INTERVAL_MS) {
            return; // Too soon since last summary
        }
        if (total == lastSummaryTotal) {
            return; // No new activity
        }

        lastSummaryTime = now;
        lastSummaryTotal = total;
        printSummary(total);
    }

    /**
     * Force log a summary regardless of timing (e.g., on shutdown or error).
     */
    void forceLogSummary() {
        final int total = totalDetections.get();
        if (total > 0) {
            printSummary(total);
        }
    }

    private void printSummary(final int total) {

        final StringBuilder sb = new StringBuilder();
        sb.append("\n╔══════════════════════════════════════════════════╗\n");
        sb.append("║  Format Detection Summary                        ║\n");
        sb.append("╠══════════════════════════════════════════════════╣\n");
        sb.append(String.format("║  Total objects processed: %-23d║%n", total));
        sb.append(String.format("║  Failed detections:       %-23d║%n", failedDetections.get()));
        sb.append("╠══════════════════════════════════════════════════╣\n");
        sb.append("║  Type Distribution:                              ║\n");

        typeCounts.entrySet().stream()
                .sorted((a, b) -> b.getValue().get() - a.getValue().get())
                .forEach(entry -> {
                    final int count = entry.getValue().get();
                    final double pct = (count * 100.0) / total;
                    sb.append(String.format("║    %-20s %4d  (%5.1f%%)            ║%n",
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
