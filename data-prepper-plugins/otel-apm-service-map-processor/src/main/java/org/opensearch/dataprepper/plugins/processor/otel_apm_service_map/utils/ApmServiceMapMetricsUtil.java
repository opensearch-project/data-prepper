/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.utils;

import org.opensearch.dataprepper.model.metric.DefaultExemplar;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonStandardHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ClientSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.HistogramBuckets;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricAggregationState;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricKey;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.SpanStateData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCommonUtils.convertUnixNanosToISO8601;

/**
 * Utility class for handling APM service map metrics generation and processing
 */
public final class ApmServiceMapMetricsUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ApmServiceMapMetricsUtil.class);
    // Standard latency buckets in seconds
    private static final List<Double> EXPLICIT_BOUNDS = List.of(0.0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1,
            0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0);

    /**
     * Generate metrics for a CLIENT span using decorated relationship data
     * Uses CLIENT-specific metric labels with remote service information
     *
     * @param clientSpan The CLIENT span
     * @param decoration The CLIENT span decoration containing pre-computed relationship data
     * @param currentTime Current timestamp
     * @param metricsStateByKey Shared map for metric aggregation
     * @param anchorTimestamp The anchor timestamp for metrics
     */
    public static void generateMetricsForClientSpan(final SpanStateData clientSpan,
                                                   final ClientSpanDecoration decoration,
                                                   final Instant currentTime,
                                                   final Map<MetricKey, MetricAggregationState> metricsStateByKey,
                                                   final Instant anchorTimestamp) {
        // Build CLIENT-side metric labels using decorated relationship data
        final Map<String, Object> labels = new HashMap<>();
        labels.put("namespace", "span_derived");
        labels.put("environment", clientSpan.getEnvironment());                // Environment = CLIENT span's environment
        labels.put("service", clientSpan.getServiceName());                         // Service = CLIENT span's own service name
        labels.put("operation", decoration.getParentServerOperationName());         // Operation = parentServerOperationName from decoration
        labels.put("remoteEnvironment", decoration.getRemoteEnvironment());         // RemoteEnvironment = remote span's environment
        labels.put("remoteService", decoration.getRemoteService());                 // RemoteService = remoteService from decoration
        labels.put("remoteOperation", decoration.getRemoteOperation());             // RemoteOperation = remoteOperation from decoration
        labels.putAll(clientSpan.getGroupByAttributes());                           // groupByAttributes = read from SpanStateData

        final MetricKey metricKey = new MetricKey(labels, anchorTimestamp);

        // Get or create aggregation state for this metric key
        MetricAggregationState state = metricsStateByKey.computeIfAbsent(metricKey, k -> new MetricAggregationState());

        // Increment request count for every CLIENT span
        state.incrementRequestCount(1);

        // Accumulate latency duration in seconds for histogram
        Long durationInNanos = clientSpan.getDurationInNanos();
        if (durationInNanos != null && durationInNanos > 0) {
            final double durationInSeconds = durationInNanos / 1_000_000_000.0;
            state.addLatencyDuration(durationInSeconds);
        }

        // Use pre-computed error and fault indicators from SpanStateData
        state.incrementErrorCount(clientSpan.getError());
        state.incrementFaultCount(clientSpan.getFault());

        // Add exemplars for error spans
        if (clientSpan.getError() == 1 && state.getErrorExemplars().size() < 10) {
            state.addErrorExemplar(createExemplarFromSpan(clientSpan, state.getErrorCount()));
        }

        // Add exemplars for fault spans
        if (clientSpan.getFault() == 1 && state.getFaultExemplars().size() < 10) {
            state.addFaultExemplar(createExemplarFromSpan(clientSpan, state.getFaultCount()));
        }
    }

    /**
     * Generate metrics for a SERVER span using span data directly
     *
     * @param serverSpan The SERVER span
     * @param currentTime Current timestamp
     * @param metricsStateByKey Shared map for metric aggregation
     * @param anchorTimestamp The anchor timestamp for metrics
     */
    public static void generateMetricsForServerSpan(final SpanStateData serverSpan,
                                                   final Instant currentTime,
                                                   final Map<MetricKey, MetricAggregationState> metricsStateByKey,
                                                   final Instant anchorTimestamp) {
        // Build metric labels using span's groupByAttributes (read directly from SpanStateData)
        final Map<String, Object> labels = new HashMap<>();
        labels.put("namespace", "span_derived");
        labels.put("environment", serverSpan.getEnvironment());
        labels.put("service", serverSpan.getServiceName());
        labels.put("operation", serverSpan.getOperationName());
        labels.putAll(serverSpan.getGroupByAttributes());

        final MetricKey metricKey = new MetricKey(labels, anchorTimestamp);

        // Get or create aggregation state for this metric key
        MetricAggregationState state = metricsStateByKey.computeIfAbsent(metricKey, k -> new MetricAggregationState());

        // Increment request count for every SERVER span
        state.incrementRequestCount(1);

        // Accumulate latency duration in seconds for histogram
        Long durationInNanos = serverSpan.getDurationInNanos();
        if (durationInNanos != null && durationInNanos > 0) {
            final double durationInSeconds = durationInNanos / 1_000_000_000.0;
            state.addLatencyDuration(durationInSeconds);
        }

        // Use pre-computed error and fault indicators from SpanStateData
        state.incrementErrorCount(serverSpan.getError());
        state.incrementFaultCount(serverSpan.getFault());

        // Add exemplars for error spans
        if (serverSpan.getError() == 1 && state.getErrorExemplars().size() < 10) {
            state.addErrorExemplar(createExemplarFromSpan(serverSpan, state.getErrorCount()));
        }

        // Add exemplars for fault spans
        if (serverSpan.getFault() == 1 && state.getFaultExemplars().size() < 10) {
            state.addFaultExemplar(createExemplarFromSpan(serverSpan, state.getFaultCount()));
        }
    }

    /**
     * Create all JacksonSum and JacksonStandardHistogram metrics from aggregated state
     * This method is called after ALL traces have been processed
     *
     * @param metricsStateByKey Shared map containing aggregated metric state for all traces
     * @return List of JacksonMetric objects (JacksonSum and JacksonStandardHistogram)
     */
    public static List<JacksonMetric> createMetricsFromAggregatedState(final Map<MetricKey, MetricAggregationState> metricsStateByKey) {
        final List<JacksonMetric> metrics = new ArrayList<>();

        // Generate JacksonSum and JacksonStandardHistogram metrics from aggregated state
        for (Map.Entry<MetricKey, MetricAggregationState> entry : metricsStateByKey.entrySet()) {
            final MetricKey metricKey = entry.getKey();
            final MetricAggregationState state = entry.getValue();

            // Create request_count metric (always generated for every SERVER span)
            metrics.add(createJacksonSumMetric(
                    "request",
                    "Number of requests",
                    state.getRequestCount(),
                    metricKey.getLabels(),
                    metricKey.getTimestamp(),
                    Collections.emptyList() // No exemplars for request count
            ));

            metrics.add(createJacksonSumMetric(
                    "error",
                    "Number of error requests",
                    state.getErrorCount(),
                    metricKey.getLabels(),
                    metricKey.getTimestamp(),
                    state.getErrorExemplars()
            ));

            metrics.add(createJacksonSumMetric(
                    "fault",
                    "Number of fault requests",
                    state.getFaultCount(),
                    metricKey.getLabels(),
                    metricKey.getTimestamp(),
                    state.getFaultExemplars()
            ));

            // Create latency_seconds histogram (only if there are duration samples)
            if (!state.getLatencyDurations().isEmpty()) {
                metrics.add(createJacksonStandardHistogram(
                        "latency_seconds",
                        "Request latency in seconds",
                        state.getLatencyDurations(),
                        metricKey.getLabels(),
                        metricKey.getTimestamp()
                ));
            }
        }

        // Sort metrics by timestamp for consistent output ordering
        metrics.sort(Comparator.comparing(JacksonMetric::getTime));
        return metrics;
    }


    /**
     * Create a single exemplar from a span
     *
     * @param span The span to create exemplar from
     * @param value The metric value (count) for the exemplar
     * @return Exemplar created from the span
     */
    static Exemplar createExemplarFromSpan(final SpanStateData spanStateData, final double value) {
        try {
            final String traceId = spanStateData.getTraceId();
            final String spanId = spanStateData.getSpanId();
            final long timestampNanos = getTimeNanos(Instant.now()); // Use current time for exemplar

            // Create attributes map for exemplar
            final Map<String, Object> attributes = new HashMap<>();
            attributes.put("service.name", spanStateData.getServiceName());
            attributes.put("operation.name", spanStateData.getOperationName());
            if (spanStateData.getStatus() != null) {
                attributes.put("status", spanStateData.getStatus());
            }

            return new DefaultExemplar(
                    convertUnixNanosToISO8601(timestampNanos),
                    value,
                    spanId,
                    traceId,
                    attributes
            );
        } catch (Exception e) {
            LOG.debug("Failed to create exemplar from span: {}", e.getMessage());
            // Return a minimal exemplar if creation fails
            return new DefaultExemplar(
                    convertUnixNanosToISO8601(getTimeNanos(Instant.now())),
                    value,
                    null,
                    null,
                    Collections.emptyMap()
            );
        }
    }

    /**
     * Create a JacksonSum metric with the specified parameters
     *
     * @param metricName Name of the metric
     * @param description Description of the metric
     * @param value Value of the metric
     * @param labels Labels for the metric
     * @param timestamp Timestamp for the metric
     * @param exemplars List of exemplars for the metric
     * @return JacksonSum metric event
     */
    static JacksonMetric createJacksonSumMetric(final String metricName,
                                                      final String description,
                                                      final double value,
                                                      final Map<String, Object> labels,
                                                      final Instant timestamp,
                                                      final List<Exemplar> exemplars) {
        final long timestampNanos = getTimeNanos(timestamp);
        final long startTimeNanos = timestampNanos; // For counter metrics, start time can be same as timestamp

        final Map<String, Object> labelsWithRandomKey = new HashMap<>();
        labelsWithRandomKey.putAll(labels);
        labelsWithRandomKey.put("randomKey", UUID.randomUUID().toString());

        return JacksonSum.builder()
                .withName(metricName)
                .withDescription(description)
                .withTime(convertUnixNanosToISO8601(timestampNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withIsMonotonic(true) // These are counter metrics
                .withUnit("1") // Count unit
                .withAggregationTemporality("AGGREGATION_TEMPORALITY_DELTA")
                .withValue(value)
                .withExemplars(exemplars)
                .withAttributes(labelsWithRandomKey)
                .build(false);
    }

    /**
     * Create a JacksonStandardHistogram metric from collected latency durations
     *
     * @param metricName Name of the metric
     * @param description Description of the metric
     * @param durations List of duration values in seconds
     * @param labels Labels for the metric
     * @param timestamp Timestamp for the metric
     * @return JacksonStandardHistogram metric event
     */
    static JacksonMetric createJacksonStandardHistogram(final String metricName,
                                                              final String description,
                                                              final List<Double> durations,
                                                              final Map<String, Object> labels,
                                                              final Instant timestamp) {
        final long timestampNanos = getTimeNanos(timestamp);
        final long startTimeNanos = timestampNanos; // For histogram metrics, start time can be same as timestamp

        // Create histogram buckets from raw duration values
        final HistogramBuckets buckets = createHistogramBucketsFromDurations(durations);

        final Map<String, Object> labelsWithRandomKey = new HashMap<>();
        labelsWithRandomKey.putAll(labels);
        labelsWithRandomKey.put("randomKey", UUID.randomUUID().toString());

        return JacksonStandardHistogram.builder()
                .withName(metricName)
                .withDescription(description)
                .withTime(convertUnixNanosToISO8601(timestampNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withUnit("s") // Seconds unit for latency
                .withAggregationTemporality("AGGREGATION_TEMPORALITY_DELTA")
                .withCount((long) durations.size())
                .withSum(durations.stream().mapToDouble(Double::doubleValue).sum())
                .withMin(durations.stream().mapToDouble(Double::doubleValue).min().orElse(0.0))
                .withMax(durations.stream().mapToDouble(Double::doubleValue).max().orElse(0.0))
                .withBucketCountsList(buckets.getBucketCounts())
                .withExplicitBoundsList(buckets.getExplicitBounds())
                .withBucketCount(buckets.getBucketCounts().size())
                .withExplicitBoundsCount(buckets.getExplicitBounds().size())
                .withAttributes(labelsWithRandomKey)
                .build(false);
    }

    /**
     * Create histogram buckets from raw duration values
     * Uses O-Tel Java SDK bucket: 0.0 ms to 10 sec
     * https://opentelemetry.io/docs/specs/otel/metrics/sdk/?utm_source=chatgpt.com#explicit-bucket-histogram-aggregation
     *
     * @param durations List of duration values in seconds
     * @return HistogramBuckets with counts and bounds
     */
    static HistogramBuckets createHistogramBucketsFromDurations(final List<Double> durations) {

        // Initialize bucket counts (one more than bounds for the overflow bucket)
        final List<Long> bucketCounts = new ArrayList<>(Collections.nCopies(EXPLICIT_BOUNDS.size() + 1, 0L));

        // Count durations into buckets
        for (Double duration : durations) {
            if (duration == null) continue;

            int bucketIndex = 0;
            for (int i = 0; i < EXPLICIT_BOUNDS.size(); i++) {
                if (duration <= EXPLICIT_BOUNDS.get(i)) {
                    bucketIndex = i;
                    break;
                }
                bucketIndex = EXPLICIT_BOUNDS.size(); // Overflow bucket
            }

            bucketCounts.set(bucketIndex, bucketCounts.get(bucketIndex) + 1);
        }

        return new HistogramBuckets(bucketCounts, EXPLICIT_BOUNDS);
    }

    // Private constructor to prevent instantiation
    private ApmServiceMapMetricsUtil() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    private static long getTimeNanos(final Instant time) {
        final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
        long currentTimeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        return currentTimeNanos;
    }
}
