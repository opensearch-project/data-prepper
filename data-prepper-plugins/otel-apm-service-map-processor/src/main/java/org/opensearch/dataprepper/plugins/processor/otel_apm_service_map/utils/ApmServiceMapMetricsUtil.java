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

import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCommonUtils.convertUnixNanosToISO8601;

/**
 * Utility class for handling APM service map metrics generation and processing.
 *
 * All metrics (sum and histogram) share the same anchor timestamp, truncated to
 * the configured granularity (seconds or minutes) via {@code metric_timestamp_granularity}.
 */
public final class ApmServiceMapMetricsUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ApmServiceMapMetricsUtil.class);
    private static final String HOST_ID_LABEL = "service_map_processor_host_id";
    // Standard latency buckets in seconds
    private static final List<Double> EXPLICIT_BOUNDS = List.of(0.0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1,
            0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0);

    /**
     * Generate metrics for a CLIENT span using decorated relationship data.
     *
     * @param clientSpan The CLIENT span
     * @param decoration The CLIENT span decoration containing pre-computed relationship data
     * @param currentTime Current timestamp
     * @param sumStateByKey Map for sum metric aggregation
     * @param histogramStateByKey Map for histogram metric aggregation
     * @param anchorTimestamp Timestamp truncated to the configured granularity for all metrics
     * @param hostId Stable identifier for this Data Prepper host
     */
    public static void generateMetricsForClientSpan(final SpanStateData clientSpan,
                                                   final ClientSpanDecoration decoration,
                                                   final Instant currentTime,
                                                   final Map<MetricKey, MetricAggregationState> sumStateByKey,
                                                   final Map<MetricKey, MetricAggregationState> histogramStateByKey,
                                                   final Instant anchorTimestamp,
                                                   final String hostId) {
        // Build CLIENT-side metric labels using decorated relationship data
        final Map<String, Object> labels = new HashMap<>();
        putCommonLabels(labels, clientSpan.getEnvironment(), clientSpan.getServiceName(),
                decoration.getParentServerOperationName(), hostId);
        labels.put("remoteEnvironment", decoration.getRemoteEnvironment());
        labels.put("remoteService", decoration.getRemoteService());
        labels.put("remoteOperation", decoration.getRemoteOperation());
        labels.putAll(clientSpan.getGroupByAttributes());

        // Sum metrics (request, error, fault)
        final MetricKey sumKey = new MetricKey(labels, anchorTimestamp);
        final MetricAggregationState sumState = sumStateByKey.computeIfAbsent(sumKey, k -> new MetricAggregationState());
        sumState.incrementRequestCount(1);
        sumState.incrementErrorCount(clientSpan.getError());
        sumState.incrementFaultCount(clientSpan.getFault());

        if (clientSpan.getError() == 1 && sumState.getErrorExemplars().size() < 10) {
            sumState.addErrorExemplar(createExemplarFromSpan(clientSpan, sumState.getErrorCount()));
        }
        if (clientSpan.getFault() == 1 && sumState.getFaultExemplars().size() < 10) {
            sumState.addFaultExemplar(createExemplarFromSpan(clientSpan, sumState.getFaultCount()));
        }

        // Histogram metrics (latency)
        final Long durationInNanos = clientSpan.getDurationInNanos();
        if (durationInNanos != null && durationInNanos > 0) {
            final MetricKey histKey = new MetricKey(labels, anchorTimestamp);
            final MetricAggregationState histState = histogramStateByKey.computeIfAbsent(histKey, k -> new MetricAggregationState());
            histState.addLatencyDuration(durationInNanos / 1_000_000_000.0);
        }
    }

    /**
     * Generate metrics for a SERVER span using span data directly.
     *
     * @param serverSpan The SERVER span
     * @param currentTime Current timestamp
     * @param sumStateByKey Map for sum metric aggregation
     * @param histogramStateByKey Map for histogram metric aggregation
     * @param anchorTimestamp Timestamp truncated to the configured granularity for all metrics
     * @param hostId Stable identifier for this Data Prepper host
     */
    public static void generateMetricsForServerSpan(final SpanStateData serverSpan,
                                                   final Instant currentTime,
                                                   final Map<MetricKey, MetricAggregationState> sumStateByKey,
                                                   final Map<MetricKey, MetricAggregationState> histogramStateByKey,
                                                   final Instant anchorTimestamp,
                                                   final String hostId) {
        // Build metric labels using span's groupByAttributes
        final Map<String, Object> labels = new HashMap<>();
        putCommonLabels(labels, serverSpan.getEnvironment(), serverSpan.getServiceName(),
                serverSpan.getOperationName(), hostId);
        labels.putAll(serverSpan.getGroupByAttributes());

        // Sum metrics (request, error, fault)
        final MetricKey sumKey = new MetricKey(labels, anchorTimestamp);
        final MetricAggregationState sumState = sumStateByKey.computeIfAbsent(sumKey, k -> new MetricAggregationState());
        sumState.incrementRequestCount(1);
        sumState.incrementErrorCount(serverSpan.getError());
        sumState.incrementFaultCount(serverSpan.getFault());

        if (serverSpan.getError() == 1 && sumState.getErrorExemplars().size() < 10) {
            sumState.addErrorExemplar(createExemplarFromSpan(serverSpan, sumState.getErrorCount()));
        }
        if (serverSpan.getFault() == 1 && sumState.getFaultExemplars().size() < 10) {
            sumState.addFaultExemplar(createExemplarFromSpan(serverSpan, sumState.getFaultCount()));
        }

        // Histogram metrics (latency)
        final Long durationInNanos = serverSpan.getDurationInNanos();
        if (durationInNanos != null && durationInNanos > 0) {
            final MetricKey histKey = new MetricKey(labels, anchorTimestamp);
            final MetricAggregationState histState = histogramStateByKey.computeIfAbsent(histKey, k -> new MetricAggregationState());
            histState.addLatencyDuration(durationInNanos / 1_000_000_000.0);
        }
    }

    /**
     * Create all JacksonSum and JacksonStandardHistogram metrics from aggregated state.
     *
     * @param sumStateByKey Map containing aggregated sum metric state
     * @param histogramStateByKey Map containing aggregated histogram metric state
     * @return List of JacksonMetric objects (JacksonSum and JacksonStandardHistogram)
     */
    public static List<JacksonMetric> createMetricsFromAggregatedState(
            final Map<MetricKey, MetricAggregationState> sumStateByKey,
            final Map<MetricKey, MetricAggregationState> histogramStateByKey) {
        final List<JacksonMetric> metrics = new ArrayList<>();

        // Generate Sum metrics (request, error, fault)
        for (Map.Entry<MetricKey, MetricAggregationState> entry : sumStateByKey.entrySet()) {
            final MetricKey metricKey = entry.getKey();
            final MetricAggregationState state = entry.getValue();

            metrics.add(createJacksonSumMetric(
                    "request",
                    "Number of requests",
                    state.getRequestCount(),
                    metricKey.getLabels(),
                    metricKey.getTimestamp(),
                    Collections.emptyList()
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
        }

        // Generate Histogram metrics (latency)
        for (Map.Entry<MetricKey, MetricAggregationState> entry : histogramStateByKey.entrySet()) {
            final MetricKey metricKey = entry.getKey();
            final MetricAggregationState state = entry.getValue();

            if (!state.getLatencyDurations().isEmpty()) {
                metrics.add(createJacksonStandardHistogram(
                        "latency",
                        "Request latency in seconds",
                        state.getLatencyDurations(),
                        metricKey.getLabels(),
                        metricKey.getTimestamp()
                ));
            }
        }

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
     */
    static JacksonMetric createJacksonSumMetric(final String metricName,
                                                      final String description,
                                                      final double value,
                                                      final Map<String, Object> labels,
                                                      final Instant timestamp,
                                                      final List<Exemplar> exemplars) {
        final long timestampNanos = getTimeNanos(timestamp);
        final long startTimeNanos = timestampNanos;

        return JacksonSum.builder()
                .withName(metricName)
                .withDescription(description)
                .withTime(convertUnixNanosToISO8601(timestampNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withIsMonotonic(true)
                .withUnit("1")
                .withAggregationTemporality("AGGREGATION_TEMPORALITY_DELTA")
                .withValue(value)
                .withExemplars(exemplars)
                .withAttributes(labels)
                .build(false);
    }

    /**
     * Create a JacksonStandardHistogram metric from collected latency durations
     */
    static JacksonMetric createJacksonStandardHistogram(final String metricName,
                                                              final String description,
                                                              final List<Double> durations,
                                                              final Map<String, Object> labels,
                                                              final Instant timestamp) {
        final long timestampNanos = getTimeNanos(timestamp);
        final long startTimeNanos = timestampNanos;

        // Create histogram buckets from raw duration values
        final HistogramBuckets buckets = createHistogramBucketsFromDurations(durations);

        return JacksonStandardHistogram.builder()
                .withName(metricName)
                .withDescription(description)
                .withTime(convertUnixNanosToISO8601(timestampNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withUnit("s")
                .withAggregationTemporality("AGGREGATION_TEMPORALITY_DELTA")
                .withCount((long) durations.size())
                .withSum(durations.stream().mapToDouble(Double::doubleValue).sum())
                .withMin(durations.stream().mapToDouble(Double::doubleValue).min().orElse(0.0))
                .withMax(durations.stream().mapToDouble(Double::doubleValue).max().orElse(0.0))
                .withBucketCountsList(buckets.getBucketCounts())
                .withExplicitBoundsList(buckets.getExplicitBounds())
                .withBucketCount(buckets.getBucketCounts().size())
                .withExplicitBoundsCount(buckets.getExplicitBounds().size())
                .withAttributes(labels)
                .build(false);
    }

    /**
     * Create histogram buckets from raw duration values
     * Uses O-Tel Java SDK bucket: 0.0 ms to 10 sec
     * https://opentelemetry.io/docs/specs/otel/metrics/sdk/#explicit-bucket-histogram-aggregation
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

    private static void putCommonLabels(final Map<String, Object> labels,
                                         final String environment,
                                         final String serviceName,
                                         final String operationName,
                                         final String hostId) {
        labels.put("namespace", "span_derived");
        labels.put("environment", environment);
        labels.put("service", serviceName);
        labels.put("operation", operationName);
        labels.put(HOST_ID_LABEL, hostId);
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
