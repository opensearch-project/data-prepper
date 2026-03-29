/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.google.protobuf.InvalidProtocolBufferException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Parses Prometheus Remote Write protocol data and converts it to Data Prepper Metric events.
 *
 * <p>Prometheus Remote Write v1 does not carry metric type metadata in the protobuf payload.
 * This parser infers the type from the metric name suffix and labels following Prometheus naming conventions:
 * <ul>
 *   <li>{@code _bucket} suffix with {@code le} label - Histogram bucket</li>
 *   <li>{@code quantile} label - Summary quantile</li>
 *   <li>{@code _total}, {@code _created} suffix - Sum (monotonic counter)</li>
 *   <li>everything else - Gauge</li>
 * </ul>
 *
 * <p>Histogram and Summary metrics are grouped from multiple TimeSeries into a single metric event
 * per unique combination of (base name, common label set, sample timestamp). Prometheus cumulative
 * histogram buckets are converted to per-bucket counts.
 *
 * <p>The grouping strategy uses a two-pass approach:
 * <ol>
 *   <li>First pass: parse all labels (cached for reuse) and identify definitive histogram bases
 *       (from {@code _bucket+le}) and summary bases (from {@code quantile} label).</li>
 *   <li>Second pass: classify each TimeSeries using the cached labels. {@code _count} and
 *       {@code _sum} are only grouped if their base name matches a known histogram or summary;
 *       others are emitted as standalone gauges. Within each group, TimeSeries are further
 *       sub-grouped by common label set and sample timestamp to produce one event per distinct
 *       combination.</li>
 * </ol>
 */
public class RemoteWriteProtobufParser {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteWriteProtobufParser.class);
    private static final String METRIC_NAME_LABEL = "__name__";
    private static final String DEFAULT_METRIC_NAME = "unknown_metric";
    private static final String LE_LABEL = "le";
    private static final String QUANTILE_LABEL = "quantile";
    private static final String BUCKET_SUFFIX = "_bucket";
    private static final String COUNT_SUFFIX = "_count";
    private static final String SUM_SUFFIX = "_sum";
    private static final String TOTAL_SUFFIX = "_total";
    private static final String CREATED_SUFFIX = "_created";
    private static final String SERVICE_NAME_LABEL = "service.name";
    private static final String SERVICE_NAME_UNDERSCORE_LABEL = "service_name";
    private static final String JOB_LABEL = "job";

    private final PrometheusRemoteWriteSourceConfig config;

    public RemoteWriteProtobufParser(final PrometheusRemoteWriteSourceConfig config) {
        this.config = config;
    }

    /**
     * Parses the Prometheus Remote Write request body and converts to Data Prepper records.
     *
     * @param body the raw request body bytes (Snappy-compressed protobuf)
     * @return list of records containing converted metric events
     * @throws IOException if Snappy decompression fails
     * @throws PrometheusParseException if protobuf parsing fails
     */
    public List<Record<Event>> parse(final byte[] body) throws IOException, PrometheusParseException {
        final byte[] decompressed = SnappyDecompressor.decompress(body);
        return parseDecompressed(decompressed);
    }

    /**
     * Parses already-decompressed protobuf data and converts to Data Prepper records.
     *
     * @param decompressed the decompressed protobuf data
     * @return list of records containing converted metric events
     * @throws PrometheusParseException if protobuf parsing fails
     */
    public List<Record<Event>> parseDecompressed(final byte[] decompressed) throws PrometheusParseException {
        final Remote.WriteRequest writeRequest = parseProtobuf(decompressed);
        return convertToRecords(writeRequest);
    }

    private static Remote.WriteRequest parseProtobuf(final byte[] data) throws PrometheusParseException {
        try {
            return Remote.WriteRequest.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            LOG.error("Failed to parse protobuf WriteRequest: {}", e.getMessage());
            throw new PrometheusParseException("Failed to parse Prometheus Remote Write protobuf", e);
        }
    }

    /**
     * Converts a WriteRequest to records using a two-pass grouping strategy.
     */
    private List<Record<Event>> convertToRecords(final Remote.WriteRequest writeRequest) {
        final List<Record<Event>> records = new ArrayList<>();
        final List<Types.TimeSeries> timeSeriesList = writeRequest.getTimeseriesList();

        final List<ParsedLabels> allParsedLabels = new ArrayList<>(timeSeriesList.size());
        for (final Types.TimeSeries timeSeries : timeSeriesList) {
            allParsedLabels.add(parseLabels(timeSeries));
        }

        final Set<String> histogramBaseNames = new HashSet<>();
        final Set<String> summaryBaseNames = new HashSet<>();

        for (final ParsedLabels labels : allParsedLabels) {
            if (labels.metricName.endsWith(BUCKET_SUFFIX) && labels.hasLe) {
                histogramBaseNames.add(
                        labels.metricName.substring(0, labels.metricName.length() - BUCKET_SUFFIX.length()));
            } else if (labels.hasQuantile) {
                summaryBaseNames.add(labels.metricName);
            }
        }

        final Map<String, HistogramGroup> histogramGroups = new LinkedHashMap<>();
        final Map<String, SummaryGroup> summaryGroups = new LinkedHashMap<>();
        final List<StandaloneTimeSeries> standaloneList = new ArrayList<>();

        for (int i = 0; i < timeSeriesList.size(); i++) {
            final Types.TimeSeries timeSeries = timeSeriesList.get(i);
            final ParsedLabels labels = allParsedLabels.get(i);
            final String name = labels.metricName;

            if (name.endsWith(BUCKET_SUFFIX) && labels.hasLe) {
                final String base = name.substring(0, name.length() - BUCKET_SUFFIX.length());
                final String groupKey = base + "|" + labels.commonLabelKey;
                histogramGroups.computeIfAbsent(groupKey, k -> new HistogramGroup(base))
                        .addBucket(timeSeries, labels);

            } else if (labels.hasQuantile) {
                final String groupKey = name + "|" + labels.commonLabelKey;
                summaryGroups.computeIfAbsent(groupKey, k -> new SummaryGroup(name))
                        .addQuantile(timeSeries, labels);

            } else if (name.endsWith(COUNT_SUFFIX)) {
                final String base = name.substring(0, name.length() - COUNT_SUFFIX.length());
                if (histogramBaseNames.contains(base)) {
                    final String groupKey = base + "|" + labels.commonLabelKey;
                    histogramGroups.computeIfAbsent(groupKey, k -> new HistogramGroup(base))
                            .setCount(timeSeries);
                } else if (summaryBaseNames.contains(base)) {
                    final String groupKey = base + "|" + labels.commonLabelKey;
                    summaryGroups.computeIfAbsent(groupKey, k -> new SummaryGroup(base))
                            .setCount(timeSeries);
                } else {
                    standaloneList.add(new StandaloneTimeSeries(timeSeries, labels, false));
                }

            } else if (name.endsWith(SUM_SUFFIX)) {
                final String base = name.substring(0, name.length() - SUM_SUFFIX.length());
                if (histogramBaseNames.contains(base)) {
                    final String groupKey = base + "|" + labels.commonLabelKey;
                    histogramGroups.computeIfAbsent(groupKey, k -> new HistogramGroup(base))
                            .setSum(timeSeries);
                } else if (summaryBaseNames.contains(base)) {
                    final String groupKey = base + "|" + labels.commonLabelKey;
                    summaryGroups.computeIfAbsent(groupKey, k -> new SummaryGroup(base))
                            .setSum(timeSeries);
                } else {
                    standaloneList.add(new StandaloneTimeSeries(timeSeries, labels, false));
                }

            } else if (isCounter(name)) {
                standaloneList.add(new StandaloneTimeSeries(timeSeries, labels, true));

            } else {
                standaloneList.add(new StandaloneTimeSeries(timeSeries, labels, false));
            }
        }

        for (final HistogramGroup group : histogramGroups.values()) {
            records.addAll(convertHistogramGroup(group));
        }

        for (final SummaryGroup group : summaryGroups.values()) {
            records.addAll(convertSummaryGroup(group));
        }

        for (final StandaloneTimeSeries standalone : standaloneList) {
            records.addAll(convertStandalone(standalone));
        }

        LOG.debug("Converted {} time series to {} records", writeRequest.getTimeseriesCount(), records.size());
        return records;
    }

    /**
     * Parses labels from a TimeSeries, extracting the metric name, detecting special labels,
     * and computing a common label key for label-set grouping.
     */
    private static ParsedLabels parseLabels(final Types.TimeSeries timeSeries) {
        String metricName = DEFAULT_METRIC_NAME;
        final Map<String, Object> attributes = new HashMap<>();
        boolean hasLe = false;
        boolean hasQuantile = false;

        for (final Types.Label label : timeSeries.getLabelsList()) {
            if (METRIC_NAME_LABEL.equals(label.getName())) {
                metricName = label.getValue();
            } else {
                attributes.put(label.getName(), label.getValue());
                if (LE_LABEL.equals(label.getName())) {
                    hasLe = true;
                } else if (QUANTILE_LABEL.equals(label.getName())) {
                    hasQuantile = true;
                }
            }
        }

        final Map<String, Object> commonLabels = new HashMap<>(attributes);
        commonLabels.remove(LE_LABEL);
        commonLabels.remove(QUANTILE_LABEL);
        final String commonLabelKey = buildSortedLabelKey(commonLabels);

        return new ParsedLabels(metricName, attributes, Collections.unmodifiableMap(commonLabels),
                commonLabelKey, hasLe, hasQuantile);
    }

    /**
     * Builds a deterministic string key from a label map for grouping.
     * Labels are sorted by key to ensure consistent grouping regardless of insertion order.
     */
    private static String buildSortedLabelKey(final Map<String, Object> labels) {
        if (labels.isEmpty()) {
            return "";
        }
        final TreeMap<String, Object> sorted = new TreeMap<>(labels);
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, Object> entry : sorted.entrySet()) {
            if (sb.length() > 0) {
                sb.append(';');
            }
            sb.append(entry.getKey()).append(';').append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Converts a histogram group to one or more events, one per distinct sample timestamp
     * across all bucket TimeSeries
     */
    private List<Record<Event>> convertHistogramGroup(final HistogramGroup group) {
        final Map<Long, Boolean> timestampOrder = new LinkedHashMap<>();
        for (final BucketEntry bucket : group.buckets) {
            for (final Types.Sample sample : bucket.timeSeries.getSamplesList()) {
                timestampOrder.put(sample.getTimestamp(), Boolean.TRUE);
            }
        }

        if (timestampOrder.isEmpty()) {
            return new ArrayList<>();
        }

        final List<Record<Event>> records = new ArrayList<>();
        final Map<String, Object> commonAttributes = new HashMap<>(group.buckets.get(0).labels.commonLabels);
        final String serviceName = extractServiceName(commonAttributes);
        final Instant timeReceived = Instant.now();

        for (final long ts : timestampOrder.keySet()) {
            final TreeMap<Double, Long> cumulativeBuckets = new TreeMap<>();
            for (final BucketEntry bucket : group.buckets) {
                final Double leBound = parseLeValue((String) bucket.labels.attributes.get(LE_LABEL));
                if (leBound == null) {
                    continue;
                }
                for (final Types.Sample sample : bucket.timeSeries.getSamplesList()) {
                    if (sample.getTimestamp() == ts) {
                        cumulativeBuckets.put(leBound, (long) sample.getValue());
                        break;
                    }
                }
            }

            if (cumulativeBuckets.isEmpty()) {
                continue;
            }

            final List<Double> explicitBounds = new ArrayList<>();
            final List<Long> perBucketCounts = new ArrayList<>();
            long previousCumulative = 0;
            for (final Map.Entry<Double, Long> entry : cumulativeBuckets.entrySet()) {
                long perBucket = entry.getValue() - previousCumulative;
                if (perBucket < 0) {
                    LOG.warn("Negative per-bucket count detected for histogram '{}' at le={}: cumulative={}, previous={}. Clamping to 0.",
                            group.baseName, entry.getKey(), entry.getValue(), previousCumulative);
                    perBucket = 0;
                }
                perBucketCounts.add(perBucket);
                if (!Double.isInfinite(entry.getKey())) {
                    explicitBounds.add(entry.getKey());
                }
                previousCumulative = entry.getValue();
            }

            double sumValue = 0.0;
            long countValue = 0;
            if (group.countTimeSeries != null) {
                countValue = getSampleValueAtTimestamp(group.countTimeSeries, ts);
            }
            if (group.sumTimeSeries != null) {
                sumValue = getSampleDoubleAtTimestamp(group.sumTimeSeries, ts);
            }

            final String timestamp = resolveTimestamp(ts);

            final Event event = JacksonHistogram.builder()
                    .withName(group.baseName)
                    .withTime(timestamp)
                    .withSum(sumValue)
                    .withCount(countValue)
                    .withBucketCountsList(perBucketCounts)
                    .withExplicitBoundsList(explicitBounds)
                    .withBucketCount(perBucketCounts.size())
                    .withExplicitBoundsCount(explicitBounds.size())
                    .withAggregationTemporality("AGGREGATION_TEMPORALITY_CUMULATIVE")
                    .withAttributes(new HashMap<>(commonAttributes))
                    .withServiceName(serviceName)
                    .withTimeReceived(timeReceived)
                    .build(config.isFlattenLabels());

            records.add(new Record<>(event));
        }

        return records;
    }

    /**
     * Converts a summary group to one or more events, one per distinct sample timestamp
     * across all quantile TimeSeries
     */
    private List<Record<Event>> convertSummaryGroup(final SummaryGroup group) {
        final Map<Long, Boolean> timestampOrder = new LinkedHashMap<>();
        for (final QuantileEntry qe : group.quantiles) {
            for (final Types.Sample sample : qe.timeSeries.getSamplesList()) {
                timestampOrder.put(sample.getTimestamp(), Boolean.TRUE);
            }
        }

        if (timestampOrder.isEmpty()) {
            return new ArrayList<>();
        }

        final List<Record<Event>> records = new ArrayList<>();
        final Map<String, Object> commonAttributes = new HashMap<>(group.quantiles.get(0).labels.commonLabels);
        final String serviceName = extractServiceName(commonAttributes);
        final Instant timeReceived = Instant.now();

        for (final long ts : timestampOrder.keySet()) {
            final List<Quantile> quantiles = new ArrayList<>();

            for (final QuantileEntry qe : group.quantiles) {
                final Double quantileValue = parseQuantileValue(
                        (String) qe.labels.attributes.get(QUANTILE_LABEL));
                if (quantileValue == null) {
                    continue;
                }
                for (final Types.Sample sample : qe.timeSeries.getSamplesList()) {
                    if (sample.getTimestamp() == ts) {
                        quantiles.add(new DefaultQuantile(quantileValue, sample.getValue()));
                        break;
                    }
                }
            }

            if (quantiles.isEmpty()) {
                continue;
            }

            double sumValue = 0.0;
            long countValue = 0;
            if (group.countTimeSeries != null) {
                countValue = getSampleValueAtTimestamp(group.countTimeSeries, ts);
            }
            if (group.sumTimeSeries != null) {
                sumValue = getSampleDoubleAtTimestamp(group.sumTimeSeries, ts);
            }

            final String timestamp = resolveTimestamp(ts);

            final Event event = JacksonSummary.builder()
                    .withName(group.baseName)
                    .withTime(timestamp)
                    .withSum(sumValue)
                    .withCount(countValue)
                    .withQuantiles(quantiles)
                    .withQuantilesValueCount(quantiles.size())
                    .withAttributes(new HashMap<>(commonAttributes))
                    .withServiceName(serviceName)
                    .withTimeReceived(timeReceived)
                    .build(config.isFlattenLabels());

            records.add(new Record<>(event));
        }

        return records;
    }

    private List<Record<Event>> convertStandalone(final StandaloneTimeSeries standalone) {
        final List<Record<Event>> records = new ArrayList<>();
        final String serviceName = extractServiceName(standalone.labels.attributes);
        final Instant timeReceived = Instant.now();

        for (final Types.Sample sample : standalone.timeSeries.getSamplesList()) {
            final String timestamp = resolveTimestamp(sample.getTimestamp());

            if (standalone.isCounter) {
                final String counterName = stripCounterSuffix(standalone.labels.metricName);
                records.add(new Record<>(JacksonSum.builder()
                        .withName(counterName)
                        .withTime(timestamp)
                        .withValue(sample.getValue())
                        .withAttributes(new HashMap<>(standalone.labels.attributes))
                        .withIsMonotonic(true)
                        .withAggregationTemporality("AGGREGATION_TEMPORALITY_CUMULATIVE")
                        .withServiceName(serviceName)
                        .withTimeReceived(timeReceived)
                        .build(config.isFlattenLabels())));
            } else {
                records.add(new Record<>(JacksonGauge.builder()
                        .withName(standalone.labels.metricName)
                        .withTime(timestamp)
                        .withValue(sample.getValue())
                        .withAttributes(new HashMap<>(standalone.labels.attributes))
                        .withServiceName(serviceName)
                        .withTimeReceived(timeReceived)
                        .build(config.isFlattenLabels())));
            }
        }

        return records;
    }

    /**
     * Extracts the service name from attributes using priority order:
     * service.name > service_name > job > empty string.
     */
    static String extractServiceName(final Map<String, Object> attributes) {
        if (attributes.containsKey(SERVICE_NAME_LABEL)) {
            return (String) attributes.get(SERVICE_NAME_LABEL);
        }
        if (attributes.containsKey(SERVICE_NAME_UNDERSCORE_LABEL)) {
            return (String) attributes.get(SERVICE_NAME_UNDERSCORE_LABEL);
        }
        if (attributes.containsKey(JOB_LABEL)) {
            return (String) attributes.get(JOB_LABEL);
        }
        return "";
    }

    /**
     * Strips the {@code _total} or {@code _created} suffix from counter metric names.
     */
    static String stripCounterSuffix(final String metricName) {
        if (metricName.endsWith(TOTAL_SUFFIX)) {
            return metricName.substring(0, metricName.length() - TOTAL_SUFFIX.length());
        }
        if (metricName.endsWith(CREATED_SUFFIX)) {
            return metricName.substring(0, metricName.length() - CREATED_SUFFIX.length());
        }
        return metricName;
    }

    /**
     * Infers whether a metric is a counter (Sum) based on its name suffix.
     *
     * @param metricName the metric name
     * @return true if the metric is a counter
     */
    static boolean isCounter(final String metricName) {
        return metricName.endsWith(TOTAL_SUFFIX) || metricName.endsWith(CREATED_SUFFIX);
    }

    /**
     * Resolves a timestamp, using current time if the value is 0.
     */
    private static String resolveTimestamp(final long timestampMs) {
        if (timestampMs == 0) {
            return Instant.now().toString();
        }
        return Instant.ofEpochMilli(timestampMs).toString();
    }

    /**
     * Parses an {@code le} label value to a Double. Returns null if unparseable.
     */
    static Double parseLeValue(final String leValue) {
        if (leValue == null) {
            return null;
        }
        if ("+Inf".equals(leValue)) {
            return Double.POSITIVE_INFINITY;
        }
        try {
            return Double.parseDouble(leValue);
        } catch (final NumberFormatException e) {
            LOG.warn("Skipping histogram bucket with unparseable le value: '{}'", leValue);
            return null;
        }
    }

    /**
     * Parses a {@code quantile} label value to a Double. Returns null if unparseable.
     */
    static Double parseQuantileValue(final String quantileValue) {
        if (quantileValue == null) {
            return null;
        }
        try {
            return Double.parseDouble(quantileValue);
        } catch (final NumberFormatException e) {
            LOG.warn("Skipping summary quantile with unparseable value: '{}'", quantileValue);
            return null;
        }
    }

    /**
     * Gets the sample value as long at a given timestamp from a TimeSeries.
     * Returns 0 if no sample exists at the requested timestamp.
     */
    private static long getSampleValueAtTimestamp(final Types.TimeSeries ts, final long timestamp) {
        for (final Types.Sample sample : ts.getSamplesList()) {
            if (sample.getTimestamp() == timestamp) {
                return (long) sample.getValue();
            }
        }
        return 0;
    }

    /**
     * Gets the sample value as double at a given timestamp from a TimeSeries.
     * Returns 0.0 if no sample exists at the requested timestamp.
     */
    private static double getSampleDoubleAtTimestamp(final Types.TimeSeries ts, final long timestamp) {
        for (final Types.Sample sample : ts.getSamplesList()) {
            if (sample.getTimestamp() == timestamp) {
                return sample.getValue();
            }
        }
        return 0.0;
    }

    private static class ParsedLabels {
        final String metricName;
        final Map<String, Object> attributes;
        final Map<String, Object> commonLabels;
        final String commonLabelKey;
        final boolean hasLe;
        final boolean hasQuantile;

        ParsedLabels(final String metricName, final Map<String, Object> attributes,
                     final Map<String, Object> commonLabels, final String commonLabelKey,
                     final boolean hasLe, final boolean hasQuantile) {
            this.metricName = metricName;
            this.attributes = attributes;
            this.commonLabels = commonLabels;
            this.commonLabelKey = commonLabelKey;
            this.hasLe = hasLe;
            this.hasQuantile = hasQuantile;
        }
    }

    private static class BucketEntry {
        final Types.TimeSeries timeSeries;
        final ParsedLabels labels;

        BucketEntry(final Types.TimeSeries timeSeries, final ParsedLabels labels) {
            this.timeSeries = timeSeries;
            this.labels = labels;
        }
    }

    private static class QuantileEntry {
        final Types.TimeSeries timeSeries;
        final ParsedLabels labels;

        QuantileEntry(final Types.TimeSeries timeSeries, final ParsedLabels labels) {
            this.timeSeries = timeSeries;
            this.labels = labels;
        }
    }

    private static class HistogramGroup {
        final String baseName;
        final List<BucketEntry> buckets = new ArrayList<>();
        Types.TimeSeries countTimeSeries;
        Types.TimeSeries sumTimeSeries;

        HistogramGroup(final String baseName) {
            this.baseName = baseName;
        }

        void addBucket(final Types.TimeSeries ts, final ParsedLabels labels) {
            buckets.add(new BucketEntry(ts, labels));
        }

        void setCount(final Types.TimeSeries ts) {
            countTimeSeries = ts;
        }

        void setSum(final Types.TimeSeries ts) {
            sumTimeSeries = ts;
        }
    }

    private static class SummaryGroup {
        final String baseName;
        final List<QuantileEntry> quantiles = new ArrayList<>();
        Types.TimeSeries countTimeSeries;
        Types.TimeSeries sumTimeSeries;

        SummaryGroup(final String baseName) {
            this.baseName = baseName;
        }

        void addQuantile(final Types.TimeSeries ts, final ParsedLabels labels) {
            quantiles.add(new QuantileEntry(ts, labels));
        }

        void setCount(final Types.TimeSeries ts) {
            countTimeSeries = ts;
        }

        void setSum(final Types.TimeSeries ts) {
            sumTimeSeries = ts;
        }
    }

    private static class StandaloneTimeSeries {
        final Types.TimeSeries timeSeries;
        final ParsedLabels labels;
        final boolean isCounter;

        StandaloneTimeSeries(final Types.TimeSeries timeSeries, final ParsedLabels labels,
                             final boolean isCounter) {
            this.timeSeries = timeSeries;
            this.labels = labels;
            this.isCounter = isCounter;
        }
    }
}
