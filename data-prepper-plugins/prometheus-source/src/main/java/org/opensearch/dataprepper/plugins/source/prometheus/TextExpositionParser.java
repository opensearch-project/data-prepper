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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TextExpositionParser {

    private static final Logger LOG = LoggerFactory.getLogger(TextExpositionParser.class);
    private static final String TOTAL_SUFFIX = "_total";
    private static final String CREATED_SUFFIX = "_created";
    private static final String BUCKET_SUFFIX = "_bucket";
    private static final String COUNT_SUFFIX = "_count";
    private static final String SUM_SUFFIX = "_sum";
    private static final String LE_LABEL = "le";
    private static final String QUANTILE_LABEL = "quantile";

    private static final String TYPE_COUNTER = "counter";
    private static final String TYPE_GAUGE = "gauge";
    private static final String TYPE_HISTOGRAM = "histogram";
    private static final String TYPE_SUMMARY = "summary";
    private static final String TYPE_UNTYPED = "untyped";

    private static final String AGGREGATION_TEMPORALITY_CUMULATIVE = PrometheusMetricUtils.AGGREGATION_TEMPORALITY_CUMULATIVE;

    private static final String[] TYPE_LOOKUP_SUFFIXES = {BUCKET_SUFFIX, COUNT_SUFFIX, SUM_SUFFIX, TOTAL_SUFFIX, CREATED_SUFFIX};

    private final boolean flattenLabels;

    public TextExpositionParser(final boolean flattenLabels) {
        this.flattenLabels = flattenLabels;
    }

    public List<Record<Event>> parse(final String body) {
        if (body == null || body.isEmpty()) {
            return Collections.emptyList();
        }

        final Map<String, String> declaredTypes = new HashMap<>();
        final List<ParsedSample> samples = new ArrayList<>();

        final String[] lines = body.split("\n");
        for (final String rawLine : lines) {
            final String line = rawLine.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("# TYPE ")) {
                parseTypeLine(line, declaredTypes);
            } else if (line.startsWith("#")) {
                continue;
            } else {
                final ParsedSample sample = parseSampleLine(line);
                if (sample != null) {
                    samples.add(sample);
                }
            }
        }

        final Instant timeReceived = Instant.now();
        return convertSamples(samples, declaredTypes, timeReceived);
    }

    void parseTypeLine(final String line, final Map<String, String> declaredTypes) {
        final String rest = line.substring("# TYPE ".length()).trim();
        final int spaceIdx = rest.indexOf(' ');
        if (spaceIdx < 0) {
            return;
        }
        final String metricName = rest.substring(0, spaceIdx);
        final String typeName = rest.substring(spaceIdx + 1).trim().toLowerCase();
        declaredTypes.put(metricName, typeName);
    }

    ParsedSample parseSampleLine(final String line) {
        try {
            int idx = 0;
            final int len = line.length();

            final int nameEnd = findNameEnd(line, idx);
            if (nameEnd <= idx) {
                return null;
            }
            final String name = line.substring(idx, nameEnd);
            idx = nameEnd;

            final Map<String, String> labels = new LinkedHashMap<>();
            if (idx < len && line.charAt(idx) == '{') {
                idx = parseLabels(line, idx, labels);
            }

            while (idx < len && Character.isWhitespace(line.charAt(idx))) {
                idx++;
            }

            final int valueStart = idx;
            while (idx < len && !Character.isWhitespace(line.charAt(idx))) {
                idx++;
            }
            if (valueStart == idx) {
                return null;
            }
            final String valueStr = line.substring(valueStart, idx);
            final double value = parseValue(valueStr);

            while (idx < len && Character.isWhitespace(line.charAt(idx))) {
                idx++;
            }

            Long timestampMs = null;
            if (idx < len) {
                final String tsStr = line.substring(idx).trim();
                if (!tsStr.isEmpty()) {
                    try {
                        if (tsStr.contains(".")) {
                            timestampMs = (long) (Double.parseDouble(tsStr) * 1000);
                        } else {
                            timestampMs = Long.parseLong(tsStr);
                        }
                    } catch (final NumberFormatException e) {
                        LOG.warn("Unparseable timestamp '{}' in line: {}", tsStr, line);
                    }
                }
            }

            return new ParsedSample(name, labels, value, timestampMs);
        } catch (final Exception e) {
            LOG.warn("Failed to parse exposition line: '{}': {}", line, e.getMessage());
            return null;
        }
    }

    int parseLabels(final String line, final int startIdx, final Map<String, String> labels) {
        int idx = startIdx + 1;
        final int len = line.length();

        while (idx < len) {
            while (idx < len && Character.isWhitespace(line.charAt(idx))) {
                idx++;
            }
            if (idx < len && line.charAt(idx) == '}') {
                return idx + 1;
            }
            if (idx < len && line.charAt(idx) == ',') {
                idx++;
                continue;
            }

            final int keyStart = idx;
            while (idx < len && line.charAt(idx) != '=' && line.charAt(idx) != '}') {
                idx++;
            }
            if (idx >= len || line.charAt(idx) != '=') {
                break;
            }
            final String key = line.substring(keyStart, idx).trim();
            idx++;

            if (idx >= len || line.charAt(idx) != '"') {
                break;
            }
            idx++;

            final StringBuilder valueBuilder = new StringBuilder();
            while (idx < len) {
                final char c = line.charAt(idx);
                if (c == '\\' && idx + 1 < len) {
                    final char next = line.charAt(idx + 1);
                    if (next == '"') {
                        valueBuilder.append('"');
                        idx += 2;
                    } else if (next == '\\') {
                        valueBuilder.append('\\');
                        idx += 2;
                    } else if (next == 'n') {
                        valueBuilder.append('\n');
                        idx += 2;
                    } else {
                        valueBuilder.append(c);
                        idx++;
                    }
                } else if (c == '"') {
                    idx++;
                    break;
                } else {
                    valueBuilder.append(c);
                    idx++;
                }
            }

            labels.put(key, valueBuilder.toString());
        }

        return idx;
    }

    private List<Record<Event>> convertSamples(final List<ParsedSample> samples,
                                                final Map<String, String> declaredTypes,
                                                final Instant timeReceived) {
        final List<Record<Event>> records = new ArrayList<>();
        final Map<String, HistogramAccumulator> histogramAccumulators = new LinkedHashMap<>();
        final Map<String, SummaryAccumulator> summaryAccumulators = new LinkedHashMap<>();

        for (final ParsedSample sample : samples) {
            final String resolvedType = resolveType(sample.name, declaredTypes);

            switch (resolvedType) {
                case TYPE_COUNTER:
                    if (sample.name.endsWith(CREATED_SUFFIX)) {
                        break;
                    }
                    records.add(buildSumRecord(sample, timeReceived));
                    break;
                case TYPE_GAUGE:
                    records.add(buildGaugeRecord(sample, timeReceived));
                    break;
                case TYPE_HISTOGRAM:
                    accumulateHistogram(sample, histogramAccumulators);
                    break;
                case TYPE_SUMMARY:
                    accumulateSummary(sample, summaryAccumulators);
                    break;
                default:
                    records.add(buildGaugeRecord(sample, timeReceived));
                    break;
            }
        }

        for (final HistogramAccumulator acc : histogramAccumulators.values()) {
            final Record<Event> record = buildHistogramRecord(acc, timeReceived);
            if (record != null) {
                records.add(record);
            }
        }

        for (final SummaryAccumulator acc : summaryAccumulators.values()) {
            final Record<Event> record = buildSummaryRecord(acc, timeReceived);
            if (record != null) {
                records.add(record);
            }
        }

        return records;
    }

    String resolveType(final String sampleName, final Map<String, String> declaredTypes) {
        if (declaredTypes.containsKey(sampleName)) {
            return declaredTypes.get(sampleName);
        }

        for (final String suffix : TYPE_LOOKUP_SUFFIXES) {
            if (sampleName.endsWith(suffix)) {
                final String base = sampleName.substring(0, sampleName.length() - suffix.length());
                if (declaredTypes.containsKey(base)) {
                    return declaredTypes.get(base);
                }
            }
        }

        return TYPE_GAUGE;
    }

    private Record<Event> buildGaugeRecord(final ParsedSample sample, final Instant timeReceived) {
        final Map<String, Object> attributes = new HashMap<>(sample.labels);
        final String serviceName = extractServiceName(attributes);
        final String timestamp = resolveTimestamp(sample.timestampMs, timeReceived);

        final Event event = JacksonGauge.builder()
                .withName(sample.name)
                .withTime(timestamp)
                .withValue(sample.value)
                .withAttributes(attributes)
                .withServiceName(serviceName)
                .withTimeReceived(timeReceived)
                .build(flattenLabels);

        return new Record<>(event);
    }

    private Record<Event> buildSumRecord(final ParsedSample sample, final Instant timeReceived) {
        final Map<String, Object> attributes = new HashMap<>(sample.labels);
        final String serviceName = extractServiceName(attributes);
        final String timestamp = resolveTimestamp(sample.timestampMs, timeReceived);

        final Event event = JacksonSum.builder()
                .withName(stripCounterSuffix(sample.name))
                .withTime(timestamp)
                .withValue(sample.value)
                .withAttributes(attributes)
                .withIsMonotonic(true)
                .withAggregationTemporality(AGGREGATION_TEMPORALITY_CUMULATIVE)
                .withServiceName(serviceName)
                .withTimeReceived(timeReceived)
                .build(flattenLabels);

        return new Record<>(event);
    }

    private void accumulateHistogram(final ParsedSample sample,
                                     final Map<String, HistogramAccumulator> accumulators) {
        final String baseName = deriveHistogramBaseName(sample.name);
        final Map<String, String> commonLabels = new LinkedHashMap<>(sample.labels);
        commonLabels.remove(LE_LABEL);
        final String groupKey = baseName + "|" + buildSortedLabelKey(commonLabels);

        final HistogramAccumulator acc = accumulators.computeIfAbsent(groupKey,
                k -> new HistogramAccumulator(baseName, commonLabels, sample.timestampMs));

        if (sample.name.endsWith(BUCKET_SUFFIX)) {
            final String leStr = sample.labels.get(LE_LABEL);
            final Double leBound = parseLeValue(leStr);
            if (leBound != null && !Double.isNaN(sample.value)) {
                acc.cumulativeBuckets.put(leBound, (long) sample.value);
            }
        } else if (sample.name.endsWith(COUNT_SUFFIX)) {
            if (!Double.isNaN(sample.value)) {
                acc.countValue = (long) sample.value;
            }
        } else if (sample.name.endsWith(SUM_SUFFIX)) {
            acc.sumValue = sample.value;
        }
    }

    private void accumulateSummary(final ParsedSample sample,
                                   final Map<String, SummaryAccumulator> accumulators) {
        final String baseName = deriveSummaryBaseName(sample.name);
        final Map<String, String> commonLabels = new LinkedHashMap<>(sample.labels);
        commonLabels.remove(QUANTILE_LABEL);
        final String groupKey = baseName + "|" + buildSortedLabelKey(commonLabels);

        final SummaryAccumulator acc = accumulators.computeIfAbsent(groupKey,
                k -> new SummaryAccumulator(baseName, commonLabels, sample.timestampMs));

        if (sample.labels.containsKey(QUANTILE_LABEL)) {
            final Double quantile = parseQuantileValue(sample.labels.get(QUANTILE_LABEL));
            if (quantile != null) {
                acc.quantiles.add(new DefaultQuantile(quantile, sample.value));
            }
        } else if (sample.name.endsWith(COUNT_SUFFIX)) {
            if (!Double.isNaN(sample.value)) {
                acc.countValue = (long) sample.value;
            }
        } else if (sample.name.endsWith(SUM_SUFFIX)) {
            acc.sumValue = sample.value;
        }
    }

    private Record<Event> buildHistogramRecord(final HistogramAccumulator acc, final Instant timeReceived) {
        if (acc.cumulativeBuckets.isEmpty() && acc.countValue == 0 && acc.sumValue == 0.0) {
            return null;
        }

        final List<Double> explicitBounds = new ArrayList<>();
        final List<Long> perBucketCounts = new ArrayList<>();
        long previousCumulative = 0;

        for (final Map.Entry<Double, Long> entry : acc.cumulativeBuckets.entrySet()) {
            long perBucket = entry.getValue() - previousCumulative;
            if (perBucket < 0) {
                LOG.warn("Negative per-bucket count for histogram '{}' at le={}: cumulative={}, previous={}. Clamping to 0.",
                        acc.baseName, entry.getKey(), entry.getValue(), previousCumulative);
                perBucket = 0;
            }
            previousCumulative = Math.max(previousCumulative, entry.getValue());
            perBucketCounts.add(perBucket);
            if (!Double.isInfinite(entry.getKey())) {
                explicitBounds.add(entry.getKey());
            }
        }

        final Map<String, Object> attributes = new HashMap<>(acc.commonLabels);
        final String serviceName = extractServiceName(attributes);
        final String timestamp = resolveTimestamp(acc.timestampMs, timeReceived);

        final Event event = JacksonHistogram.builder()
                .withName(acc.baseName)
                .withTime(timestamp)
                .withSum(acc.sumValue)
                .withCount(acc.countValue)
                .withBucketCountsList(perBucketCounts)
                .withExplicitBoundsList(explicitBounds)
                .withBucketCount(perBucketCounts.size())
                .withExplicitBoundsCount(explicitBounds.size())
                .withAggregationTemporality(AGGREGATION_TEMPORALITY_CUMULATIVE)
                .withAttributes(attributes)
                .withServiceName(serviceName)
                .withTimeReceived(timeReceived)
                .build(flattenLabels);

        return new Record<>(event);
    }

    private Record<Event> buildSummaryRecord(final SummaryAccumulator acc, final Instant timeReceived) {
        if (acc.quantiles.isEmpty() && acc.countValue == 0 && acc.sumValue == 0.0) {
            return null;
        }

        final Map<String, Object> attributes = new HashMap<>(acc.commonLabels);
        final String serviceName = extractServiceName(attributes);
        final String timestamp = resolveTimestamp(acc.timestampMs, timeReceived);

        final Event event = JacksonSummary.builder()
                .withName(acc.baseName)
                .withTime(timestamp)
                .withSum(acc.sumValue)
                .withCount(acc.countValue)
                .withQuantiles(acc.quantiles)
                .withQuantilesValueCount(acc.quantiles.size())
                .withAttributes(attributes)
                .withServiceName(serviceName)
                .withTimeReceived(timeReceived)
                .build(flattenLabels);

        return new Record<>(event);
    }

    static String extractServiceName(final Map<String, Object> attributes) {
        return PrometheusMetricUtils.extractServiceName(attributes);
    }

    static String stripCounterSuffix(final String metricName) {
        return PrometheusMetricUtils.stripCounterSuffix(metricName);
    }

    static String resolveTimestamp(final Long timestampMs, final Instant timeReceived) {
        if (timestampMs == null) {
            return timeReceived.toString();
        }
        return Instant.ofEpochMilli(timestampMs).toString();
    }

    static Double parseLeValue(final String leValue) {
        return PrometheusMetricUtils.parseLeValue(leValue);
    }

    static Double parseQuantileValue(final String quantileValue) {
        return PrometheusMetricUtils.parseQuantileValue(quantileValue);
    }

    static double parseValue(final String valueStr) {
        if ("NaN".equalsIgnoreCase(valueStr)) {
            return Double.NaN;
        }
        if ("+Inf".equals(valueStr)) {
            return Double.POSITIVE_INFINITY;
        }
        if ("-Inf".equals(valueStr)) {
            return Double.NEGATIVE_INFINITY;
        }
        return Double.parseDouble(valueStr);
    }

    private static int findNameEnd(final String line, final int start) {
        int idx = start;
        final int len = line.length();
        while (idx < len) {
            final char c = line.charAt(idx);
            if (c == '{' || Character.isWhitespace(c)) {
                break;
            }
            idx++;
        }
        return idx;
    }

    String deriveHistogramBaseName(final String sampleName) {
        if (sampleName.endsWith(BUCKET_SUFFIX)) {
            return sampleName.substring(0, sampleName.length() - BUCKET_SUFFIX.length());
        }
        if (sampleName.endsWith(COUNT_SUFFIX)) {
            return sampleName.substring(0, sampleName.length() - COUNT_SUFFIX.length());
        }
        if (sampleName.endsWith(SUM_SUFFIX)) {
            return sampleName.substring(0, sampleName.length() - SUM_SUFFIX.length());
        }
        return sampleName;
    }

    String deriveSummaryBaseName(final String sampleName) {
        if (sampleName.endsWith(COUNT_SUFFIX)) {
            return sampleName.substring(0, sampleName.length() - COUNT_SUFFIX.length());
        }
        if (sampleName.endsWith(SUM_SUFFIX)) {
            return sampleName.substring(0, sampleName.length() - SUM_SUFFIX.length());
        }
        return sampleName;
    }

    static String buildSortedLabelKey(final Map<String, String> labels) {
        if (labels.isEmpty()) {
            return "";
        }
        final TreeMap<String, String> sorted = new TreeMap<>(labels);
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, String> entry : sorted.entrySet()) {
            if (sb.length() > 0) {
                sb.append(';');
            }
            sb.append(entry.getKey()).append(';').append(entry.getValue());
        }
        return sb.toString();
    }

    static class ParsedSample {
        final String name;
        final Map<String, String> labels;
        final double value;
        final Long timestampMs;

        ParsedSample(final String name, final Map<String, String> labels,
                     final double value, final Long timestampMs) {
            this.name = name;
            this.labels = labels;
            this.value = value;
            this.timestampMs = timestampMs;
        }
    }

    static class HistogramAccumulator {
        final String baseName;
        final Map<String, String> commonLabels;
        final Long timestampMs;
        final TreeMap<Double, Long> cumulativeBuckets = new TreeMap<>();
        long countValue;
        double sumValue;

        HistogramAccumulator(final String baseName, final Map<String, String> commonLabels,
                             final Long timestampMs) {
            this.baseName = baseName;
            this.commonLabels = commonLabels;
            this.timestampMs = timestampMs;
        }
    }

    static class SummaryAccumulator {
        final String baseName;
        final Map<String, String> commonLabels;
        final Long timestampMs;
        final List<Quantile> quantiles = new ArrayList<>();
        long countValue;
        double sumValue;

        SummaryAccumulator(final String baseName, final Map<String, String> commonLabels,
                           final Long timestampMs) {
            this.baseName = baseName;
            this.commonLabels = commonLabels;
            this.timestampMs = timestampMs;
        }
    }
}