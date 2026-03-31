/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class TSDBDocumentBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(TSDBDocumentBuilder.class);
    private static final String NAME_LABEL = "__name__";
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    public List<String> build(final Event event) {
        if (!(event instanceof Metric)) {
            throw new IllegalArgumentException(
                    "TSDB index_type requires Metric events. Received: " + event.getClass().getName());
        }

        final Metric metric = (Metric) event;
        final long timestamp = parseTimestamp(metric.getTime());
        final Map<String, Object> attributes = metric.getAttributes() != null
                ? metric.getAttributes() : Collections.emptyMap();
        final String[][] sortedAttrs = sortAndSanitizeAttributes(attributes);

        if (metric instanceof Gauge) {
            return buildGauge((Gauge) metric, sortedAttrs, timestamp);
        } else if (metric instanceof Sum) {
            return buildSum((Sum) metric, sortedAttrs, timestamp);
        } else if (metric instanceof Histogram) {
            return buildHistogram((Histogram) metric, sortedAttrs, timestamp);
        } else if (metric instanceof Summary) {
            return buildSummary((Summary) metric, sortedAttrs, timestamp);
        }

        LOG.warn("Unsupported metric kind '{}' for metric '{}', building single document from value 0.0",
                metric.getKind(), metric.getName());
        return List.of(buildJsonDoc(buildLabels(metric.getName(), sortedAttrs, null, null), timestamp, 0.0));
    }

    private static List<String> buildGauge(final Gauge gauge, final String[][] sortedAttrs, final long timestamp) {
        final double value = gauge.getValue() != null ? gauge.getValue() : 0.0;
        final String labels = buildLabels(gauge.getName(), sortedAttrs, null, null);
        return List.of(buildJsonDoc(labels, timestamp, value));
    }

    private static List<String> buildSum(final Sum sum, final String[][] sortedAttrs, final long timestamp) {
        final double value = sum.getValue() != null ? sum.getValue() : 0.0;
        final String name = sum.isMonotonic() && !sum.getName().endsWith("_total")
                ? sum.getName() + "_total"
                : sum.getName();
        final String labels = buildLabels(name, sortedAttrs, null, null);
        return List.of(buildJsonDoc(labels, timestamp, value));
    }

    private static List<String> buildHistogram(final Histogram histogram, final String[][] sortedAttrs, final long timestamp) {
        final String baseName = histogram.getName();
        final List<Long> bucketCounts = histogram.getBucketCountsList();
        final List<Double> explicitBounds = histogram.getExplicitBoundsList();
        final int bucketSize = (bucketCounts != null && explicitBounds != null) ? bucketCounts.size() : 0;
        final List<String> documents = new ArrayList<>(bucketSize + 2);

        if (bucketSize > 0) {
            final String bucketName = baseName + "_bucket";
            long cumulativeCount = 0;
            for (int i = 0; i < bucketCounts.size(); i++) {
                cumulativeCount += bucketCounts.get(i);
                final String le = i < explicitBounds.size()
                        ? formatDouble(explicitBounds.get(i))
                        : "+Inf";
                final String labels = buildLabels(bucketName, sortedAttrs, "le", le);
                documents.add(buildJsonDoc(labels, timestamp, (double) cumulativeCount));
            }
        }

        appendCountAndSumDocuments(documents, baseName, histogram.getCount(), histogram.getSum(), sortedAttrs, timestamp);
        return documents;
    }

    private static List<String> buildSummary(final Summary summary, final String[][] sortedAttrs, final long timestamp) {
        final String baseName = summary.getName();
        final List<? extends Quantile> quantiles = summary.getQuantiles();
        final int quantileSize = quantiles != null ? quantiles.size() : 0;
        final List<String> documents = new ArrayList<>(quantileSize + 2);

        if (quantiles != null) {
            for (final Quantile q : quantiles) {
                final String labels = buildLabels(baseName, sortedAttrs, "quantile", formatDouble(q.getQuantile()));
                documents.add(buildJsonDoc(labels, timestamp, q.getValue() != null ? q.getValue() : 0.0));
            }
        }

        appendCountAndSumDocuments(documents, baseName, summary.getCount(), summary.getSum(), sortedAttrs, timestamp);
        return documents;
    }

    private static void appendCountAndSumDocuments(final List<String> documents, final String baseName,
                                                   final Long count, final Double sum,
                                                   final String[][] sortedAttrs, final long timestamp) {
        documents.add(buildJsonDoc(
                buildLabels(baseName + "_count", sortedAttrs, null, null),
                timestamp, count != null ? count.doubleValue() : 0.0));
        documents.add(buildJsonDoc(
                buildLabels(baseName + "_sum", sortedAttrs, null, null),
                timestamp, sum != null ? sum : 0.0));
    }

    private static String[][] sortAndSanitizeAttributes(final Map<String, Object> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return new String[0][];
        }
        final String[][] entries = new String[attributes.size()][];
        int idx = 0;
        for (final Map.Entry<String, Object> e : attributes.entrySet()) {
            if (e.getValue() != null) {
                entries[idx++] = new String[]{sanitizeLabelKey(e.getKey()), sanitizeLabelValue(e.getValue().toString())};
            }
        }
        final String[][] trimmed = idx == entries.length ? entries : Arrays.copyOf(entries, idx);
        Arrays.sort(trimmed, Comparator.comparing(a -> a[0]));
        return trimmed;
    }

    private static String buildLabels(final String metricName, final String[][] sortedAttrs,
                                      final String extraKey, final String extraValue) {
        final int estimatedSize = 10 + metricName.length()
                + sortedAttrs.length * 20
                + (extraKey != null ? extraKey.length() + extraValue.length() + 2 : 0);
        final StringBuilder sb = new StringBuilder(estimatedSize);

        sb.append(NAME_LABEL).append(' ').append(sanitizeLabelValue(metricName));

        boolean extraInserted = (extraKey == null);
        for (final String[] attr : sortedAttrs) {
            if (!extraInserted && attr[0].equals(extraKey)) {
                continue;
            }
            if (!extraInserted && extraKey.compareTo(attr[0]) < 0) {
                sb.append(' ').append(extraKey).append(' ').append(extraValue);
                extraInserted = true;
            }
            sb.append(' ').append(attr[0]).append(' ').append(attr[1]);
        }
        if (!extraInserted) {
            sb.append(' ').append(extraKey).append(' ').append(extraValue);
        }

        return sb.toString();
    }

    private static String buildJsonDoc(final String labels, final long timestamp, final double value) {
        final StringBuilder sb = new StringBuilder(labels.length() + 64);
        sb.append("{\"labels\":\"");
        appendJsonEscaped(sb, labels);
        sb.append("\",\"timestamp\":");
        sb.append(timestamp);
        sb.append(",\"value\":");
        sb.append(value);
        sb.append('}');
        return sb.toString();
    }

    private static void appendJsonEscaped(final StringBuilder sb, final String s) {
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append("\\u00");
                        sb.append(HEX_CHARS[(c >> 4) & 0xF]);
                        sb.append(HEX_CHARS[c & 0xF]);
                    } else {
                        sb.append(c);
                    }
            }
        }
    }

    private static long parseTimestamp(final String isoTime) {
        if (isoTime == null || isoTime.isEmpty()) {
            LOG.warn("Metric has no timestamp, using current time");
            return System.currentTimeMillis();
        }
        try {
            return Instant.parse(isoTime).toEpochMilli();
        } catch (final Exception e) {
            LOG.error("Failed to parse timestamp '{}', using current time", isoTime, e);
            return System.currentTimeMillis();
        }
    }

    private static String sanitizeLabelKey(final String key) {
        if (key == null || key.isEmpty()) {
            return "_";
        }
        final StringBuilder sb = new StringBuilder(key.length() + 1);
        final char first = key.charAt(0);
        if (!(Character.isLetter(first) || first == '_')) {
            sb.append('_');
        }
        for (int i = 0; i < key.length(); i++) {
            final char c = key.charAt(i);
            sb.append(Character.isLetterOrDigit(c) || c == '_' ? c : '_');
        }
        return sb.toString();
    }

    private static String sanitizeLabelValue(final String value) {
        if (value == null) {
            return "";
        }
        return value.replace(' ', '_');
    }

    private static String formatDouble(final Double value) {
        if (value == null) {
            return "0";
        }
        if (value == Double.POSITIVE_INFINITY) {
            return "+Inf";
        }
        if (value == Double.NEGATIVE_INFINITY) {
            return "-Inf";
        }
        if (value == (long) value.doubleValue()) {
            return String.valueOf((long) value.doubleValue());
        }
        return String.valueOf(value);
    }
}
