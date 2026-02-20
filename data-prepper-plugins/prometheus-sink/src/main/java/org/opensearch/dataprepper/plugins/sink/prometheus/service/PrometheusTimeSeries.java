/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import com.arpnetworking.metrics.prometheus.Types.Label;
import com.arpnetworking.metrics.prometheus.Types.Sample;
import com.arpnetworking.metrics.prometheus.Types.TimeSeries;
import org.opensearch.dataprepper.model.metric.ExponentialHistogram;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

public class PrometheusTimeSeries {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusTimeSeries.class);
    private static final int APPROXIMATE_PROTOBUF_LABEL_OVERHEAD = 8;
    private static final int APPROXIMATE_PROTOBUF_SAMPLE_OVERHEAD = 2;
    private static final String UNDERSCORE = "_";
    private static final String TOTAL_SUFFIX = "_total";
    private static final String RATIO_SUFFIX = "_ratio";
    private static final String COUNT_SUFFIX = "_count";
    private static final String SUM_SUFFIX = "_sum";
    private static final String MIN_SUFFIX = "_min";
    private static final String MAX_SUFFIX = "_max";
    private static final String BUCKET_SUFFIX = "_bucket";
    private static final String ZERO_COUNT_SUFFIX = "_zero_count";
    private static final String ZERO_THRESHOLD_SUFFIX = "_zero_threshold";
    private static final String NAME_LABEL = "__name__";
    private static final String QUANTILE_LABEL = "quantile";
    private static final String LE_LABEL = "le";
    private static final String GE_LABEL = "ge";
    private static final String PLUS_INF = "+Inf";
    private static final String RESOURCE_PREFIX = "resource_";
    private static final String SCOPE_PREFIX = "scope_";

    private static final Map<String, String> otelToPrometheusUnitsMap = Map.ofEntries(
            Map.entry("d",    "days"),
            Map.entry("h",    "hours"),
            Map.entry("min",  "minutes"),
            Map.entry("s",    "seconds"),
            Map.entry("ms",   "milliseconds"),
            Map.entry("us",   "microseconds"),
            Map.entry("ns",   "nanoseconds"),
            Map.entry("By",   "bytes"),
            Map.entry("KiBy", "kibibytes"),
            Map.entry("MiBy", "mebibytes"),
            Map.entry("GiBy", "gibibytes"),
            Map.entry("TiBy", "tibibytes"),
            Map.entry("KBy",  "kilobytes"),
            Map.entry("MBy",  "megabytes"),
            Map.entry("GBy",  "gigabytes"),
            Map.entry("TBy",  "terabytes"),
            Map.entry("V",    "volts"),
            Map.entry("A",    "amperes"),
            Map.entry("J",    "joules"),
            Map.entry("W",    "watts"),
            Map.entry("g",    "grams"),
            Map.entry("Cel",  "celsius"),
            Map.entry("Hz",   "hertz"),
            Map.entry("%",    "percent"),
            Map.entry("m",    "meters")
    );
    private final String metricName;
    private long timestamp;
    private boolean sanitizeNames;
    private List<TimeSeries> timeSeriesList;
    private List<Label> baseLabels;
    private long baseLabelsSize;
    private int seriesSize;
    private final PrometheusMetricMetadata metadata;

    public PrometheusTimeSeries(Metric metric, final boolean sanitizeNames) throws Exception {
        this.sanitizeNames = sanitizeNames;
        this.metricName = sanitizeNames ? sanitizeMetricName(metric) : metric.getName();

        String time = metric.getTime();
        String startTime = metric.getStartTime();
        this.timestamp = (time != null) ? getTimestampVal(time) : getTimestampVal(startTime);

        this.timeSeriesList = new ArrayList<>();
        this.baseLabels = new ArrayList<>();
        this.seriesSize = 0;

        // Process all attributes in one pass
        baseLabelsSize = processAttributes(metric.getAttributes(), "");
        baseLabelsSize += processResourceAndScopeAttributes(metric);

        // Generate metadata for this metric
        this.metadata = PrometheusMetricMetadata.fromMetric(metric, this.metricName);

        if (metric instanceof Gauge) {
            addGaugeMetric((Gauge)metric);
        } else if (metric instanceof Sum) {
            addSumMetric((Sum)metric);
        } else if (metric instanceof Summary) {
            addSummaryMetric((Summary)metric);
        } else if (metric instanceof Histogram) {
            addHistogramMetric((Histogram)metric);
        } else if (metric instanceof ExponentialHistogram) {
            addExponentialHistogramMetric((ExponentialHistogram)metric);
        } else {
            throw new RuntimeException("Unknown Metric");
        }
    }

    public String getMetricKey() {
        StringBuilder result = new StringBuilder();
        for (final Label l : timeSeriesList.get(0).getLabelsList()) {
            result.append(l.getName()).append(" ").append(l.getValue()).append(" ");
        }
        return result.toString();
    }

    public String getMetricName() {
        return metricName;
    }

    private long processAttributes(Map<String, Object> attributesMap, String prefix) {
        long size = 0L;
        if (attributesMap != null) {
            for (Map.Entry<String, Object> entry : attributesMap.entrySet()) {
                String key = prefix.isEmpty() ? entry.getKey() : prefix + entry.getKey();
                Object value = entry.getValue();

                if (value instanceof Map) {
                    processAttributes((Map<String, Object>) value, key + UNDERSCORE);
                } else {
                    size += addLabel(key, value);
                }
            }
        }
        return size;
    }

    private long processResourceAndScopeAttributes(Metric metric) {
        long size = 0L;
        try {
            if (metric.getResource() != null) {
                Map<String, Object> resourceAttributes = (Map<String, Object>) metric.getResource().get("attributes");
                size += processAttributes(resourceAttributes, RESOURCE_PREFIX);
            }
            if (metric.getScope() != null) {
                Map<String, Object> scopeAttributes = (Map<String, Object>) metric.getScope().get("attributes");
                size += processAttributes(scopeAttributes, SCOPE_PREFIX);
            }
        } catch (Exception e) {
            LOG.warn(NOISY, "Failed to get resource/scope attributes", e);
        }
        return size;
    }

    private long addLabel(String name, final Object value) {
        if (sanitizeNames) {
            name = sanitizeLabelName(name);
        }
        return addLabelSanitized(name, value);
    }

    private long addLabelSanitized(final String name, final Object value) {
        String valueStr = (value instanceof String) ? (String) value : String.valueOf(value);
        Label label = Label.newBuilder().setName(name).setValue(valueStr).build();
        baseLabels.add(label);
        return estimateLabelSize(name, valueStr);
    }

    private long estimateLabelSize(String name, String value) {
        return (long)name.length() + value.length() + APPROXIMATE_PROTOBUF_LABEL_OVERHEAD;
    }

    private void addTimeSeries(final String labelName, final String labelValue, final Double sampleValue) {
        addTimeSeries(labelName, labelValue, null, null, sampleValue);
    }

    private void addTimeSeries(final String labelName, final String labelValue, final String labelName2, final String labelValue2, final Double sampleValue) {
        List<Label> labels = new ArrayList<>(baseLabels.size() + 2);
        labels.addAll(baseLabels);
        labels.add(Label.newBuilder().setName(labelName).setValue(labelValue).build());
        if (labelName2 != null) {
            labels.add(Label.newBuilder().setName(labelName2).setValue(labelValue2).build());
        }
        Collections.sort(labels, Comparator.comparing(Label::getName));
        TimeSeries ts = TimeSeries.newBuilder()
                .addAllLabels(labels)
                .addSamples(Sample.newBuilder().setValue(sampleValue).setTimestamp(timestamp).build())
                .build();
        seriesSize += ts.getSerializedSize();
        timeSeriesList.add(ts);
    }

    public List<TimeSeries> getTimeSeriesList() { return timeSeriesList; }
    public long getTimestamp() { return timestamp; }
    public int getSize() { return seriesSize; }

    public PrometheusMetricMetadata getMetadata() {
        return metadata;
    }

    public void addSumMetric(Sum sum) {
        addTimeSeries(NAME_LABEL, metricName, sum.getValue());
    }

    public void addGaugeMetric(Gauge gauge) {
        addTimeSeries(NAME_LABEL, metricName, gauge.getValue());
    }

    public void addSummaryMetric(Summary summary) {
        addTimeSeries(NAME_LABEL, metricName + COUNT_SUFFIX, (double) summary.getCount());
        addTimeSeries(NAME_LABEL, metricName + SUM_SUFFIX, (double) summary.getSum());
        List<? extends Quantile> quantiles = summary.getQuantiles();
        for (Quantile quantile : quantiles) {
            addTimeSeries(NAME_LABEL, metricName, QUANTILE_LABEL, quantile.getQuantile().toString(), (double) quantile.getValue());
        }
    }

    public void addHistogramMetric(Histogram histogram) {
        addTimeSeries(NAME_LABEL, metricName + COUNT_SUFFIX, (double) histogram.getCount());
        addTimeSeries(NAME_LABEL, metricName + SUM_SUFFIX, (double) histogram.getSum());

        Double min = histogram.getMin();
        if (min != null) {
            addTimeSeries(NAME_LABEL, metricName + MIN_SUFFIX, min);
        }
        Double max = histogram.getMax();
        if (max != null) {
            addTimeSeries(NAME_LABEL, metricName + MAX_SUFFIX, max);
        }

        List<Double> explicitBounds = histogram.getExplicitBoundsList();
        List<Long> bucketCounts = histogram.getBucketCountsList();
        if (explicitBounds != null && bucketCounts != null) {
            int lastIndex = bucketCounts.size() - 1;
            for (int i = 0; i < bucketCounts.size(); i++) {
                String labelValue = (i == lastIndex) ? PLUS_INF : explicitBounds.get(i).toString();
                addTimeSeries(NAME_LABEL, metricName + BUCKET_SUFFIX, LE_LABEL, labelValue, (double) bucketCounts.get(i));
            }
        }
    }

    public void addExponentialHistogramMetric(ExponentialHistogram histogram) {
        addTimeSeries(NAME_LABEL, metricName + COUNT_SUFFIX, (double) histogram.getCount());
        addTimeSeries(NAME_LABEL, metricName + SUM_SUFFIX, (double) histogram.getSum());

        Long zeroCount = histogram.getZeroCount();
        if (zeroCount != null) {
            addTimeSeries(NAME_LABEL, metricName + ZERO_COUNT_SUFFIX, (double) zeroCount);
        }
        Double zeroThreshold = histogram.getZeroThreshold();
        if (zeroThreshold != null) {
            addTimeSeries(NAME_LABEL, metricName + ZERO_THRESHOLD_SUFFIX, zeroThreshold);
        }
        Integer scale = histogram.getScale();
        if (scale != null) {
            addTimeSeries(NAME_LABEL, metricName + ZERO_THRESHOLD_SUFFIX, (double) scale);
        }

        List<Long> positiveBucketCounts = histogram.getPositive();
        Integer positiveOffset = histogram.getPositiveOffset();
        List<Long> negativeBucketCounts = histogram.getNegative();
        Integer negativeOffset = histogram.getPositiveOffset();

        boolean positiveBucketsPresent = (positiveBucketCounts != null) && (positiveOffset != null);
        boolean negativeBucketsPresent = (negativeBucketCounts != null) && (negativeOffset != null);

        if (positiveBucketsPresent || negativeBucketsPresent) {
            if (positiveBucketsPresent) {
                for (int i = 0; i < positiveBucketCounts.size(); i++) {
                    double bound = calculateBucketBound(i + positiveOffset + 1, scale);
                    addTimeSeries(NAME_LABEL, metricName + BUCKET_SUFFIX, LE_LABEL, Double.toString(bound), (double) positiveBucketCounts.get(i));
                }
            }
            if (negativeBucketsPresent) {
                for (int i = 0; i < negativeBucketCounts.size(); i++) {
                    double bound = -calculateBucketBound(i + negativeOffset + 1, scale);
                    addTimeSeries(NAME_LABEL, metricName + BUCKET_SUFFIX, GE_LABEL, Double.toString(bound), (double) negativeBucketCounts.get(i));
                }
            }
        }
    }

    private static double calculateBucketBound(int index, int scale) {
        return Math.pow(2, index * Math.pow(2, -scale));
    }

    static String sanitizeMetricName(final Metric metric) {
        final String name = metric.getName();
        final String unit = metric.getUnit();
        final boolean isGauge = metric.getKind().equals(Metric.KIND.GAUGE.toString());
        final boolean isCounter = metric.getKind().equals(Metric.KIND.SUM.toString()) &&
                ((Sum) metric).isMonotonic() &&
                ((Sum) metric).getAggregationTemporality().equals("AGGREGATION_TEMPORALITY_CUMULATIVE");

        StringBuilder metricNameBuilder = new StringBuilder(sanitizeName(name, true, false));
        String suffix = isCounter ? TOTAL_SUFFIX : "";

        if (unit.startsWith("{")) {
            return metricNameBuilder.append(suffix).toString();
        }

        if ("1".equals(unit) && isGauge) {
            return metricNameBuilder.append(RATIO_SUFFIX).toString();
        }

        String mappedUnit = otelToPrometheusUnitsMap.get(unit);
        if (mappedUnit != null) {
            return metricNameBuilder.append(UNDERSCORE).append(mappedUnit).append(suffix).toString();
        }

        if (unit.contains("/")) {
            String[] unitSplit = unit.split("/", 2);
            if (unitSplit.length == 2) {
                String unit1 = otelToPrometheusUnitsMap.get(unitSplit[0]);
                String unit2 = otelToPrometheusUnitsMap.get(unitSplit[1]);
                if (unit1 != null && unit2 != null) {
                    return metricNameBuilder.append(UNDERSCORE).append(unit1)
                            .append(UNDERSCORE).append(unit2).append(suffix).toString();
                }
            }
        }

        if (!"1".equals(unit)) {
            metricNameBuilder.append(UNDERSCORE).append(unit);
        }
        return metricNameBuilder.append(suffix).toString();
    }

    static String sanitizeLabelName(final String name) {
        return sanitizeName(name, false, true);
    }

    static String sanitizeName(final String name, final boolean allowColon, final boolean isLabel) {
        StringBuilder sb = new StringBuilder(name.length());
        char prevChar = 0;

        for (int i = 0; i < name.length(); i++) {
            char curChar = sanitizeChar(name.charAt(i), i == 0, allowColon);
            if (isLabel || (curChar != '_' || prevChar != '_')) {
                sb.append(curChar);
            }
            prevChar = curChar;
        }

        String result = sb.toString();
        if (!isLabel) {
            // Strip leading and trailing underscores
            int start = 0, end = result.length();
            while (start < end && result.charAt(start) == '_') start++;
            while (end > start && result.charAt(end - 1) == '_') end--;
            result = result.substring(start, end);
        }
        return result;
    }

    private static char sanitizeChar(char c, boolean isFirst, boolean allowColon) {
        if (allowColon && c == ':') return c;
        if (isFirst) return Character.isLetter(c) ? c : '_';
        return Character.isLetterOrDigit(c) ? c : '_';
    }

    private static long getTimestampVal(final String time) throws Exception {
        return Instant.parse(time).toEpochMilli();
    }
}
