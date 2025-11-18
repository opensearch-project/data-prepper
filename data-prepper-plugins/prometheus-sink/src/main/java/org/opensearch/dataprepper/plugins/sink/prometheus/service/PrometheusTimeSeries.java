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

import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.metric.ExponentialHistogram;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.stripEnd;
import static org.apache.commons.lang3.StringUtils.stripStart;

 public class PrometheusTimeSeries {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusTimeSeries.class);
    private static final Character UNDERSCORE = '_';

    /**
     * @see https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/translator/prometheus
     */ for the following map and metric/label sanitization rules
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
    private List<Label> labels;
    private int size;

    public PrometheusTimeSeries(Metric metric, final boolean sanitizeNames) throws Exception {
        this.sanitizeNames = sanitizeNames;
        metricName = sanitizeNames ? sanitizeMetricName(metric) : metric.getName();
        String time = metric.getTime();
        String startTime = metric.getStartTime();
        timestamp = (time != null) ? getTimeStampVal(time) : getTimeStampVal(startTime);
        timeSeriesList = new ArrayList<>();
        labels = new ArrayList<>();
        Map<String, Object> attributesMap = metric.getAttributes();
        Map<String, Object> flattenedAttributeMap = flattenMap(attributesMap);
        for (Map.Entry<String, Object> entry : flattenedAttributeMap.entrySet()) {
            addLabel(entry.getKey(), entry.getValue());
        }
        Map<String, Object> resourceAttributesMap = null;
        Map<String, Object> scopeAttributesMap = null;
        try {
            if (metric.getResource() != null) {
                resourceAttributesMap = (Map<String, Object>) metric.getResource().get("attributes");
            }
            if (metric.getScope() != null) {
                scopeAttributesMap = (Map<String, Object>) metric.getScope().get("attributes");
            }
        } catch (Exception e) {
            LOG.warn("Failed to get resource/scope attributes", e);
        }
        if (resourceAttributesMap != null) {
            flattenedAttributeMap = flattenMap(resourceAttributesMap);
            for (Map.Entry<String, Object> entry : flattenedAttributeMap.entrySet()) {
                addLabel("resource_"+entry.getKey(), entry.getValue());
            }
        }
        if (scopeAttributesMap != null) {
            flattenedAttributeMap = flattenMap(scopeAttributesMap);
            for (Map.Entry<String, Object> entry : flattenedAttributeMap.entrySet()) {
                addLabel("scope_"+entry.getKey(), entry.getValue());
            }
        }
    }

    private void addLabel(String name, final Object value) {
        if (sanitizeNames) {
            name = sanitizeLabelName(name);
        }
        addLabelSanitized(name, value);
    }

    private void addLabelSanitized(final String name, final Object value) {
        String valueStr;
        if (value instanceof String) {
            valueStr = (String)value;
        } else {
            valueStr = String.valueOf(value);
        }
        Label label = Label.newBuilder().setName(name).setValue(valueStr).build();
        labels.add(label);
        size += label.toByteArray().length;
    }

    public List<TimeSeries> getTimeSeriesList() {
        return timeSeriesList;
    }

    private void addTimeSeries(final String labelName, final String labelValue, final Double sampleValue) {
        // labelName here is a constant string without any invalid characters
        Label label = Label.newBuilder().setName(labelName).setValue(labelValue).build();
        size += label.toByteArray().length;
        Sample sample = Sample.newBuilder().setValue(sampleValue).setTimestamp(timestamp).build();
        size += sample.toByteArray().length;
        timeSeriesList.add(TimeSeries.newBuilder()
                .addAllLabels(labels)
                .addLabels(label)
                .addSamples(sample)
                .build());
    }

    private void addTimeSeries(final String metricName, final String labelName,
                                final String labelValue, final Double sampleValue) {
        Label label1 = Label.newBuilder().setName("__name__").setValue(metricName).build();
        // labelName here is a constant string without any invalid characters
        Label label2 = Label.newBuilder().setName(labelName).setValue(labelValue).build();
        size += label1.toByteArray().length + label2.toByteArray().length;
        Sample sample = Sample.newBuilder().setValue(sampleValue).setTimestamp(timestamp).build();
        size += sample.toByteArray().length;
        timeSeriesList.add(TimeSeries.newBuilder()
                .addAllLabels(labels)
                .addLabels(label1)
                .addLabels(label2)
                .addSamples(sample)
                .build());
    }


    public long getTimeStamp() {
        return timestamp;
    }

    public void addSumMetric(Sum sum) {
        addTimeSeries("__name__", metricName, sum.getValue());
    }

    public void addGaugeMetric(Gauge gauge) {
        addTimeSeries("__name__", metricName, gauge.getValue());
    }

    public void addSummaryMetric(Summary summary) {
        List<? extends Quantile> quantiles = summary.getQuantiles();
        for (int i = 0; i < quantiles.size(); i++) {
            Quantile quantile = quantiles.get(i);
            addTimeSeries(metricName, "quantile", quantile.getQuantile().toString(), (double)quantile.getValue());

        }
    }

    public void addHistogramMetric(Histogram histogram) {
        addTimeSeries("__name__", metricName + "_count", (double)histogram.getCount());
        addTimeSeries("__name__", metricName + "_sum", (double)histogram.getSum());

        Double min = histogram.getMin();
        if (min != null) {
            addTimeSeries("__name__", metricName + "_min", (double)min);
        }
        Double max = histogram.getMax();
        if (max != null) {
            addTimeSeries("__name__", metricName + "_max", (double)max);
        }
        List<Double> explicitBounds = histogram.getExplicitBoundsList();
        List<Long> bucketCounts = histogram.getBucketCountsList();
        if (explicitBounds != null && bucketCounts != null) {
            addLabelSanitized("__name__", metricName+"_bucket");
            for (int i = 0; i < bucketCounts.size(); i++) {
                final String labelValue = (i == bucketCounts.size()-1) ? "+Inf" : explicitBounds.get(i).toString();
                addTimeSeries("le", labelValue, (double)bucketCounts.get(i));
            }
        }
    }

    public void addExponentialHistogramMetric(ExponentialHistogram histogram) {
        addTimeSeries("__name__", metricName + "_count", (double)histogram.getCount());
        addTimeSeries("__name__", metricName + "_sum", (double)histogram.getSum());
        Long zeroCount = histogram.getZeroCount();
        if (zeroCount != null) {
            addTimeSeries("__name__", metricName + "_zero_count", (double)zeroCount);
        }
        Double zeroThreshold = histogram.getZeroThreshold();
        if (zeroThreshold != null) {
            addTimeSeries("__name__", metricName + "_zero_threshold", (double)zeroThreshold);
        }
        Integer scale = histogram.getScale();
        if (scale != null) {
            addTimeSeries("__name__", metricName + "_zero_threshold", (double)scale);
        }
        List<Long> positiveBucketCounts = histogram.getPositive();
        Integer positiveOffset = histogram.getPositiveOffset();
        List<Long> negativeBucketCounts = histogram.getNegative();
        Integer negativeOffset = histogram.getPositiveOffset();
        boolean positiveBucketsPresent = (positiveBucketCounts != null) && (positiveOffset != null);
        boolean negativeBucketsPresent = (negativeBucketCounts != null) && (negativeOffset != null);

        if (positiveBucketsPresent || negativeBucketsPresent) {
            addLabelSanitized("__name__", metricName+"_bucket");
            if (positiveBucketsPresent) {
                for (int i = 0; i < positiveBucketCounts.size(); i++) {
                    double bound = calculateBucketBound(i + positiveOffset + 1, scale);
                    addTimeSeries("le", Double.toString(bound), (double)positiveBucketCounts.get(i));
                }
            }
            if (negativeBucketsPresent) {
                for (int i = 0; i < negativeBucketCounts.size(); i++) {
                    double bound = -1 * calculateBucketBound(i + negativeOffset + 1, scale);
                    addTimeSeries("ge", Double.toString(bound), (double)negativeBucketCounts.get(i));
                }
            }
        }
    }

    private double calculateBucketBound(int index, int scale) {
        return Math.pow(2, index * Math.pow(2, -scale));
    }

    private static void flattenHelper(Map<String, Object> map, String prefix, Map<String, Object> flatMap) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "_" + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                flattenHelper((Map<String, Object>) value, key, flatMap);
            } else {
                flatMap.put(key, value.toString());
            }
        }
    }

    // See https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/translator/prometheus
    static String sanitizeMetricName(final Metric metric) {

        final String name = metric.getName();
        final String unit = metric.getUnit();
        final boolean isGauge = metric.getKind().equals(Metric.KIND.GAUGE.toString());
        final boolean isCounter = metric.getKind().equals(Metric.KIND.SUM.toString()) &&
                                 ((Sum)metric).isMonotonic() &&
                                 ((Sum)metric).getAggregationTemporality().equals("AGGREGATION_TEMPORALITY_CUMULATIVE");

        String metricName = sanitizeName(name, true, false);  // metric names allow colon
        String suffix = isCounter ? (UNDERSCORE+"total") : "";
        if (unit.startsWith("{")) {
            return metricName+suffix;
        }

        if (unit.equals("1") && isGauge) {
            return metricName+UNDERSCORE+"ratio";
        }
        String val = otelToPrometheusUnitsMap.get(unit);
        if (val != null) {
            return metricName+UNDERSCORE+val+suffix;
        }
        if (unit.contains("/")) {
            String[] unitSplit = unit.split("/");
            if (unitSplit.length == 2) {
                String unit1 = otelToPrometheusUnitsMap.get(unitSplit[0]);
                String unit2 = otelToPrometheusUnitsMap.get(unitSplit[1]);
                if (unit1 != null && unit2 != null) {
                    return metricName + UNDERSCORE + unit1 + UNDERSCORE + unit2+suffix;
                }
            }
        }
        return unit.equals("1") ? metricName+suffix : metricName+UNDERSCORE+unit+suffix;
    }

    static String sanitizeLabelName(final String name) {
        return sanitizeName(name, false, true); // label names do NOT allow colon
    }

    static String sanitizeName(final String name, final boolean allowColon, final boolean isLabel) {
        StringBuilder sb = new StringBuilder(name.length());
        Character prevChar = null;
        for (int i = 0; i < name.length(); i++) {
            Character curChar = sanitizeChar(name.charAt(i), i == 0, allowColon);
            if (isLabel || (curChar != UNDERSCORE || prevChar != UNDERSCORE)) {
                sb.append(curChar);
            }
            prevChar = curChar;
        }
        return isLabel ? sb.toString() : stripEnd(stripStart(sb.toString(), "_"), "_");
    }

    private static char sanitizeChar(char c, boolean isFirst, boolean allowColon) {
        if (allowColon && c == ':') {
            return c;
        }
        if (isFirst) {
            return (Character.isLetter(c)) ? c : '_';
        } else {
            return (Character.isLetterOrDigit(c)) ? c : '_';
        }
    }


    private static Map<String, Object> flattenMap(Map<String, Object> map) {
        Map<String, Object> flatMap = new HashMap<>();
        flattenHelper(map, "", flatMap);
        return flatMap;
    }

    private static long getTimeStampVal(final String time) throws Exception {
        long timeStampVal = 0;
        timeStampVal = Instant.parse(time).toEpochMilli();
        return timeStampVal;
    }

    public int getSize() {
        return size;
    }

}

