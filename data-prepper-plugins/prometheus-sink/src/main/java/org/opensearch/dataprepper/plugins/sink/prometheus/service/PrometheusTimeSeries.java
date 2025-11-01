/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;


import com.arpnetworking.metrics.prometheus.Types.Label;
import com.arpnetworking.metrics.prometheus.Types.Sample;
import com.arpnetworking.metrics.prometheus.Types.TimeSeries;

import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrometheusTimeSeries {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusTimeSeries.class);
    private final String metricName;
    private long timestamp;
    List<TimeSeries> timeSeriesList;
    List<Label> labels;
    private int size;

    public String toString() {
        String result = "";
        for (final TimeSeries ts : timeSeriesList) {
            result += "{\n\tLabels:[";
            for (final Label l : ts.getLabelsList()) {
                result += "\t\t{"+l.getName()+", "+l.getValue()+"}";
            }
            result += "\t]";
            result += "\tSamples:[";
            for (final Sample s : ts.getSamplesList()) {
                result += "\t\t{"+s.getValue()+", "+s.getTimestamp()+"}";
            }
            result += "\t]\n}";
        }
        return result;
    }

    public PrometheusTimeSeries(JacksonMetric metric) throws Exception {
        metricName = metric.getName();
        String time = metric.getTime();
        String startTime = metric.getStartTime();
        timestamp = (time != null) ? getTimeStampVal(time) : getTimeStampVal(startTime);
        timeSeriesList = new ArrayList<>();
        labels = new ArrayList<>();
        Map<String, Object> attributesMap = metric.getAttributes();
        Map<String, Object> flattenedAttributeMap = flattenMap(attributesMap);
        for (Map.Entry<String, Object> entry : flattenedAttributeMap.entrySet()) {
            final String key = entry.getKey().replace(".", "_");
            addLabel(key, entry.getValue());
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
                final String key = entry.getKey().replace(".", "_");
                addLabel("resource_"+key, entry.getValue());
            }
        }
        if (scopeAttributesMap != null) {
            flattenedAttributeMap = flattenMap(scopeAttributesMap);
            for (Map.Entry<String, Object> entry : flattenedAttributeMap.entrySet()) {
                final String key = entry.getKey().replace(".", "_");
                addLabel("scope_"+key, entry.getValue());
            }
        }
    }

    private void addLabel(final String name, final Object value) {
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

    public void addSumMetric(JacksonSum sum) {
        addTimeSeries("__name__", metricName + "_sum", sum.getValue());
    }

    public void addGaugeMetric(JacksonGauge gauge) {
        addTimeSeries("__name__", metricName, gauge.getValue());
    }

    public void addSummaryMetric(JacksonSummary summary) {
        List<? extends Quantile> quantiles = summary.getQuantiles();
        for (int i = 0; i < quantiles.size(); i++) {
            Quantile quantile = quantiles.get(i);
            addTimeSeries(metricName, "quantile", quantile.getQuantile().toString(), (double)quantile.getValue());

        }
    }

    public void addHistogramMetric(JacksonHistogram histogram) {
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
            addLabel("__name__", metricName+"_bucket");
            for (int i = 0; i < bucketCounts.size(); i++) {
                final String labelValue = (i == bucketCounts.size()-1) ? "+Inf" : explicitBounds.get(i).toString();
                addTimeSeries("le", labelValue, (double)bucketCounts.get(i));
            }
        }
    }

    public void addExponentialHistogramMetric(JacksonExponentialHistogram histogram) {
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
            addLabel("__name__", metricName+"_bucket");
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

