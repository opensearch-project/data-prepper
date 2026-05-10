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

import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.ExponentialHistogram;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;
import org.opensearch.dataprepper.model.metric.Gauge;

/**
 * Represents Prometheus metric metadata that can be sent via Remote Write protocol.
 * This class holds the TYPE and HELP information for metrics.
 */
public class PrometheusMetricMetadata {

    public enum MetricType {
        UNKNOWN(0),
        COUNTER(1),
        GAUGE(2),
        HISTOGRAM(3),
        GAUGEHISTOGRAM(4),
        SUMMARY(5),
        INFO(6),
        STATESET(7);

        private final int value;

        MetricType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    private final MetricType type;
    private final String metricFamilyName;
    private final String help;
    private final String unit;

    private PrometheusMetricMetadata(Builder builder) {
        this.type = builder.type;
        this.metricFamilyName = builder.metricFamilyName;
        this.help = builder.help;
        this.unit = builder.unit;
    }

    public MetricType getType() {
        return type;
    }

    public String getMetricFamilyName() {
        return metricFamilyName;
    }

    public String getHelp() {
        return help;
    }

    public String getUnit() {
        return unit;
    }

    /**
     * Creates metadata from a Data Prepper metric.
     *
     * @param metric The metric to extract metadata from
     * @param sanitizedName The sanitized metric name (family name)
     * @return PrometheusMetricMetadata object
     */
    public static PrometheusMetricMetadata fromMetric(Metric metric, String sanitizedName) {
        Builder builder = builder()
                .metricFamilyName(sanitizedName)
                .help(metric.getDescription() != null ? metric.getDescription() : "")
                .unit(metric.getUnit() != null ? metric.getUnit() : "");

        // Determine metric type based on Data Prepper metric kind
        if (metric instanceof Gauge) {
            builder.type(MetricType.GAUGE);
        } else if (metric instanceof Sum) {
            Sum sum = (Sum) metric;
            // Counter: monotonic cumulative sum
            if (sum.isMonotonic() &&
                "AGGREGATION_TEMPORALITY_CUMULATIVE".equals(sum.getAggregationTemporality())) {
                builder.type(MetricType.COUNTER);
            } else {
                builder.type(MetricType.GAUGE);
            }
        } else if (metric instanceof Histogram) {
            builder.type(MetricType.HISTOGRAM);
        } else if (metric instanceof ExponentialHistogram) {
            // Exponential histograms are represented as regular histograms in Prometheus
            builder.type(MetricType.HISTOGRAM);
        } else if (metric instanceof Summary) {
            builder.type(MetricType.SUMMARY);
        } else {
            builder.type(MetricType.UNKNOWN);
        }

        return builder.build();
    }

    /**
     * Estimates the size in bytes when serialized to protobuf.
     * This is used for buffer size calculations.
     *
     * @return Estimated size in bytes
     */
    public int estimateSize() {
        // Protobuf overhead: field tags + length prefixes
        int size = 0;

        // type field (1 byte for tag + 1 byte for enum value)
        size += 2;

        // metric_family_name field (1 byte for tag + length prefix + string bytes)
        if (metricFamilyName != null && !metricFamilyName.isEmpty()) {
            size += 1 + computeVarIntSize(metricFamilyName.length()) + metricFamilyName.length();
        }

        // help field (1 byte for tag + length prefix + string bytes)
        if (help != null && !help.isEmpty()) {
            size += 1 + computeVarIntSize(help.length()) + help.length();
        }

        // unit field (1 byte for tag + length prefix + string bytes)
        if (unit != null && !unit.isEmpty()) {
            size += 1 + computeVarIntSize(unit.length()) + unit.length();
        }

        return size;
    }

    private static int computeVarIntSize(int value) {
        if (value < 0) return 10;
        if (value < (1 << 7)) return 1;
        if (value < (1 << 14)) return 2;
        if (value < (1 << 21)) return 3;
        if (value < (1 << 28)) return 4;
        return 5;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private MetricType type = MetricType.UNKNOWN;
        private String metricFamilyName = "";
        private String help = "";
        private String unit = "";

        public Builder type(MetricType type) {
            this.type = type;
            return this;
        }

        public Builder metricFamilyName(String metricFamilyName) {
            this.metricFamilyName = metricFamilyName != null ? metricFamilyName : "";
            return this;
        }

        public Builder help(String help) {
            this.help = help != null ? help : "";
            return this;
        }

        public Builder unit(String unit) {
            this.unit = unit != null ? unit : "";
            return this;
        }

        public PrometheusMetricMetadata build() {
            return new PrometheusMetricMetadata(this);
        }
    }

    @Override
    public String toString() {
        return "PrometheusMetricMetadata{" +
                "type=" + type +
                ", metricFamilyName='" + metricFamilyName + '\'' +
                ", help='" + help + '\'' +
                ", unit='" + unit + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrometheusMetricMetadata that = (PrometheusMetricMetadata) o;

        if (type != that.type) return false;
        if (!metricFamilyName.equals(that.metricFamilyName)) return false;
        if (!help.equals(that.help)) return false;
        return unit.equals(that.unit);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + metricFamilyName.hashCode();
        result = 31 * result + help.hashCode();
        result = 31 * result + unit.hashCode();
        return result;
    }
}
