/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.Set;
import java.util.List;
import java.util.HashSet;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotNull;

public class HistogramAggregateActionConfig {
    public static final String HISTOGRAM_METRIC_NAME = "histogram";
    public static final String DEFAULT_GENERATED_KEY_PREFIX = "aggr._";
    public static final String SUM_KEY = "sum";
    public static final String COUNT_KEY = "count";
    public static final String BUCKETS_KEY = "buckets";
    public static final String BUCKET_COUNTS_KEY = "bucket_counts";
    public static final String MIN_KEY = "min";
    public static final String MAX_KEY = "max";
    public static final String START_TIME_KEY = "startTime";
    public static final String END_TIME_KEY = "endTime";
    public static final String DURATION_KEY = "duration";
    public static final Set<String> validOutputFormats = new HashSet<>(Set.of(OutputFormat.OTEL_METRICS.toString(), OutputFormat.RAW.toString()));

    @JsonPropertyDescription("Name of the field in the events the histogram generates.")
    @JsonProperty("key")
    @NotNull
    String key;

    @JsonPropertyDescription("The name of units for the values in the key. For example, bytes, traces etc")
    @JsonProperty("units")
    @NotNull
    String units;

    @JsonPropertyDescription("Metric name to be used when otel format is used.")
    @JsonProperty("metric_name")
    String metricName = HISTOGRAM_METRIC_NAME;

    @JsonPropertyDescription("Key prefix used by all the fields created in the aggregated event. Having a prefix ensures that the names of the histogram event do not conflict with the field names in the event.")
    @JsonProperty("generated_key_prefix")
    String generatedKeyPrefix = DEFAULT_GENERATED_KEY_PREFIX;

    @JsonPropertyDescription("A list of buckets (values of type double) indicating the buckets in the histogram.")
    @JsonProperty("buckets")
    @NotNull
    List<Number> buckets;

    @JsonPropertyDescription("Format of the aggregated event. otel_metrics is the default output format which outputs in OTel metrics SUM type with count as value. Other options is - raw - which generates a JSON object with the count_key field as a count value and the start_time_key field with aggregation start time as value.")
    @JsonProperty("output_format")
    String outputFormat = OutputFormat.OTEL_METRICS.toString();

    @JsonPropertyDescription("A Boolean value indicating whether the histogram should include the min and max of the values in the aggregation.")
    @JsonProperty("record_minmax")
    boolean recordMinMax = false;

    public String getMetricName() {
        return metricName;
    }

    public boolean getRecordMinMax() {
        return recordMinMax;
    }

    public String getGeneratedKeyPrefix() {
        return generatedKeyPrefix;
    }

    public String getUnits() {
        return units;
    }

    public String getKey() {
        return key;
    }

    public String getSumKey() {
        return generatedKeyPrefix + SUM_KEY;
    }

    public String getMinKey() {
        return generatedKeyPrefix + MIN_KEY;
    }

    public String getMaxKey() {
        return generatedKeyPrefix + MAX_KEY;
    }

    public String getCountKey() {
        return generatedKeyPrefix + COUNT_KEY;
    }

    public String getBucketsKey() {
        return generatedKeyPrefix + BUCKETS_KEY;
    }

    public String getBucketCountsKey() {
        return generatedKeyPrefix + BUCKET_COUNTS_KEY;
    }

    public String getStartTimeKey() {
        return generatedKeyPrefix + START_TIME_KEY;
    }

    public String getEndTimeKey() {
        return generatedKeyPrefix + END_TIME_KEY;
    }

    public String getDurationKey() {
        return generatedKeyPrefix + DURATION_KEY;
    }

    public List<Number> getBuckets() {
        if (buckets.size() == 0) {
            throw new IllegalArgumentException("Bucket list must not be empty");
        }
        return buckets;
    }

    public String getOutputFormat() {
        if (!validOutputFormats.contains(outputFormat)) {
            throw new IllegalArgumentException("Unknown output format " + outputFormat);
        }
        return outputFormat;
    }
} 
