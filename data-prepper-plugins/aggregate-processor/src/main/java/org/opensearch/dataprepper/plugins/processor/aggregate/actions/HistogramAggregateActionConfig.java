/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.Set;
import java.util.List;
import java.util.HashSet;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    @JsonProperty("key")
    @NotNull
    String key;

    @JsonProperty("units")
    @NotNull
    String units;

    @JsonProperty("name")
    String name = HISTOGRAM_METRIC_NAME;

    @JsonProperty("generated_key_prefix")
    String generatedKeyPrefix = DEFAULT_GENERATED_KEY_PREFIX;

    @JsonProperty("buckets")
    @NotNull
    List<Number> buckets;

    @JsonProperty("output_format")
    String outputFormat = OutputFormat.OTEL_METRICS.toString();

    @JsonProperty("record_minmax")
    boolean recordMinMax = false;

    public String getName() {
        return name;
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
