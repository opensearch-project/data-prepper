/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

public class CountAggregateActionConfig {
    static final String SUM_METRIC_NAME = "count";
    public static final String DEFAULT_COUNT_KEY = "aggr._count";
    public static final String DEFAULT_START_TIME_KEY = "aggr._start_time";
    public static final String DEFAULT_END_TIME_KEY = "aggr._end_time";
    public static final Set<String> validOutputFormats = new HashSet<>(Set.of(OutputFormat.OTEL_METRICS.toString(), OutputFormat.RAW.toString()));

    @JsonPropertyDescription("Key used for storing the count. Default name is aggr._count.")
    @JsonProperty("count_key")
    String countKey = DEFAULT_COUNT_KEY;

    @JsonPropertyDescription("Metric name to be used when otel format is used.")
    @JsonProperty("metric_name")
    String metricName = SUM_METRIC_NAME;

    @JsonPropertyDescription("List of unique keys to count.")
    @JsonProperty("unique_keys")
    List<String> uniqueKeys = null;

    @JsonPropertyDescription("Key used for storing the start time. Default name is aggr._start_time.")
    @JsonProperty("start_time_key")
    String startTimeKey = DEFAULT_START_TIME_KEY;

    @JsonPropertyDescription("Key used for storing the end time. Default name is aggr._end_time.")
    @JsonProperty("end_time_key")
    String endTimeKey = DEFAULT_END_TIME_KEY;

    @JsonPropertyDescription("Format of the aggregated event. otel_metrics is the default output format which outputs in OTel metrics SUM type with count as value. Other options is - raw - which generates a JSON object with the count_key field as a count value and the start_time_key field with aggregation start time as value.")
    @JsonProperty("output_format")
    String outputFormat = OutputFormat.OTEL_METRICS.toString();

    public String getMetricName() {
        return metricName;
    }

    public List<String> getUniqueKeys() {
        return uniqueKeys;
    }

    public String getCountKey() {
        return countKey;
    }

    public String getEndTimeKey() {
        return endTimeKey;
    }

    public String getStartTimeKey() {
        return startTimeKey;
    }

    public String getOutputFormat() {
        if (!validOutputFormats.contains(outputFormat)) {
            throw new IllegalArgumentException("Unknown output format " + outputFormat);
        }
        return outputFormat;
    }
}
