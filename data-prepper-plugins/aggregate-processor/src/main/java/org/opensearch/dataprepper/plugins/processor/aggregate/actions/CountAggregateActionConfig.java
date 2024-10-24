/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder
@JsonClassDescription("The <code>count</code> action counts events that belong to the same group and " +
        "generates a new event with values of the <code>identification_keys</code> and the count, which indicates the number of new events.")
public class CountAggregateActionConfig {
    static final String SUM_METRIC_NAME = "count";
    public static final String DEFAULT_COUNT_KEY = "aggr._count";
    public static final String DEFAULT_START_TIME_KEY = "aggr._start_time";
    public static final String DEFAULT_END_TIME_KEY = "aggr._end_time";

    @JsonPropertyDescription("Format of the aggregated event. Specifying <code>otel_metrics</code> outputs aggregate events in OTel metrics SUM type with count as value. " +
            "Specifying <code>raw</code> outputs aggregate events as with the <code>count_key</code> field as a count value and includes the <code>start_time_key</code> and <code>end_time_key</code> keys.")
    @JsonProperty(value = "output_format", defaultValue = "otel_metrics")
    OutputFormat outputFormat = OutputFormat.OTEL_METRICS;

    @JsonPropertyDescription("Metric name to be used when the OTel metrics format is used. The default value is <code>count</code>.")
    @JsonProperty(value = "metric_name", defaultValue = SUM_METRIC_NAME)
    String metricName = SUM_METRIC_NAME;

    @JsonPropertyDescription("The key in the aggregate event that will have the count value. " +
            "This is the count of events in the aggregation. Default name is <code>aggr._count</code>.")
    @JsonProperty(value = "count_key", defaultValue = DEFAULT_COUNT_KEY)
    String countKey = DEFAULT_COUNT_KEY;

    @JsonPropertyDescription("The key in the aggregate event that will have the start time of the aggregation. " +
            "Default name is <code>aggr._start_time</code>.")
    @JsonProperty(value = "start_time_key", defaultValue = DEFAULT_START_TIME_KEY)
    String startTimeKey = DEFAULT_START_TIME_KEY;

    @JsonPropertyDescription("The key in the aggregate event that will have the end time of the aggregation. " +
            "Default name is <code>aggr._end_time</code>.")
    @JsonProperty(value = "end_time_key", defaultValue = DEFAULT_END_TIME_KEY)
    String endTimeKey = DEFAULT_END_TIME_KEY;

    @JsonPropertyDescription("List of unique keys to count.")
    @JsonProperty("unique_keys")
    List<String> uniqueKeys = null;

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

    public OutputFormat getOutputFormat() {
        return outputFormat;
    }
}
