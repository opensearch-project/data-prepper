/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotNull;

@JsonPropertyOrder
@JsonClassDescription("The <code>sum</code> action sums the numeric value of a configured <code>key</code> for events that belong to the same " +
        "group and generates a new event with the values of the <code>identification_keys</code> and the accumulated sum.")
public class SumAggregateActionConfig {
    static final String SUM_METRIC_NAME = "sum";
    public static final String DEFAULT_COUNT_KEY = "aggr._count";

    @JsonPropertyDescription("Name of the field in the events to sum. The value of this field must be numeric.")
    @JsonProperty("key")
    @NotNull
    String key;

    @JsonPropertyDescription("Format of the aggregated event. otel_metrics is the default output format, which outputs in OTel metrics SUM type with the total as value. " +
            "raw outputs a JSON object with the total and the count of events that contributed to it.")
    @JsonProperty(value = "output_format", defaultValue = "otel_metrics")
    OutputFormat outputFormat = OutputFormat.OTEL_METRICS;

    @JsonPropertyDescription("Metric name to be used when the OTel metrics format is used. The default value is <code>sum</code>.")
    @JsonProperty(value = "metric_name", defaultValue = SUM_METRIC_NAME)
    String metricName = SUM_METRIC_NAME;

    @JsonPropertyDescription("The key in the aggregate event that will have the number of events that contributed to the sum. Default name is <code>aggr._count</code>.")
    @JsonProperty(value = "count_key", defaultValue = DEFAULT_COUNT_KEY)
    String countKey = DEFAULT_COUNT_KEY;

    public String getKey() {
        return key;
    }

    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getCountKey() {
        return countKey;
    }
}
