/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.Set;
import java.util.HashSet;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HistogramAggregateActionConfig {
    public static final String DEFAULT_COUNT_KEY = "aggr._count";
    public static final String DEFAULT_HISTOGRAM_KEY = "aggr._histogram";
    public static final String DEFAULT_SUM_KEY = "aggr._sum";
    public static final String DEFAULT_MIN_KEY = "aggr._min";
    public static final String DEFAULT_MAX_KEY = "aggr._max";
    public static final String DEFAULT_START_TIME_KEY = "aggr._start_time";
    public static final String DEFAULT_END_TIME_KEY = "aggr._end_time";
    public static final Set<String> validOutputFormats = new HashSet<>(Set.of(OutputFormat.OTEL_METRICS.toString(), OutputFormat.RAW.toString()));

    @JsonProperty("count_key")
    String countKey = DEFAULT_COUNT_KEY;

    @JsonProperty("start_time_key")
    String startTimeKey = DEFAULT_START_TIME_KEY;

    @JsonProperty("output_format")
    String outputFormat = OutputFormat.OTEL_METRICS.toString();

    public String getCountKey() {
        return countKey;
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
