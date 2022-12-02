/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.Set;
import java.util.HashSet;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CountAggregateActionConfig {
    public static final String DEFAULT_COUNT_KEY = "aggr._count";
    public static final Set<String> validOutputFormats = new HashSet<>(Set.of(OutputFormat.OTEL_METRICS.toString(), OutputFormat.DEFAULT.toString()));

    @JsonProperty("countKey")
    String countKey = DEFAULT_COUNT_KEY;

    @JsonProperty("outputFormat")
    String outputFormat = OutputFormat.DEFAULT.toString();

    public String getCountKey() {
        return countKey;
    }

    public String getOutputFormat() {
        if (!validOutputFormats.contains(outputFormat)) {
            throw new IllegalArgumentException("Unknown output format " + outputFormat);
        }
        return outputFormat;
    }
} 
