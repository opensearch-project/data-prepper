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
    public static final String DEFAULT_GENERATED_KEY_PREFIX = "aggr._";
    public static final Set<String> validOutputFormats = new HashSet<>(Set.of(OutputFormat.OTEL_METRICS.toString(), OutputFormat.RAW.toString()));

    @JsonProperty("key")
    @NotNull
    String key;

    @JsonProperty("units")
    @NotNull
    String units;

    @JsonProperty("generated_key_prefix")
    String generatedKeyPrefix = DEFAULT_GENERATED_KEY_PREFIX;

    @JsonProperty("buckets")
    @NotNull
    List<Number> buckets;

    @JsonProperty("output_format")
    String outputFormat = OutputFormat.OTEL_METRICS.toString();

    @JsonProperty("record_minmax")
    boolean recordMinMax = false;

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
