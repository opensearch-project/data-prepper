/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec.DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_metrics</code> processor serializes a collection of <code>ExportMetricsServiceRequest</code> records " +
        "sent from the <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sources/otel-metrics-source/\">OTel metrics source</a> into a collection of string records.")
public class OtelMetricsRawProcessorConfig {

    @JsonProperty(value = "flatten_attributes", defaultValue = "true")
    @JsonPropertyDescription("Whether or not to flatten the <code>attributes</code> field in the JSON data. Default value is <code>true</code>.")
    boolean flattenAttributesFlag = true;

    @JsonProperty(defaultValue = "true")
    @JsonPropertyDescription("Whether or not to calculate histogram buckets. Default value is <code>true</code>.")
    private Boolean calculateHistogramBuckets = true;

    @JsonProperty(defaultValue = "true")
    @JsonPropertyDescription("Whether or not to calculate exponential histogram buckets. Default value is <code>true</code>.")
    private Boolean calculateExponentialHistogramBuckets = true;

    @JsonPropertyDescription("Maximum allowed scale in exponential histogram calculation. By default, the maximum allowed scale is <code>10</code>.")
    private Integer exponentialHistogramMaxAllowedScale = DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE;

    public Boolean getCalculateExponentialHistogramBuckets() {
        return calculateExponentialHistogramBuckets;
    }

    public Boolean getCalculateHistogramBuckets() {
        return calculateHistogramBuckets;
    }

    public Integer getExponentialHistogramMaxAllowedScale() {
        return exponentialHistogramMaxAllowedScale;
    }

    public Boolean getFlattenAttributesFlag() {
        return flattenAttributesFlag;
    }
}
