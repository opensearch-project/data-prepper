/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec.DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OtelMetricsRawProcessorConfig {

    @JsonProperty("flatten_attributes")
    boolean flattenAttributesFlag = true;

    private Boolean calculateHistogramBuckets = true;

    private Boolean calculateExponentialHistogramBuckets = true;

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
