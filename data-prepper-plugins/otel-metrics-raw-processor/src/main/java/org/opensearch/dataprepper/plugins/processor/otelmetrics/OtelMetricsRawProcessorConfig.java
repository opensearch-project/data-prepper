/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OtelMetricsRawProcessorConfig {

    @JsonProperty("calculateHistogramBuckets")
    private Boolean calculateHistogramBuckets = false;

    @JsonProperty("calculateExponentialHistogramBuckets")
    private Boolean calculateExponentialHistogramBuckets = false;

    public Boolean getCalculateExponentialHistogramBuckets() {
        return calculateExponentialHistogramBuckets;
    }

    public Boolean getCalculateHistogramBuckets() {
        return calculateHistogramBuckets;
    }
}
