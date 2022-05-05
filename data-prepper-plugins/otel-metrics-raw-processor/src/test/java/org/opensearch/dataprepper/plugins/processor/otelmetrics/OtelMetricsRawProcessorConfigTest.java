/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class OtelMetricsRawProcessorConfigTest {

    @Test
    void testDefaultConfig() {
        final OtelMetricsRawProcessorConfig dateProcessorConfig = new OtelMetricsRawProcessorConfig();

        assertThat(dateProcessorConfig.getCalculateExponentialHistogramBuckets(), equalTo(false));
        assertThat(dateProcessorConfig.getCalculateHistogramBuckets(), equalTo(false));
        assertThat(dateProcessorConfig.getExponentialHistogramMaxAllowedScale(), equalTo(10));
    }

}
