/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class CloudWatchLogsLimitsTest {
    private static CloudWatchLogsLimits cloudWatchLogsLimits;
    private static ThresholdConfig thresholdConfig;

    @BeforeAll
    static void setUp() {
        thresholdConfig = new ThresholdConfig();
        cloudWatchLogsLimits = new CloudWatchLogsLimits(ThresholdConfig.DEFAULT_BATCH_SIZE, thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSizeBytes(), ThresholdConfig.DEFAULT_FLUSH_INTERVAL);
    }

    @ParameterizedTest(name = "offset from max event size {0} -> {1}")
    @CsvSource({
            "1, true",
            "0, false",
            "-1, false"
    })
    void isGreaterThanMaxEventSize_eventSizeRelativeToMax_returnsExpected(final int offsetFromMaxEventSize, final boolean expected) {
        final long eventSize = (thresholdConfig.getMaxEventSizeBytes() + offsetFromMaxEventSize)
                - CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE;
        assertThat(cloudWatchLogsLimits.isGreaterThanMaxEventSize(eventSize), is(expected));
    }
}
