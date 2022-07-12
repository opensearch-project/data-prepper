/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loggenerator;



import org.junit.jupiter.api.Test;

import static com.amazon.dataprepper.plugins.source.loggenerator.LogGeneratorSourceConfig.DEFAULT_INTERVAL_SECONDS;
import static com.amazon.dataprepper.plugins.source.loggenerator.LogGeneratorSourceConfig.INFINITE_LOG_COUNT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.time.Duration;

class LogGeneratorSourceConfigTest {
    @Test
    void GIVEN_defaultLogGeneratorSourceConfig_THEN_returnExpectedValues() {
        LogGeneratorSourceConfig objectUnderTest = new LogGeneratorSourceConfig();
        assertThat(objectUnderTest.getInterval(), equalTo(Duration.ofSeconds(DEFAULT_INTERVAL_SECONDS)));
        assertThat(objectUnderTest.getCount(), equalTo(INFINITE_LOG_COUNT));
    }
}