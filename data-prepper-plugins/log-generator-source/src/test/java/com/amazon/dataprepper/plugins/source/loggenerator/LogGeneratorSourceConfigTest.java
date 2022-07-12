/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loggenerator;



import org.junit.jupiter.api.Test;

import static com.amazon.dataprepper.plugins.source.loggenerator.LogGeneratorSourceConfig.DEFAULT_INTERVAL_SECONDS;
import static com.amazon.dataprepper.plugins.source.loggenerator.LogGeneratorSourceConfig.DEFAULT_LOG_COUNT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.time.Duration;

class LogGeneratorSourceConfigTest {
    @Test
    void GIVEN_defaultLogGeneratorSourceConfig_WHEN_getIntervalCalled_THEN_defaultConstantReturned() {
        assertThat(new LogGeneratorSourceConfig().getInterval(), equalTo(Duration.ofSeconds(DEFAULT_INTERVAL_SECONDS)));
    }

    @Test
    void GIVEN_defaultLogGeneratorSourceConfig_WHEN_getCountCalled_THEN_defaultConstantReturned() {
        assertThat(new LogGeneratorSourceConfig().getCount(), equalTo(DEFAULT_LOG_COUNT));
    }
}