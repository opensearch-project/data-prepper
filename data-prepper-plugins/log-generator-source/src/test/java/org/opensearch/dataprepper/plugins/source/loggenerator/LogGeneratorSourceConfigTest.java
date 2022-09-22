/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator;


import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.source.loggenerator.LogGeneratorSourceConfig.DEFAULT_INTERVAL_SECONDS;
import static org.opensearch.dataprepper.plugins.source.loggenerator.LogGeneratorSourceConfig.INFINITE_LOG_COUNT;

class LogGeneratorSourceConfigTest {
    @Test
    void GIVEN_defaultLogGeneratorSourceConfig_THEN_returnExpectedValues() {
        LogGeneratorSourceConfig objectUnderTest = new LogGeneratorSourceConfig();
        assertThat(objectUnderTest.getInterval(), equalTo(Duration.ofSeconds(DEFAULT_INTERVAL_SECONDS)));
        assertThat(objectUnderTest.getCount(), equalTo(INFINITE_LOG_COUNT));
    }
}