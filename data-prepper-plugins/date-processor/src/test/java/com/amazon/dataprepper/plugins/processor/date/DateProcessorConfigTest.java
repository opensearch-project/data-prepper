/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class DateProcessorConfigTest {
    @Test
    void testDefaultConfig() {
        final DateProcessorConfig dateProcessorConfig = new DateProcessorConfig();

        assertThat(dateProcessorConfig.getDestination(), equalTo(DateProcessorConfig.DEFAULT_DESTINATION));
        assertThat(dateProcessorConfig.getTimezone(), equalTo(DateProcessorConfig.DEFAULT_TIMEZONE));
    }
}