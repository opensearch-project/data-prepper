/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AcknowledgementsConfigTest {
    @Test
    void defaultConfiguration_should_return_non_null_config() {
        final AcknowledgementsConfig objectUnderTest = AcknowledgementsConfig.defaultConfiguration();
        assertThat(objectUnderTest, notNullValue());
    }

    @Test
    void defaultConfiguration_should_return_config_with_default_shutdownTimeout() {
        final AcknowledgementsConfig objectUnderTest = AcknowledgementsConfig.defaultConfiguration();
        assertThat(objectUnderTest.getShutdownTimeout(), equalTo(Duration.ofSeconds(5)));
    }

    @Test
    void setShutdownTimeout_with_positive_value_updates_shutdownTimeout() {
        final AcknowledgementsConfig objectUnderTest = new AcknowledgementsConfig();
        objectUnderTest.setShutdownTimeout(Duration.ofSeconds(30));
        assertThat(objectUnderTest.getShutdownTimeout(), equalTo(Duration.ofSeconds(30)));
    }

    @Test
    void setShutdownTimeout_with_null_throws() {
        final AcknowledgementsConfig objectUnderTest = new AcknowledgementsConfig();
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.setShutdownTimeout(null));
    }

    @Test
    void setShutdownTimeout_with_zero_throws() {
        final AcknowledgementsConfig objectUnderTest = new AcknowledgementsConfig();
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.setShutdownTimeout(Duration.ZERO));
    }

    @Test
    void setShutdownTimeout_with_negative_value_throws() {
        final AcknowledgementsConfig objectUnderTest = new AcknowledgementsConfig();
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.setShutdownTimeout(Duration.ofSeconds(-1)));
    }
}
