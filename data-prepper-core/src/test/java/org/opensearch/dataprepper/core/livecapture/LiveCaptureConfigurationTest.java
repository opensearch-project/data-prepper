/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LiveCaptureConfigurationTest {

    @Test
    void testDefaultConstructor() {
        LiveCaptureConfiguration config = new LiveCaptureConfiguration();

        assertThat(config.isDefaultEnabled(), is(false));
        assertThat(config.getDefaultRate(), equalTo(1.0));
        assertThat(config.getLiveCaptureOutputSinkConfig(), nullValue());
    }

    @Test
    void testParameterizedConstructorWithAllValues() {
        Object sinkConfig = new Object();
        LiveCaptureConfiguration config = new LiveCaptureConfiguration(true, 5.0, sinkConfig);

        assertThat(config.isDefaultEnabled(), is(true));
        assertThat(config.getDefaultRate(), equalTo(5.0));
        assertThat(config.getLiveCaptureOutputSinkConfig(), equalTo(sinkConfig));
    }

    @Test
    void testParameterizedConstructorWithNullValues() {
        LiveCaptureConfiguration config = new LiveCaptureConfiguration(null, null, null);

        assertThat(config.isDefaultEnabled(), is(false));
        assertThat(config.getDefaultRate(), equalTo(1.0));
        assertThat(config.getLiveCaptureOutputSinkConfig(), nullValue());
    }

    @Test
    void testParameterizedConstructorWithZeroRate() {
        assertThrows(IllegalArgumentException.class, () -> {
            new LiveCaptureConfiguration(true, 0.0, null);
        });
    }

    @Test
    void testParameterizedConstructorWithNegativeRate() {
        assertThrows(IllegalArgumentException.class, () -> {
            new LiveCaptureConfiguration(true, -1.0, null);
        });
    }

}