/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LiveCaptureConfigurationTest {

    @Test
    void testDefaultConstructor() {
        final LiveCaptureConfiguration config = new LiveCaptureConfiguration();

        assertThat(config.isDefaultEnabled(), is(false));
        assertThat(config.getDefaultRate(), equalTo(1.0));
        assertThat(config.getSinkConfigurations().isEmpty(), is(true));
        assertThat(config.getFirstSinkConfiguration(), nullValue());
    }

    @Test
    void testParameterizedConstructorWithAllValues() {
        final Object sinkConfig = new Object();
        final List<Object> sinkConfigurations = List.of(sinkConfig);
        final LiveCaptureConfiguration config = new LiveCaptureConfiguration(true, 5.0, sinkConfigurations);

        assertThat(config.isDefaultEnabled(), is(true));
        assertThat(config.getDefaultRate(), equalTo(5.0));
        assertThat(config.getSinkConfigurations(), equalTo(sinkConfigurations));
        assertThat(config.getFirstSinkConfiguration(), equalTo(sinkConfig));
    }

    @Test
    void testParameterizedConstructorWithNullValues() {
        final LiveCaptureConfiguration config = new LiveCaptureConfiguration(null, null, null);

        assertThat(config.isDefaultEnabled(), is(false));
        assertThat(config.getDefaultRate(), equalTo(1.0));
        assertThat(config.getSinkConfigurations().isEmpty(), is(true));
        assertThat(config.getFirstSinkConfiguration(), nullValue());
    }

    @Test
    void testParameterizedConstructorWithZeroRate() {
        assertThrows(IllegalArgumentException.class, () -> {
            new LiveCaptureConfiguration(true, 0.0, Collections.emptyList());
        });
    }

    @Test
    void testParameterizedConstructorWithNegativeRate() {
        assertThrows(IllegalArgumentException.class, () -> {
            new LiveCaptureConfiguration(true, -1.0, Collections.emptyList());
        });
    }

    @Test
    void testParameterizedConstructorWithEmptyList() {
        final LiveCaptureConfiguration config = new LiveCaptureConfiguration(true, 2.0, Collections.emptyList());

        assertThat(config.isDefaultEnabled(), is(true));
        assertThat(config.getDefaultRate(), equalTo(2.0));
        assertThat(config.getSinkConfigurations().isEmpty(), is(true));
        assertThat(config.getFirstSinkConfiguration(), nullValue());
    }

    @Test
    void testParameterizedConstructorWithMultipleSinksThrowsException() {
        final List<Object> multipleSinks = List.of(new Object(), new Object());
        assertThrows(IllegalArgumentException.class, () -> {
            new LiveCaptureConfiguration(true, 1.0, multipleSinks);
        });
    }

}