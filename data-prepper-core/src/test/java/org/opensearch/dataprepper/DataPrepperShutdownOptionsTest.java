/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataPrepperShutdownOptionsTest {
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
    }

    @Test
    void defaultOptions_returns_correct_defaults() {
        final DataPrepperShutdownOptions options = DataPrepperShutdownOptions.defaultOptions();

        assertThat(options.getBufferDrainTimeout(), nullValue());
        assertThat(options.getBufferReadTimeout(), nullValue());
    }

    @Test
    void builder_returns_valid_builder() {
        final DataPrepperShutdownOptions.Builder builder = DataPrepperShutdownOptions.builder();

        assertThat(builder, notNullValue());
    }

    @Test
    void build_throws_if_bufferReadTimeout_is_greater_than_bufferDrainTimeout() {
        final Duration bufferDrainTimeout = Duration.ofSeconds(random.nextInt(20));
        final Duration bufferReadTimeout = bufferDrainTimeout.plus(1, ChronoUnit.MILLIS);
        final DataPrepperShutdownOptions.Builder builder = DataPrepperShutdownOptions.builder()
                .withBufferDrainTimeout(bufferDrainTimeout)
                .withBufferReadTimeout(bufferReadTimeout);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void build_creates_new_options_with_bufferReadTimeout_equal_to_bufferDrainTimeout() {
        final Duration timeout = Duration.ofSeconds(random.nextInt(20));
        final DataPrepperShutdownOptions dataPrepperShutdownOptions = DataPrepperShutdownOptions.builder()
                .withBufferDrainTimeout(timeout)
                .withBufferReadTimeout(timeout)
                .build();


        assertThat(dataPrepperShutdownOptions, notNullValue());
        assertThat(dataPrepperShutdownOptions.getBufferReadTimeout(), equalTo(timeout));
        assertThat(dataPrepperShutdownOptions.getBufferDrainTimeout(), equalTo(timeout));
    }

    @Test
    void build_creates_new_options_with_bufferReadTimeout_less_than_bufferDrainTimeout() {
        final Duration bufferReadTimeout = Duration.ofSeconds(random.nextInt(20));
        final Duration bufferDrainTimeout = Duration.ofSeconds(random.nextInt(20)).plus(bufferReadTimeout);
        final DataPrepperShutdownOptions dataPrepperShutdownOptions = DataPrepperShutdownOptions.builder()
                .withBufferDrainTimeout(bufferDrainTimeout)
                .withBufferReadTimeout(bufferReadTimeout)
                .build();


        assertThat(dataPrepperShutdownOptions, notNullValue());
        assertThat(dataPrepperShutdownOptions.getBufferReadTimeout(), equalTo(bufferReadTimeout));
        assertThat(dataPrepperShutdownOptions.getBufferDrainTimeout(), equalTo(bufferDrainTimeout));
    }
}