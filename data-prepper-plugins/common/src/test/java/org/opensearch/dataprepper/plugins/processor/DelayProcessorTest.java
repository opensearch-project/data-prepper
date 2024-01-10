/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DelayProcessorTest {
    @Mock
    private DelayProcessor.Configuration configuration;
    private List<Record<?>> records;
    private Duration delayDuration;

    @BeforeEach
    void setUp() {
        delayDuration = Duration.ofMillis(50);
        when(configuration.getDelayFor()).thenReturn(delayDuration);

        records = List.of(mock(Record.class), mock(Record.class), mock(Record.class));
    }

    private DelayProcessor createObjectUnderTest() {
        return new DelayProcessor(configuration);
    }

    @Test
    void isReadyForShutdown_returns_true() {
        assertThat(createObjectUnderTest().isReadyForShutdown(), equalTo(true));
    }

    @Test
    void execute_returns_input_records() {
        assertThat(createObjectUnderTest().execute(records),
                equalTo(records));
    }

    @Test
    void execute_takes_at_least_as_long_as_defined() {
        final Instant before = Instant.now();
        createObjectUnderTest().execute(records);
        final Instant after = Instant.now();

        assertThat(Duration.between(before, after), greaterThanOrEqualTo(delayDuration));
        assertThat(Duration.between(before, after), lessThanOrEqualTo(delayDuration.multipliedBy(2)));
    }
}