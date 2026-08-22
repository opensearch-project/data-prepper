/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HoldForeverSinkTest {
    @Mock
    private HoldForeverSink.HoldForeverSinkConfig sinkConfig;
    private List<Event> events;
    private List<Record<Event>> records;

    @BeforeEach
    void setUp() {
        when(sinkConfig.getOutputFrequency()).thenReturn(Duration.ZERO);

        events = IntStream.range(0, 3)
                .mapToObj(i -> mock(Event.class))
                .collect(Collectors.toList());
        records = events.stream()
                .map(Record::new)
                .collect(Collectors.toList());

    }

    private HoldForeverSink createObjectUnderTest() {
        return new HoldForeverSink(sinkConfig);
    }

    @Test
    void output_should_not_release_when_logging() throws InterruptedException {
        Thread.sleep(50);

        createObjectUnderTest().output((Collection) records);

        for (final Event event : events) {
            verify(event, never()).getEventHandle();
        }
    }

    @Test
    void output_should_not_release_when_not_logging() {
        reset(sinkConfig);
        when(sinkConfig.getOutputFrequency()).thenReturn(Duration.ofDays(30));

        createObjectUnderTest().output((Collection) records);

        for (final Event event : events) {
            verify(event, never()).getEventHandle();
        }
    }

    @Test
    void isReady_returns_true() {
        assertThat(createObjectUnderTest().isReady(), equalTo(true));
    }

    @Test
    void initialize_is_ok() {
        createObjectUnderTest().initialize();
    }

    @Test
    void shutdown_is_ok() {
        createObjectUnderTest().shutdown();
    }
}