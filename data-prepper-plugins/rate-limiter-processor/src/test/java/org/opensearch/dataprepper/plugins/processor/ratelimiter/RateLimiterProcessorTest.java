/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.ratelimiter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RateLimiterProcessorTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private RateLimiterProcessorConfig config;

    private RateLimiterProcessor createObjectUnderTest(final int eventsPerSecond) {
        when(config.getEventsPerSecond()).thenReturn(eventsPerSecond);
        when(config.getCounterRetentionSeconds()).thenReturn(60);
        return new RateLimiterProcessor(pluginMetrics, config);
    }

    private List<Record<Event>> createEventsInSecond(final Instant second, final int count) {
        final List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final Event event = JacksonEvent.builder()
                    .withEventType("event")
                    .withData(Collections.singletonMap("message", "test-" + i))
                    .withTimeReceived(second.plusMillis(i))
                    .build();
            records.add(new Record<>(event));
        }
        return records;
    }

    @Test
    void test_events_under_limit_all_pass_through() {
        final RateLimiterProcessor processor = createObjectUnderTest(400);
        final Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        final Collection<Record<Event>> result = processor.doExecute(createEventsInSecond(now, 200));

        assertThat(result.size(), equalTo(200));
    }

    @Test
    void test_events_over_limit_are_dropped() {
        final RateLimiterProcessor processor = createObjectUnderTest(400);
        final Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        final Collection<Record<Event>> result = processor.doExecute(createEventsInSecond(now, 500));

        assertThat(result.size(), equalTo(400));
    }

    @Test
    void test_events_from_different_seconds_are_counted_independently() {
        final RateLimiterProcessor processor = createObjectUnderTest(400);
        final Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        final Instant nextSecond = now.plusSeconds(1);

        final List<Record<Event>> records = new ArrayList<>();
        records.addAll(createEventsInSecond(now, 500));
        records.addAll(createEventsInSecond(nextSecond, 500));

        final Collection<Record<Event>> result = processor.doExecute(records);

        assertThat(result.size(), equalTo(800));
    }

    @Test
    void test_counts_accumulate_across_multiple_doExecute_calls() {
        final RateLimiterProcessor processor = createObjectUnderTest(400);
        final Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);

        final Collection<Record<Event>> result1 = processor.doExecute(createEventsInSecond(now, 300));
        final Collection<Record<Event>> result2 = processor.doExecute(createEventsInSecond(now, 200));

        assertThat(result1.size(), equalTo(300));
        assertThat(result2.size(), equalTo(100));
    }
}
