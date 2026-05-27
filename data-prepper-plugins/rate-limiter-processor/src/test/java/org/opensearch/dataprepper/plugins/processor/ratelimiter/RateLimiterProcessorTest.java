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
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@DataPrepperPluginTest(pluginName = "rate_limiter", pluginType = Processor.class)
class RateLimiterProcessorTest extends BaseDataPrepperPluginStandardTestSuite {

    @Test
    void test_events_under_limit_all_pass_through(
            @PluginConfigurationFile("rate_limiter_default.yaml") final Processor<Record<Event>, Record<Event>> processor,
            final EventFactory eventFactory) {
        final List<Record<Event>> records = createEvents(eventFactory, 200);

        final Collection<Record<Event>> result = processor.execute(records);

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(200));
    }

    @Test
    void test_events_over_limit_are_dropped(
            @PluginConfigurationFile("rate_limiter_default.yaml") final Processor<Record<Event>, Record<Event>> processor,
            final EventFactory eventFactory) {
        final List<Record<Event>> records = createEvents(eventFactory, 500);

        final Collection<Record<Event>> result = processor.execute(records);

        assertThat(result, notNullValue());
        assertThat(result.size(), lessThanOrEqualTo(400));
    }

    @Test
    void test_counts_accumulate_across_multiple_execute_calls(
            @PluginConfigurationFile("rate_limiter_default.yaml") final Processor<Record<Event>, Record<Event>> processor,
            final EventFactory eventFactory) {
        final Collection<Record<Event>> result1 = processor.execute(createEvents(eventFactory, 300));
        final Collection<Record<Event>> result2 = processor.execute(createEvents(eventFactory, 200));

        assertThat(result1.size() + result2.size(), lessThanOrEqualTo(400));
    }

    @Test
    void test_limit_when_only_rate_limits_matching_events(
            @PluginConfigurationFile("rate_limiter_with_limit_when.yaml") final Processor<Record<Event>, Record<Event>> processor,
            final EventFactory eventFactory) {
        final List<Record<Event>> records = new ArrayList<>();
        records.addAll(createEvents(eventFactory, 5, Map.of("message", "test", "level", "DEBUG")));
        records.addAll(createEvents(eventFactory, 5, Map.of("message", "test", "level", "INFO")));

        final Collection<Record<Event>> result = processor.execute(records);

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(7));
    }

    @Test
    void test_hold_mode(
            @PluginConfigurationFile("rate_limiter_hold_mode.yaml") final Processor<Record<Event>, Record<Event>> processor,
            final EventFactory eventFactory) {
        final List<Record<Event>> records = createEvents(eventFactory, 3);
        final Collection<Record<Event>> result = processor.execute(records);
        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(3));
    }

    private List<Record<Event>> createEvents(final EventFactory eventFactory, final int count) {
        return createEvents(eventFactory, count, Map.of("message", "test"));
    }

    private List<Record<Event>> createEvents(final EventFactory eventFactory, final int count, final Map<String, Object> data) {
        final List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final Event event = eventFactory.eventBuilder(LogEventBuilder.class)
                    .withData(data)
                    .build();
            records.add(new Record<>(event));
        }
        return records;
    }
}
