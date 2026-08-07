/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@DataPrepperPluginTest(pluginName = "filter_list", pluginType = Processor.class)
class FilterListProcessorIT extends BaseDataPrepperPluginStandardTestSuite {

    @Test
    void filters_objects_by_field_value(
            @PluginConfigurationFile("filter_objects_by_status.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest,
            final EventFactory eventFactory) {

        final Event event = eventFactory.eventBuilder(LogEventBuilder.class)
                .withData(Map.of("items", List.of(
                        Map.of("name", "item1", "status", "active"),
                        Map.of("name", "item2", "status", "inactive"),
                        Map.of("name", "item3", "status", "active")
                )))
                .build();

        final Collection<Record<Event>> result = objectUnderTest.execute(Collections.singletonList(new Record<>(event)));

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));

        final Event resultEvent = result.iterator().next().getData();
        final List<Map<String, Object>> filteredList = resultEvent.get("items", List.class);
        assertThat(filteredList, notNullValue());
        assertThat(filteredList.size(), equalTo(2));
        assertThat(filteredList.get(0).get("name"), equalTo("item1"));
        assertThat(filteredList.get(1).get("name"), equalTo("item3"));
    }

    @Test
    void filters_objects_to_separate_target(
            @PluginConfigurationFile("filter_objects_to_target.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest,
            final EventFactory eventFactory) {

        final Event event = eventFactory.eventBuilder(LogEventBuilder.class)
                .withData(Map.of("items", List.of(
                        Map.of("name", "item1", "status", "active"),
                        Map.of("name", "item2", "status", "inactive")
                )))
                .build();

        final Collection<Record<Event>> result = objectUnderTest.execute(Collections.singletonList(new Record<>(event)));

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));

        final Event resultEvent = result.iterator().next().getData();

        final List<Map<String, Object>> originalList = resultEvent.get("items", List.class);
        assertThat(originalList.size(), equalTo(2));

        final List<Map<String, Object>> filteredList = resultEvent.get("active_items", List.class);
        assertThat(filteredList, notNullValue());
        assertThat(filteredList.size(), equalTo(1));
        assertThat(filteredList.get(0).get("name"), equalTo("item1"));
    }

    @Test
    void filters_primitives_with_filter_list_when_condition(
            @PluginConfigurationFile("filter_primitives_with_condition.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest,
            final EventFactory eventFactory) {

        final Event event = eventFactory.eventBuilder(LogEventBuilder.class)
                .withData(Map.of(
                        "scores", List.of(95, 30, 75, 10, 88),
                        "type", "grades"
                ))
                .build();

        final Collection<Record<Event>> result = objectUnderTest.execute(Collections.singletonList(new Record<>(event)));

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));

        final Event resultEvent = result.iterator().next().getData();
        final List<Integer> filteredList = resultEvent.get("scores", List.class);
        assertThat(filteredList, notNullValue());
        assertThat(filteredList.size(), equalTo(3));
        assertThat(filteredList.get(0), equalTo(95));
        assertThat(filteredList.get(1), equalTo(75));
        assertThat(filteredList.get(2), equalTo(88));
    }

    @Test
    void skips_filtering_when_filter_list_when_is_false(
            @PluginConfigurationFile("filter_primitives_with_condition.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest,
            final EventFactory eventFactory) {

        final Event event = eventFactory.eventBuilder(LogEventBuilder.class)
                .withData(Map.of(
                        "scores", List.of(95, 30, 75, 10, 88),
                        "type", "not_grades"
                ))
                .build();

        final Collection<Record<Event>> result = objectUnderTest.execute(Collections.singletonList(new Record<>(event)));

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));

        final Event resultEvent = result.iterator().next().getData();
        final List<Integer> scores = resultEvent.get("scores", List.class);
        assertThat(scores, notNullValue());
        assertThat(scores.size(), equalTo(5));
    }
}
