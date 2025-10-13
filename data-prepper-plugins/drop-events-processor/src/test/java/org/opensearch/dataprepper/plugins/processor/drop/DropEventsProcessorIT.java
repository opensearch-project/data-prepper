/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.drop;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@DataPrepperPluginTest(pluginName = "drop_events", pluginType = Processor.class)
class DropEventsProcessorIT extends BaseDataPrepperPluginStandardTestSuite {
    @Test
    void drops_records_when_value_is_empty_string(
            @PluginConfigurationFile("drop_when_value_is_empty_string.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest,
            final EventFactory eventFactory) {

        final List<Event> allEvents = new LinkedList<>();
        final List<Event> expectedEventsAfterProcessor = new LinkedList<>();
        for(int i = 0; i < 5; i++) {

            final boolean shouldKeepEvent = i % 2 == 0;

            final String keyValue = shouldKeepEvent ? UUID.randomUUID().toString() : "";
            final Event event = eventFactory.eventBuilder(LogEventBuilder.class)
                    .withData(Map.of(
                            "my_key", keyValue
                            ,"some_other_key", UUID.randomUUID().toString()
                    ))
                    .build();

            allEvents.add(event);
            if(shouldKeepEvent) {
                expectedEventsAfterProcessor.add(event);
            }
        }

        final List<Record<Event>> inputRecords = allEvents.stream()
                .map(Record::new)
                .collect(Collectors.toList());

        final Collection<Record<Event>> outputRecords = objectUnderTest.execute(inputRecords);

        assertThat(outputRecords, notNullValue());

        final List<Event> outputEvents = outputRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(outputEvents, equalTo(expectedEventsAfterProcessor));
    }
}