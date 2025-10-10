/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.parse.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@DataPrepperPluginTest(pluginName = "parse_json", pluginType = Processor.class)
class ParseJsonProcessorIT extends BaseDataPrepperPluginStandardTestSuite {
    private ObjectMapper objectMapper;
    private Random random;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        random = new Random();
    }

    @Test
    void parse_json_with_default_configuration(
            @PluginConfigurationFile("default.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest,
            final EventFactory eventFactory) throws JsonProcessingException {

        final List<Event> inputEvents = new LinkedList<>();
        final List<Map<String, Object>> messageMaps = new LinkedList<>();
        final List<String> messageStrings = new LinkedList<>();
        for (int i = 0; i < 5; i++) {

            final Map<String, Object> messageMap = Map.of(
                    "stringKey", UUID.randomUUID().toString(),
                    "integerKey", random.nextInt(10_000) + 10,
                    "objectKey", Map.of(
                            "nestedKey", UUID.randomUUID().toString()
                    ),
                    "arrayKey", List.of(
                            UUID.randomUUID().toString(),
                            UUID.randomUUID().toString()
                    )
            );

            final String messageString = objectMapper.writeValueAsString(messageMap);

            final Event event = eventFactory.eventBuilder(LogEventBuilder.class)
                    .withData(Map.of(
                            "message", messageString
                            , "some_other_key", UUID.randomUUID().toString()
                    ))
                    .build();

            inputEvents.add(event);
            messageMaps.add(messageMap);
            messageStrings.add(messageString);
        }

        final List<Record<Event>> inputRecords = inputEvents.stream()
                .map(Record::new)
                .collect(Collectors.toList());

        final Collection<Record<Event>> outputRecords = objectUnderTest.execute(inputRecords);

        assertThat(outputRecords, notNullValue());

        final List<Event> outputEvents = outputRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(outputEvents, equalTo(inputEvents));

        assertThat(outputEvents.size(), equalTo(5));

        for (int i = 0; i < outputEvents.size(); i++) {
            final Event event = outputEvents.get(i);
            assertThat(event, notNullValue());
            assertThat(event.get("message", String.class), equalTo(messageStrings.get(i)));
            assertThat(event.get("stringKey", String.class), equalTo(messageMaps.get(i).get("stringKey")));
            assertThat(event.get("integerKey", Integer.class), equalTo(messageMaps.get(i).get("integerKey")));
            assertThat(event.get("objectKey", Map.class), equalTo(messageMaps.get(i).get("objectKey")));
            assertThat(event.get("arrayKey", List.class), equalTo(messageMaps.get(i).get("arrayKey")));
        }
    }

    @Test
    void parse_json_with_destination(
            @PluginConfigurationFile("with-destination.yaml") final Processor<Record<Event>, Record<Event>> objectUnderTest,
            final EventFactory eventFactory) throws JsonProcessingException {

        final List<Event> inputEvents = new LinkedList<>();
        final List<Map<String, Object>> messageMaps = new LinkedList<>();
        final List<String> messageStrings = new LinkedList<>();
        for (int i = 0; i < 5; i++) {

            final Map<String, Object> messageMap = Map.of(
                    "stringKey", UUID.randomUUID().toString(),
                    "integerKey", random.nextInt(10_000) + 10,
                    "objectKey", Map.of(
                            "nestedKey", UUID.randomUUID().toString()
                    ),
                    "arrayKey", List.of(
                            UUID.randomUUID().toString(),
                            UUID.randomUUID().toString()
                    )
            );

            final String messageString = objectMapper.writeValueAsString(messageMap);

            final Event event = eventFactory.eventBuilder(LogEventBuilder.class)
                    .withData(Map.of(
                            "message", messageString
                            , "some_other_key", UUID.randomUUID().toString()
                    ))
                    .build();

            inputEvents.add(event);
            messageMaps.add(messageMap);
            messageStrings.add(messageString);
        }

        final List<Record<Event>> inputRecords = inputEvents.stream()
                .map(Record::new)
                .collect(Collectors.toList());

        final Collection<Record<Event>> outputRecords = objectUnderTest.execute(inputRecords);

        assertThat(outputRecords, notNullValue());

        final List<Event> outputEvents = outputRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(outputEvents, equalTo(inputEvents));

        assertThat(outputEvents.size(), equalTo(5));

        for (int i = 0; i < outputEvents.size(); i++) {
            final Event event = outputEvents.get(i);
            assertThat(event, notNullValue());
            assertThat(event.get("message", String.class), equalTo(messageStrings.get(i)));
            assertThat(event.get("parsed_json", Map.class), equalTo(messageMaps.get(i)));
        }
    }
}