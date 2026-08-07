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
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@DataPrepperPluginTest(pluginName = "wrap_entries", pluginType = Processor.class)
class WrapEntriesProcessorIT extends BaseDataPrepperPluginStandardTestSuite {

    @Test
    void doExecute_with_wrap_entries_when_expression_filters_records(
            @PluginConfigurationFile("wrap_entries_when_filters_records.yaml")
            final Processor<Record<Event>, Record<Event>> objectUnderTest) {
        final Record<Event> matchingRecord = new Record<>(JacksonEvent.builder()
                .withEventType("event")
                .withData(Map.of("names", Arrays.asList("alpha", "beta"), "type", "users"))
                .build());

        final Record<Event> nonMatchingRecord = new Record<>(JacksonEvent.builder()
                .withEventType("event")
                .withData(Map.of("names", Arrays.asList("gamma"), "type", "other"))
                .build());

        final Collection<Record<Event>> result = objectUnderTest.execute(Arrays.asList(matchingRecord, nonMatchingRecord));
        final List<Record<Event>> outputRecords = (List<Record<Event>>) result;

        final List<Map<String, Object>> matchedOutput = outputRecords.get(0).getData().get("/names", List.class);
        assertThat(matchedOutput, equalTo(Arrays.asList(Map.of("name", "alpha"), Map.of("name", "beta"))));

        final List<?> unmatchedOutput = outputRecords.get(1).getData().get("/names", List.class);
        assertThat(unmatchedOutput, equalTo(Arrays.asList("gamma")));
    }
}
