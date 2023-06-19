/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StdOutSinkTests {
    private static String PLUGIN_NAME = "stdout";

    List<Record<Object>> testRecords;


    @BeforeEach
    public void setup() {
        testRecords = new ArrayList<>();
        Map<String, Object> firstTestData = new HashMap<>();
        Map<String, Object> secondTestData = new HashMap<>();

        firstTestData.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        secondTestData.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        final Record<Object> firstTestRecord = new Record<>(JacksonEvent
                .builder()
                .withEventType("event")
                .withData(firstTestData)
                .build());

        final Record<Object> secondTestRecord = new Record<>(JacksonEvent
                .builder()
                .withEventType("event")
                .withData(secondTestData)
                .build());

        testRecords.add(firstTestRecord);
        testRecords.add(secondTestRecord);
    }

    @Test
    public void testSinkWithEvents() {
        final StdOutSink stdOutSink = new StdOutSink(new PluginSetting(PLUGIN_NAME, new HashMap<>()), null);
        stdOutSink.output(testRecords);
        stdOutSink.shutdown();
    }

    // TODO: remove with the completion of: https://github.com/opensearch-project/data-prepper/issues/546
    @Test
    public void testSinkWithCustomType() {
        final StdOutSink stdOutSink = new StdOutSink(new PluginSetting(PLUGIN_NAME, new HashMap<>()), null);
        stdOutSink.output(Collections.singletonList(new Record<Object>(new TestObject())));
    }

    private static class TestObject {

    }
}
