
/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

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
        final StdOutSink stdOutSink = new StdOutSink(new PluginSetting(PLUGIN_NAME, new HashMap<>()));
        stdOutSink.output(testRecords);
        stdOutSink.shutdown();
    }

    @Test
    public void testSinkWithString() {
        final StdOutSink stdOutSink = new StdOutSink(new PluginSetting(PLUGIN_NAME, new HashMap<>()));
        stdOutSink.output(Collections.singletonList(new Record<Object>(UUID.randomUUID().toString())));
    }
}
