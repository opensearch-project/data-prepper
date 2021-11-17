
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

    List<Record<Object>> TEST_RECORDS;


    @BeforeEach
    public void setup() {
        TEST_RECORDS = new ArrayList<>();
        Map<String, Object> TEST_DATA_1 = new HashMap<>();
        Map<String, Object> TEST_DATA_2 = new HashMap<>();

        TEST_DATA_1.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        TEST_DATA_2.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        final Record<Object> TEST_RECORD_1 = new Record<>(JacksonEvent
                .builder()
                .withEventType("event")
                .withData(TEST_DATA_1)
                .build());

        final Record<Object> TEST_RECORD_2 = new Record<>(JacksonEvent
                .builder()
                .withEventType("event")
                .withData(TEST_DATA_2)
                .build());

        TEST_RECORDS.add(TEST_RECORD_1);
        TEST_RECORDS.add(TEST_RECORD_2);
    }

    @Test
    public void testSinkWithEvents() {
        final StdOutSink stdOutSink = new StdOutSink(new PluginSetting(PLUGIN_NAME, new HashMap<>()));
        stdOutSink.output(TEST_RECORDS);
        stdOutSink.shutdown();
    }

    @Test
    public void testSinkWithString() {
        final StdOutSink stdOutSink = new StdOutSink(new PluginSetting(PLUGIN_NAME, new HashMap<>()));
        stdOutSink.output(Collections.singletonList(new Record<Object>(UUID.randomUUID().toString())));
    }
}
