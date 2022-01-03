/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.drop;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class DropEventsProcessorTests {
    private String messageInput;
    private DropEventsProcessor dropProcessor;
    private PluginSetting pluginSetting;
    private final String PLUGIN_NAME = "drop_events";

    @Test
    void testSingleMessageToDropProcessor() {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("dropProcessorPipeline");
        dropProcessor = new DropEventsProcessor(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        messageInput = UUID.randomUUID().toString();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        final Collection<Record<Event>> droppedRecords = dropProcessor.doExecute(Collections.singletonList(record));

        assertThat(droppedRecords.size(), equalTo(0));
    }

    @Test
    void testMultipleMessagesToDropProcessor() {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("dropProcessorPipeline");
        dropProcessor = new DropEventsProcessor(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        messageInput = UUID.randomUUID().toString();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        messageInput = UUID.randomUUID().toString();
        testData.put("message", messageInput);
        final Record<Event> record2 = buildRecordWithEvent(testData);

        final List<Record<Event>> multiSet = new LinkedList<>();
        multiSet.add(record);
        multiSet.add(record2);

        final Collection<Record<Event>> droppedRecords = dropProcessor.doExecute(multiSet);

        assertThat(droppedRecords.size(), equalTo(0));
    }

    @Test
    void testShutdownIsReady() {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("dropProcessorPipeline");
        dropProcessor = new DropEventsProcessor(pluginSetting);

        assertThat(dropProcessor.isReadyForShutdown(), is(true));
    }

    private PluginSetting getDefaultPluginSetting() {
        final Map<String, Object> settings = new HashMap<>();
        return new PluginSetting(PLUGIN_NAME, settings);
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
