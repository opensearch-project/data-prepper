package com.amazon.dataprepper.plugins.processor.drop;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.*;

public class DropProcessorTests {
    private String messageInput;
    private DropProcessor dropProcessor;
    private PluginSetting pluginSetting;
    private final String PLUGIN_NAME = "drop_events";

    @Test
    void doExecute() {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("dropProcessorPipeline");
        dropProcessor = new DropProcessor(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        messageInput = UUID.randomUUID().toString();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> droppedRecords = (List<Record<Event>>) dropProcessor.doExecute(Collections.singletonList(record));

        Assert.assertEquals(0, droppedRecords.size());
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
