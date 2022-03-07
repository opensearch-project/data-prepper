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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DropEventsProcessorTests {
    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;
    private String messageInput;
    private DropEventsProcessor dropProcessor;
    private PluginSetting pluginSetting;

    @Test
    void testSingleMessageToDropProcessor() {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("dropProcessorPipeline");
        dropProcessor = new DropEventsProcessor(pluginSetting, expressionEvaluator);

        final Map<String, Object> testData = new HashMap<>();
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
        dropProcessor = new DropEventsProcessor(pluginSetting, expressionEvaluator);

        final Map<String, Object> testData = new HashMap<>();
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
    void testGivenWhenSettingThenIsStatementFalseUsed() {
        final String whenSetting = UUID.randomUUID().toString();
        final String pipelineName = UUID.randomUUID().toString();
        final PluginSetting pluginSetting = mock(PluginSetting.class);
        final Event event = mock(Event.class);
        final Record<Event> record = mock(Record.class);
        final List<Record<Event>> recordsToBeProcessed = Collections.nCopies(10, record);

        doReturn(whenSetting)
                .when(pluginSetting)
                .getAttributeFromSettings(eq("when"));
        doReturn(pipelineName)
                .when(pluginSetting)
                .getPipelineName();
        doReturn(true, true, false)
                .when(expressionEvaluator)
                .evaluate(eq(whenSetting), eq(event));
        doReturn(event)
                .when(record)
                .getData();

        final DropEventsProcessor dropEventsProcessor = new DropEventsProcessor(pluginSetting, expressionEvaluator);

        final Collection<Record<Event>> results = dropEventsProcessor.doExecute(recordsToBeProcessed);

        assertThat(results.size(), is(2));
        verify(record, times(recordsToBeProcessed.size())).getData();
    }

    @Test
    void testGivenPrepareForShutdownWhenShutdownIsReadyThenNoExceptionThrown() {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("dropProcessorPipeline");
        dropProcessor = new DropEventsProcessor(pluginSetting, null);

        dropProcessor.prepareForShutdown();

        assertThat(dropProcessor.isReadyForShutdown(), is(true));
    }

    @Test
    void testGivenIsReadyForShutdownWhenShutdownThenNoExceptionThrown() {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("dropProcessorPipeline");
        dropProcessor = new DropEventsProcessor(pluginSetting, null);

        assertThat(dropProcessor.isReadyForShutdown(), is(true));
        dropProcessor.shutdown();
    }

    @Test
    void testShutdownIsReady() {
        pluginSetting = getDefaultPluginSetting();
        pluginSetting.setPipelineName("dropProcessorPipeline");
        dropProcessor = new DropEventsProcessor(pluginSetting, expressionEvaluator);

        assertThat(dropProcessor.isReadyForShutdown(), is(true));
    }

    private PluginSetting getDefaultPluginSetting() {
        final Map<String, Object> settings = new HashMap<>();
        final String PLUGIN_NAME = "drop_events";
        return new PluginSetting(PLUGIN_NAME, settings);
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
