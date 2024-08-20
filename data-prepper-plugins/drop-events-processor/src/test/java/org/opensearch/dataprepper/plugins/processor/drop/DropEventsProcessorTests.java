/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DropEventsProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private DropEventProcessorConfig dropEventProcessorConfig;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    private String whenSetting;
    private String messageInput;
    private DropEventsProcessor dropProcessor;

    @BeforeEach
    void beforeEach() {
        whenSetting = UUID.randomUUID().toString();
        when(expressionEvaluator.isValidExpressionStatement(anyString())).thenReturn(true);
        lenient().doReturn(HandleFailedEventsOption.SKIP)
                .when(dropEventProcessorConfig)
                .getHandleFailedEventsOption();
    }

    @Test
    void invalid_drop_when_throws_InvalidPluginConfigurationException() {

        final String dropWhen = UUID.randomUUID().toString();

        when(dropEventProcessorConfig.getDropWhen()).thenReturn(dropWhen);
        when(expressionEvaluator.isValidExpressionStatement(dropWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, () -> new DropEventsProcessor(pluginMetrics, dropEventProcessorConfig, expressionEvaluator));
    }

    @Test
    void testSingleMessageToDropProcessor() {
        doReturn("true")
                .when(dropEventProcessorConfig)
                .getDropWhen();
        dropProcessor = new DropEventsProcessor(pluginMetrics, dropEventProcessorConfig, expressionEvaluator);

        final Map<String, Object> testData = new HashMap<>();
        messageInput = UUID.randomUUID().toString();
        testData.put("message", messageInput);
        final Record<Event> record = buildRecordWithEvent(testData);

        final Collection<Record<Event>> droppedRecords = dropProcessor.doExecute(Collections.singletonList(record));

        assertThat(droppedRecords.size(), equalTo(0));
    }

    @Test
    void testMultipleMessagesToDropProcessor() {
        doReturn("true")
                .when(dropEventProcessorConfig)
                .getDropWhen();
        dropProcessor = new DropEventsProcessor(pluginMetrics, dropEventProcessorConfig, expressionEvaluator);

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
        final Event event = mock(Event.class);
        final Record<Event> record = mock(Record.class);
        final int inputRecordCount = 10;
        final List<Record<Event>> recordsToBeProcessed = Collections.nCopies(inputRecordCount, record);
        final int numberOfMockedFalseEvaluations = 8;
        final boolean repeatedReturnValue = false;

        doReturn(whenSetting)
                .when(dropEventProcessorConfig)
                .getDropWhen();
        doReturn(
                true,
                true,
                repeatedReturnValue
        ).when(expressionEvaluator)
                .evaluateConditional(eq(whenSetting), eq(event));
        doReturn(event)
                .when(record)
                .getData();

        dropProcessor = new DropEventsProcessor(pluginMetrics, dropEventProcessorConfig, expressionEvaluator);

        final Collection<Record<Event>> results = dropProcessor.doExecute(recordsToBeProcessed);

        assertThat(results.size(), is(numberOfMockedFalseEvaluations));
        verify(record, times(inputRecordCount)).getData();
    }

    @Test
    void testGivenPrepareForShutdownWhenShutdownIsReadyThenNoExceptionThrown() {
        doReturn("true")
                .when(dropEventProcessorConfig)
                .getDropWhen();
        dropProcessor = new DropEventsProcessor(pluginMetrics, dropEventProcessorConfig, expressionEvaluator);

        dropProcessor.prepareForShutdown();

        assertThat(dropProcessor.isReadyForShutdown(), is(true));
    }

    @Test
    void testGivenIsReadyForShutdownWhenShutdownThenNoExceptionThrown() {
        doReturn("true")
                .when(dropEventProcessorConfig)
                .getDropWhen();
        dropProcessor = new DropEventsProcessor(pluginMetrics, dropEventProcessorConfig, expressionEvaluator);

        assertThat(dropProcessor.isReadyForShutdown(), is(true));
        dropProcessor.shutdown();
    }

    @Test
    void testShutdownIsReady() {
        doReturn("true")
                .when(dropEventProcessorConfig)
                .getDropWhen();
        dropProcessor = new DropEventsProcessor(pluginMetrics, dropEventProcessorConfig, expressionEvaluator);

        assertThat(dropProcessor.isReadyForShutdown(), is(true));
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
