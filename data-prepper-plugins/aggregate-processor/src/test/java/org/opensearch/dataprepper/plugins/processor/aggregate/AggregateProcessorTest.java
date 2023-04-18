/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AggregateProcessorTest {

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher;

    @Mock
    private AggregateIdentificationKeysHasher.IdentificationKeysMap identificationKeysMap;

    @Mock
    private AggregateProcessorConfig aggregateProcessorConfig;

    @Mock
    private AggregateAction aggregateAction;

    @Mock
    private PluginModel actionConfiguration;

    @Mock
    private AggregateGroupManager aggregateGroupManager;

    @Mock
    private AggregateActionSynchronizer.AggregateActionSynchronizerProvider aggregateActionSynchronizerProvider;

    @Mock
    private AggregateActionSynchronizer aggregateActionSynchronizer;

    @Mock
    private AggregateGroup aggregateGroup;

    @Mock
    private AggregateActionResponse aggregateActionResponse;

    @Mock
    private AggregateActionResponse firstAggregateActionResponse;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter actionHandleEventsOutCounter;

    @Mock
    private Counter actionHandleEventsDroppedCounter;

    @Mock
    private Counter actionConcludeGroupEventsOutCounter;

    @Mock
    private Counter actionConcludeGroupEventsDroppedCounter;

    @Mock
    private Counter recordsIn;

    @Mock
    private Counter recordsOut;

    @Mock
    private Timer timeElapsed;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    private Event event;

    private AggregateProcessor createObjectUnderTest() {
        return new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, aggregateGroupManager, aggregateIdentificationKeysHasher, aggregateActionSynchronizerProvider, expressionEvaluator);
    }

    @BeforeEach
    void setUp() {
        when(aggregateProcessorConfig.getAggregateAction()).thenReturn(actionConfiguration);
        when(actionConfiguration.getPluginName()).thenReturn(UUID.randomUUID().toString());
        when(actionConfiguration.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);

        final Map<String, Object> eventMap = new HashMap<>();
        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        event = JacksonEvent.builder()
                .withData(eventMap)
                .withEventType("event")
                .build();

        when(aggregateActionSynchronizerProvider.provide(aggregateAction, aggregateGroupManager, pluginMetrics)).thenReturn(aggregateActionSynchronizer);

        when(pluginMetrics.counter(AggregateProcessor.ACTION_HANDLE_EVENTS_OUT)).thenReturn(actionHandleEventsOutCounter);
        when(pluginMetrics.counter(AggregateProcessor.ACTION_HANDLE_EVENTS_DROPPED)).thenReturn(actionHandleEventsDroppedCounter);
        when(pluginMetrics.counter(AggregateProcessor.ACTION_CONCLUDE_GROUP_EVENTS_OUT)).thenReturn(actionConcludeGroupEventsOutCounter);
        when(pluginMetrics.counter(AggregateProcessor.ACTION_CONCLUDE_GROUP_EVENTS_DROPPED)).thenReturn(actionConcludeGroupEventsDroppedCounter);

        when(pluginMetrics.counter(MetricNames.RECORDS_IN)).thenReturn(recordsIn);
        when(pluginMetrics.counter(MetricNames.RECORDS_OUT)).thenReturn(recordsOut);
        when(pluginMetrics.timer(MetricNames.TIME_ELAPSED)).thenReturn(timeElapsed);
    }

    @Test
    void getIdentificationKeys_should_return_configured_identification_keys() {
        final List<String> keys = List.of("key");
        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(keys);
        final AggregateProcessor objectUnderTest = createObjectUnderTest();
        final Collection<String> expectedIdentificationKeys = objectUnderTest.getIdentificationKeys();

        assertThat(expectedIdentificationKeys, equalTo(keys));
    }

    @Nested
    class TestDoExecute {
        @BeforeEach
        void setup() {
            when(aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(event))
                    .thenReturn(identificationKeysMap);
            when(aggregateGroupManager.getAggregateGroup(identificationKeysMap)).thenReturn(aggregateGroup);
            when(aggregateActionSynchronizer.handleEventForGroup(event, identificationKeysMap, aggregateGroup)).thenReturn(aggregateActionResponse);
        }

        @Test
        void handleEvent_returning_with_no_event_does_not_add_event_to_records_out() {
            final AggregateProcessor objectUnderTest = createObjectUnderTest();
            when(aggregateGroupManager.getGroupsToConclude(eq(false))).thenReturn(Collections.emptyList());
            when(aggregateActionResponse.getEvent()).thenReturn(null);

            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

            assertThat(recordsOut.size(), equalTo(0));

            verify(actionHandleEventsDroppedCounter).increment(1);
            verify(actionHandleEventsOutCounter).increment(0);
            verifyNoInteractions(actionConcludeGroupEventsDroppedCounter);
            verifyNoInteractions(actionConcludeGroupEventsOutCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(false));
        }

        @Test
        void handleEvent_returning_with_condition_eliminates_one_record() {
            final String eventKey = UUID.randomUUID().toString();
            final String key1 = UUID.randomUUID().toString();
            final String key2 = UUID.randomUUID().toString();
            final String condition = "/" + eventKey + " == "+key1;
            Event firstEvent;
            Event secondEvent;
            final Map<String, Object> eventMap1 = new HashMap<>();
            eventMap1.put(eventKey, key1);

            firstEvent = JacksonEvent.builder()
                .withData(eventMap1)
                .withEventType("event")
                .build();

            final Map<String, Object> eventMap2 = new HashMap<>();
            eventMap2.put(eventKey, key2);

            secondEvent = JacksonEvent.builder()
                .withData(eventMap2)
                .withEventType("event")
                .build();


            when(aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(firstEvent))
                    .thenReturn(identificationKeysMap);
            when(aggregateActionSynchronizer.handleEventForGroup(firstEvent, identificationKeysMap, aggregateGroup)).thenReturn(firstAggregateActionResponse);
            when(expressionEvaluator.evaluate(condition, event)).thenReturn(true);
            when(expressionEvaluator.evaluate(condition, firstEvent)).thenReturn(true);
            when(expressionEvaluator.evaluate(condition, secondEvent)).thenReturn(false);
            when(aggregateProcessorConfig.getWhenCondition()).thenReturn(condition);
            final AggregateProcessor objectUnderTest = createObjectUnderTest();
            when(aggregateGroupManager.getGroupsToConclude(eq(false))).thenReturn(Collections.emptyList());
            when(aggregateActionResponse.getEvent()).thenReturn(event);
            when(firstAggregateActionResponse.getEvent()).thenReturn(firstEvent);

            event.toMap().put(eventKey, key1);
            List<Record<Event>> recordsIn = new ArrayList<>();
            recordsIn.add(new Record<Event>(firstEvent));
            recordsIn.add(new Record<Event>(secondEvent));
            recordsIn.add(new Record<Event>(event));
            Collection<Record<Event>> c = recordsIn;
            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(c);

            assertThat(recordsOut.size(), equalTo(2));
            assertThat(recordsOut.get(0), notNullValue());
            assertThat(recordsOut.get(0).getData(), equalTo(firstEvent));
            assertThat(recordsOut.get(1), notNullValue());
            assertThat(recordsOut.get(1).getData(), equalTo(event));

            verify(actionHandleEventsDroppedCounter).increment(1);
            verify(actionHandleEventsOutCounter).increment(2);
            verifyNoInteractions(actionConcludeGroupEventsDroppedCounter);
            verifyNoInteractions(actionConcludeGroupEventsOutCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(false));
        }

        @Test
        void handleEvent_returning_with_event_adds_event_to_records_out() {
            final AggregateProcessor objectUnderTest = createObjectUnderTest();
            when(aggregateGroupManager.getGroupsToConclude(eq(false))).thenReturn(Collections.emptyList());
            when(aggregateActionResponse.getEvent()).thenReturn(event);

            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

            assertThat(recordsOut.size(), equalTo(1));
            assertThat(recordsOut.get(0), notNullValue());
            assertThat(recordsOut.get(0).getData(), equalTo(event));

            verify(actionHandleEventsOutCounter).increment(1);
            verify(actionHandleEventsDroppedCounter).increment(0);
            verifyNoInteractions(actionConcludeGroupEventsDroppedCounter);
            verifyNoInteractions(actionConcludeGroupEventsOutCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(false));
        }

        @Test
        void concludeGroup_returning_with_no_event_does_not_add_event_to_records_out() {
            final AggregateProcessor objectUnderTest = createObjectUnderTest();

            final Map.Entry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> groupEntry = new AbstractMap.SimpleEntry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>(identificationKeysMap, aggregateGroup);
            when(aggregateGroupManager.getGroupsToConclude(eq(false))).thenReturn(Collections.singletonList(groupEntry));
            when(aggregateActionResponse.getEvent()).thenReturn(null);
            when(aggregateActionSynchronizer.concludeGroup(identificationKeysMap, aggregateGroup, false)).thenReturn(new AggregateActionOutput(List.of()));

            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

            assertThat(recordsOut.size(), equalTo(0));

            verify(actionConcludeGroupEventsDroppedCounter).increment();
            verify(actionHandleEventsDroppedCounter).increment(1);
            verify(actionHandleEventsOutCounter).increment(0);
            verifyNoInteractions(actionConcludeGroupEventsOutCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(false));
            verify(aggregateActionSynchronizer).concludeGroup(identificationKeysMap, aggregateGroup, false);
        }

        @Test
        void concludeGroup_returning_with_event_adds_event_to_records_out() {
            final AggregateProcessor objectUnderTest = createObjectUnderTest();

            final Map.Entry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> groupEntry = new AbstractMap.SimpleEntry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>(identificationKeysMap, aggregateGroup);
            when(aggregateGroupManager.getGroupsToConclude(eq(false))).thenReturn(Collections.singletonList(groupEntry));
            when(aggregateActionResponse.getEvent()).thenReturn(null);
            when(aggregateActionSynchronizer.concludeGroup(identificationKeysMap, aggregateGroup, false)).thenReturn(new AggregateActionOutput(List.of(event)));

            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

            assertThat(recordsOut.size(), equalTo(1));
            assertThat(recordsOut.get(0), notNullValue());
            assertThat(recordsOut.get(0).getData(), equalTo(event));

            verify(actionConcludeGroupEventsOutCounter).increment();
            verify(actionHandleEventsDroppedCounter).increment(1);
            verify(actionHandleEventsOutCounter).increment(0);
            verifyNoInteractions(actionConcludeGroupEventsDroppedCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(false));
            verify(aggregateActionSynchronizer).concludeGroup(identificationKeysMap, aggregateGroup, false);
        }

        @Test
        void concludeGroup_after_prepare_for_shutdown() {
            final AggregateProcessor objectUnderTest = createObjectUnderTest();
            objectUnderTest.prepareForShutdown();

            final Map.Entry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> groupEntry = new AbstractMap.SimpleEntry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>(identificationKeysMap, aggregateGroup);
            when(aggregateGroupManager.getGroupsToConclude(eq(true))).thenReturn(Collections.singletonList(groupEntry));
            when(aggregateActionResponse.getEvent()).thenReturn(null);
            when(aggregateActionSynchronizer.concludeGroup(identificationKeysMap, aggregateGroup, true)).thenReturn(new AggregateActionOutput(List.of(event)));

            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

            assertThat(recordsOut.size(), equalTo(1));
            assertThat(recordsOut.get(0), notNullValue());
            assertThat(recordsOut.get(0).getData(), equalTo(event));

            verify(actionConcludeGroupEventsOutCounter).increment();
            verify(actionHandleEventsDroppedCounter).increment(1);
            verify(actionHandleEventsOutCounter).increment(0);
            verifyNoInteractions(actionConcludeGroupEventsDroppedCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(true));
            verify(aggregateActionSynchronizer).concludeGroup(identificationKeysMap, aggregateGroup, true);
        }
    }

    @ParameterizedTest
    @MethodSource("isReadyForShutdownArgs")
    public void isReadyForShutdown(final long groupsSize, final boolean expectedResult) {
        when(aggregateGroupManager.getAllGroupsSize()).thenReturn(groupsSize);

        final AggregateProcessor objectUnderTest = createObjectUnderTest();
        final boolean result = objectUnderTest.isReadyForShutdown();
        assertThat(result, equalTo(expectedResult));

        verify(aggregateGroupManager).getAllGroupsSize();
    }

    private static Stream<Arguments> isReadyForShutdownArgs() {
        return Stream.of(
                Arguments.of(0, true),
                Arguments.of(1, false),
                Arguments.of(Math.abs(new Random().nextLong()) + 1L, false),
                Arguments.of((Math.abs(new Random().nextInt()) * -1L) - 1L, false)
        );
    }
}
