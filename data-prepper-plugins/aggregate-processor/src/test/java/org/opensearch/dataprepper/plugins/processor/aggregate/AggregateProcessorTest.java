/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.record.Record;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private AggregateIdentificationKeysHasher.IdentificationHash identificationHash;

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

    private Event event;

    private AggregateProcessor createObjectUnderTest() {
        return new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, aggregateGroupManager, aggregateIdentificationKeysHasher, aggregateActionSynchronizerProvider);
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
            when(aggregateIdentificationKeysHasher.createIdentificationKeyHashFromEvent(event))
                    .thenReturn(identificationHash);
            when(aggregateGroupManager.getAggregateGroup(identificationHash)).thenReturn(aggregateGroup);
            when(aggregateActionSynchronizer.handleEventForGroup(event, identificationHash, aggregateGroup)).thenReturn(aggregateActionResponse);
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

            final Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> groupEntry = new AbstractMap.SimpleEntry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>(identificationHash, aggregateGroup);
            when(aggregateGroupManager.getGroupsToConclude(eq(false))).thenReturn(Collections.singletonList(groupEntry));
            when(aggregateActionResponse.getEvent()).thenReturn(null);
            when(aggregateActionSynchronizer.concludeGroup(identificationHash, aggregateGroup, false)).thenReturn(Optional.empty());

            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

            assertThat(recordsOut.size(), equalTo(0));

            verify(actionConcludeGroupEventsDroppedCounter).increment();
            verify(actionHandleEventsDroppedCounter).increment(1);
            verify(actionHandleEventsOutCounter).increment(0);
            verifyNoInteractions(actionConcludeGroupEventsOutCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(false));
            verify(aggregateActionSynchronizer).concludeGroup(identificationHash, aggregateGroup, false);
        }

        @Test
        void concludeGroup_returning_with_event_adds_event_to_records_out() {
            final AggregateProcessor objectUnderTest = createObjectUnderTest();

            final Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> groupEntry = new AbstractMap.SimpleEntry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>(identificationHash, aggregateGroup);
            when(aggregateGroupManager.getGroupsToConclude(eq(false))).thenReturn(Collections.singletonList(groupEntry));
            when(aggregateActionResponse.getEvent()).thenReturn(null);
            when(aggregateActionSynchronizer.concludeGroup(identificationHash, aggregateGroup, false)).thenReturn(Optional.of(event));

            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

            assertThat(recordsOut.size(), equalTo(1));
            assertThat(recordsOut.get(0), notNullValue());
            assertThat(recordsOut.get(0).getData(), equalTo(event));

            verify(actionConcludeGroupEventsOutCounter).increment();
            verify(actionHandleEventsDroppedCounter).increment(1);
            verify(actionHandleEventsOutCounter).increment(0);
            verifyNoInteractions(actionConcludeGroupEventsDroppedCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(false));
            verify(aggregateActionSynchronizer).concludeGroup(identificationHash, aggregateGroup, false);
        }

        @Test
        void concludeGroup_after_prepare_for_shutdown() {
            final AggregateProcessor objectUnderTest = createObjectUnderTest();
            objectUnderTest.prepareForShutdown();

            final Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> groupEntry = new AbstractMap.SimpleEntry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>(identificationHash, aggregateGroup);
            when(aggregateGroupManager.getGroupsToConclude(eq(true))).thenReturn(Collections.singletonList(groupEntry));
            when(aggregateActionResponse.getEvent()).thenReturn(null);
            when(aggregateActionSynchronizer.concludeGroup(identificationHash, aggregateGroup, true)).thenReturn(Optional.of(event));

            final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

            assertThat(recordsOut.size(), equalTo(1));
            assertThat(recordsOut.get(0), notNullValue());
            assertThat(recordsOut.get(0).getData(), equalTo(event));

            verify(actionConcludeGroupEventsOutCounter).increment();
            verify(actionHandleEventsDroppedCounter).increment(1);
            verify(actionHandleEventsOutCounter).increment(0);
            verifyNoInteractions(actionConcludeGroupEventsDroppedCounter);

            verify(aggregateGroupManager).getGroupsToConclude(eq(true));
            verify(aggregateActionSynchronizer).concludeGroup(identificationHash, aggregateGroup, true);
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
