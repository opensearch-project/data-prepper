/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AggregateProcessorTest {

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginMetrics pluginMetrics;

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

    private Event event;

    @BeforeEach
    void setup() {
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

        when(aggregateIdentificationKeysHasher.createIdentificationKeyHashFromEvent(event))
                .thenReturn(identificationHash);
        when(aggregateGroupManager.getAggregateGroup(identificationHash)).thenReturn(aggregateGroup);

        when(aggregateActionSynchronizerProvider.provide(aggregateAction, aggregateGroupManager)).thenReturn(aggregateActionSynchronizer);
        when(aggregateActionSynchronizer.handleEventForGroup(event, identificationHash, aggregateGroup)).thenReturn(aggregateActionResponse);
    }

    private AggregateProcessor createObjectUnderTest() {
        return new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, aggregateGroupManager, aggregateIdentificationKeysHasher, aggregateActionSynchronizerProvider);
    }

    @Test
    void handleEvent_returning_with_no_event_does_not_add_event_to_records_out() {
        final AggregateProcessor objectUnderTest = createObjectUnderTest();
        when(aggregateGroupManager.getGroupsToConclude()).thenReturn(Collections.emptyList());
        when(aggregateActionResponse.getEvent()).thenReturn(null);

        final List<Record<Event>> recordsOut = (List<Record<Event>>)objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

        assertThat(recordsOut.size(), equalTo(0));
    }

    @Test
    void handleEvent_returning_with_event_adds_event_to_records_out() {
        final AggregateProcessor objectUnderTest = createObjectUnderTest();
        when(aggregateGroupManager.getGroupsToConclude()).thenReturn(Collections.emptyList());
        when(aggregateActionResponse.getEvent()).thenReturn(event);

        final List<Record<Event>> recordsOut = (List<Record<Event>>)objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

        assertThat(recordsOut.size(), equalTo(1));
        assertThat(recordsOut.get(0), notNullValue());
        assertThat(recordsOut.get(0).getData(), equalTo(event));
    }

    @Test
    void concludeGroup_returning_with_no_event_does_not_add_event_to_records_out() {
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> groupEntry = new AbstractMap.SimpleEntry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>(identificationHash, aggregateGroup);
                when(aggregateGroupManager.getGroupsToConclude()).thenReturn(Collections.singletonList(groupEntry));
        when(aggregateActionResponse.getEvent()).thenReturn(null);
        when(aggregateActionSynchronizer.concludeGroup(identificationHash, aggregateGroup)).thenReturn(Optional.empty());

        final List<Record<Event>> recordsOut = (List<Record<Event>>)objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

        assertThat(recordsOut.size(), equalTo(0));

    }

    @Test
    void concludeGroup_returning_with_event_adds_event_to_records_out() {
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> groupEntry = new AbstractMap.SimpleEntry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>(identificationHash, aggregateGroup);
        when(aggregateGroupManager.getGroupsToConclude()).thenReturn(Collections.singletonList(groupEntry));
        when(aggregateActionResponse.getEvent()).thenReturn(null);
        when(aggregateActionSynchronizer.concludeGroup(identificationHash, aggregateGroup)).thenReturn(Optional.of(event));

        final List<Record<Event>> recordsOut = (List<Record<Event>>)objectUnderTest.doExecute(Collections.singletonList(new Record<>(event)));

        assertThat(recordsOut.size(), equalTo(1));
        assertThat(recordsOut.get(0), notNullValue());
        assertThat(recordsOut.get(0).getData(), equalTo(event));
    }
}
