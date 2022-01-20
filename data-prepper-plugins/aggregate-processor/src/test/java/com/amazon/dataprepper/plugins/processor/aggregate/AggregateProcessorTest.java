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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private AggregateProcessorConfig aggregateProcessorConfig;

    @Mock
    private AggregateAction aggregateAction;

    @Mock
    private PluginModel actionConfiguration;

    @Mock
    private GroupStateManager groupStateManager;

    @Mock
    private AggregateActionResponse aggregateActionResponse;

    private AggregateProcessor aggregateProcessor;
    private Event event;

    @BeforeEach
    void setup() {
        when(aggregateProcessorConfig.getAggregateAction()).thenReturn(actionConfiguration);
        when(actionConfiguration.getPluginName()).thenReturn(UUID.randomUUID().toString());
        when(actionConfiguration.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);

        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(Collections.emptyList());

        final Map<String, Object> eventMap = new HashMap<>();
        eventMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        event = JacksonEvent.builder()
                .withData(eventMap)
                .withEventType("event")
                .build();

        final Map<Object, Object> expectedIdentificationKeyHash = new HashMap<>(eventMap);

        when(aggregateIdentificationKeysHasher.createIdentificationKeyHashFromEvent(event, Collections.emptyList()))
                .thenReturn(expectedIdentificationKeyHash);
        when(groupStateManager.getGroupState(expectedIdentificationKeyHash)).thenReturn(Collections.emptyMap());
        when(aggregateAction.handleEvent(eq(event), eq(Collections.emptyMap()))).thenReturn(aggregateActionResponse);
    }

    private AggregateProcessor createObjectUnderTest() {
        return new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, groupStateManager, aggregateIdentificationKeysHasher);
    }

    @Test
    void handleEvent_returing_with_no_event_does_not_add_event_to_records_out() {
        aggregateProcessor = createObjectUnderTest();
        when(aggregateActionResponse.getEvent()).thenReturn(null);

        final List<Record<Event>> recordsOut = (List<Record<Event>>)aggregateProcessor.doExecute(Collections.singletonList(new Record<>(event)));

        assertThat(recordsOut.size(), equalTo(0));
    }

    @Test
    void handleEvent_returning_with_event_adds_event_to_records_out() {
        aggregateProcessor = createObjectUnderTest();
        when(aggregateActionResponse.getEvent()).thenReturn(event);

        final List<Record<Event>> recordsOut = (List<Record<Event>>)aggregateProcessor.doExecute(Collections.singletonList(new Record<>(event)));

        assertThat(recordsOut.size(), equalTo(1));
        assertThat(recordsOut.get(0), notNullValue());
        assertThat(recordsOut.get(0).getData(), equalTo(event));
    }
}
