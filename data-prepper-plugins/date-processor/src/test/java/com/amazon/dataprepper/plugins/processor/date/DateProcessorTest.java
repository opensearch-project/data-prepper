/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.lenient;

@RunWith(MockitoJUnitRunner.class)
class DateProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DateProcessorConfig mockDateProcessorConfig;

    private DateProcessor dateProcessor;
    private final String messageInput =  UUID.randomUUID().toString();

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    @BeforeEach
    void setup() {
        final DateProcessorConfig defaultConfig = new DateProcessorConfig();
        lenient().when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(defaultConfig.getFromTimeReceived());
        lenient().when(mockDateProcessorConfig.getMatch()).thenReturn(defaultConfig.getMatch());
        lenient().when(mockDateProcessorConfig.getDestination()).thenReturn(defaultConfig.getDestination());
        lenient().when(mockDateProcessorConfig.getTimezone()).thenReturn(defaultConfig.getDestination());
        lenient().when(mockDateProcessorConfig.getLocale()).thenReturn(defaultConfig.getLocale());

        dateProcessor = new DateProcessor(pluginMetrics, mockDateProcessorConfig);
    }

    private DateProcessor createObjectUnderTest() {
        return new DateProcessor(pluginMetrics, mockDateProcessorConfig);
    }

    @Test
    void getFormatters_test() {
        System.out.println("test");
//        when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(false);
//        System.out.println(mockDateProcessorConfig.getFromTimeReceived());
////        dateProcessor = createObjectUnderTest();
//        System.out.println(mockDateProcessorConfig.getFromTimeReceived());
//
//
        final Map<String, Object> testData = new HashMap();
        testData.put("message", messageInput);
        System.out.println(testData);

//        dateProcessor.doExecute(Collections.singletonList(buildRecordWithEvent(testData)));
    }

}