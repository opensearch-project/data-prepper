/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SplitStringProcessorTests {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SplitStringProcessorConfig config;

    private SplitStringProcessor createObjectUnderTest() {
        return new SplitStringProcessor(pluginMetrics, config);
    }

    @ParameterizedTest
    @ArgumentsSource(SplitStringArgumentsProvider.class)
    void testSingleSplitProcessor(final String message, final List<String> splitMessage) {

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", ",", null)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", ",", null)));

        final SplitStringProcessor splitStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        final List<Record<Event>> splitRecords = (List<Record<Event>>) splitStringProcessor.doExecute(Collections.singletonList(record));

        assertThat(splitRecords.get(0).getData().get("message", Object.class), notNullValue());
        assertThat(splitRecords.get(0).getData().get("message", Object.class), equalTo(splitMessage));
    }

    @Test
    public void testBothDefinedThrowsError() {
        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", ",", "a")));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", ",", "a")));

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest());
    }

    @Test
    public void testNeitherDefinedThrowsError() {
        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", null, null)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", null, null)));

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest());
    }

    @ParameterizedTest
    @ArgumentsSource(SplitStringArgumentsProvider.class)
    void testDelimiterSplitProcessor(final String message, final List<String> splitMessage) {

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", null, ",")));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", null, ",")));

        final SplitStringProcessor splitStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        final List<Record<Event>> splitRecords = (List<Record<Event>>) splitStringProcessor.doExecute(Collections.singletonList(record));

        assertThat(splitRecords.get(0).getData().get("message", Object.class), notNullValue());
        assertThat(splitRecords.get(0).getData().get("message", Object.class), equalTo(splitMessage));
    }


    private SplitStringProcessorConfig.Entry createEntry(final String source, final String delimiterRegex, final String delimiter) {
        return new SplitStringProcessorConfig.Entry(source, delimiterRegex, delimiter);
    }

    private Record<Event> createEvent(final String message) {
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message", message);
        return new Record<>(JacksonEvent.builder()
                .withEventType("event")
                .withData(eventData)
                .build());
    }

    static class SplitStringArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.arguments("hello,world,no-split", Arrays.asList("hello","world","no-split")),
                    Arguments.arguments("hello,world", Arrays.asList("hello", "world")),
                    Arguments.arguments("hello,,world", Arrays.asList("hello","","world")),
                    Arguments.arguments("hello,", Arrays.asList("hello")),
                    Arguments.arguments("hello,,", Arrays.asList("hello")),
                    Arguments.arguments(",hello", Arrays.asList("","hello")),
                    Arguments.arguments(",,hello", Arrays.asList("","","hello"))
            );
        }
    }

}