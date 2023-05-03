/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
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
import java.util.UUID;
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

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    private SplitStringProcessor createObjectUnderTest() {
        return new SplitStringProcessor(pluginMetrics, config, expressionEvaluator);
    }

    @ParameterizedTest
    @ArgumentsSource(SplitStringArgumentsProvider.class)
    void testSingleSplitProcessor(final String message, final List<String> splitMessage) {

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", ",", null, null)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", ",", null, null)));

        final SplitStringProcessor splitStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        final List<Record<Event>> splitRecords = (List<Record<Event>>) splitStringProcessor.doExecute(Collections.singletonList(record));

        assertThat(splitRecords.get(0).getData().get("message", Object.class), notNullValue());
        assertThat(splitRecords.get(0).getData().get("message", Object.class), equalTo(splitMessage));
    }

    @Test
    public void testBothDefinedThrowsError() {
        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", ",", "a", null)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", ",", "a", null)));

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest());
    }

    @Test
    public void testNeitherDefinedThrowsError() {
        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", null, null, null)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", null, null, null)));

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest());
    }

    @ParameterizedTest
    @ArgumentsSource(SplitStringDelimiterArgumentsProvider.class)
    void testDelimiterSplitProcessor(final String message, final List<String> splitMessage) {

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", null, "?", null)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", null, "?", null)));

        final SplitStringProcessor splitStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        final List<Record<Event>> splitRecords = (List<Record<Event>>) splitStringProcessor.doExecute(Collections.singletonList(record));

        assertThat(splitRecords.get(0).getData().get("message", Object.class), notNullValue());
        assertThat(splitRecords.get(0).getData().get("message", Object.class), equalTo(splitMessage));
    }

    @Test
    void test_event_is_the_same_when_splitWhen_condition_returns_false() {

        final String splitWhen = UUID.randomUUID().toString();
        final String message = UUID.randomUUID().toString();

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", ",", null, splitWhen)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", ",", null, splitWhen)));

        final SplitStringProcessor splitStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        when(expressionEvaluator.evaluate(splitWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> splitRecords = (List<Record<Event>>) splitStringProcessor.doExecute(Collections.singletonList(record));

        assertThat(splitRecords.get(0).getData().toMap(), equalTo(record.getData().toMap()));
    }


    private SplitStringProcessorConfig.Entry createEntry(final String source, final String delimiterRegex, final String delimiter, final String splitWhen) {
        return new SplitStringProcessorConfig.Entry(source, delimiterRegex, delimiter, splitWhen);
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

    static class SplitStringDelimiterArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.arguments("hello?world?no-split", Arrays.asList("hello","world","no-split")),
                    Arguments.arguments("hello?world", Arrays.asList("hello", "world")),
                    Arguments.arguments("hello??world", Arrays.asList("hello","","world")),
                    Arguments.arguments("hello?", Arrays.asList("hello")),
                    Arguments.arguments("hello??", Arrays.asList("hello")),
                    Arguments.arguments("?hello", Arrays.asList("","hello")),
                    Arguments.arguments("??hello", Arrays.asList("","","hello"))
            );
        }
    }

}