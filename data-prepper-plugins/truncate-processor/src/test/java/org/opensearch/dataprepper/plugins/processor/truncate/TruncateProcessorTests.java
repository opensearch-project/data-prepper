/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.truncate;

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
import static org.junit.jupiter.params.provider.Arguments.arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TruncateProcessorTests {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private TruncateProcessorConfig config;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private TruncateProcessor createObjectUnderTest() {
        return new TruncateProcessor(pluginMetrics, config, expressionEvaluator);
    }

    @ParameterizedTest
    @ArgumentsSource(TruncateArgumentsProvider.class)
    void testTruncateProcessor(final Object messageValue, final Integer startAt, final Integer truncateLength, final Object truncatedMessage) {

        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry(List.of("message"), startAt, truncateLength, null, false)));
        final TruncateProcessor truncateProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent("message", messageValue);
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateProcessor.doExecute(Collections.singletonList(record));
        assertThat(truncatedRecords.get(0).getData().get("message", Object.class), notNullValue());
        assertThat(truncatedRecords.get(0).getData().get("message", Object.class), equalTo(truncatedMessage));
    }

    @ParameterizedTest
    @ArgumentsSource(MultipleTruncateArgumentsProvider.class)
    void testTruncateProcessorMultipleEntries(final Object messageValue, final Integer startAt1, final Integer truncateLength1, final Integer startAt2, final Integer truncateLength2, final Object truncatedMessage1, final Object truncatedMessage2) {
        TruncateProcessorConfig.Entry entry1 = createEntry(List.of("message1"), startAt1, truncateLength1, null, false);
        TruncateProcessorConfig.Entry entry2 = createEntry(List.of("message2"), startAt2, truncateLength2, null, false);
        when(config.getEntries()).thenReturn(List.of(entry1, entry2));
        final Record<Event> record1 = createEvent("message1", messageValue);
        final Record<Event> record2 = createEvent("message2", messageValue);
        final TruncateProcessor truncateProcessor = createObjectUnderTest();
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateProcessor.doExecute(List.of(record1, record2));
        assertThat(truncatedRecords.get(0).getData().get("message1", Object.class), notNullValue());
        assertThat(truncatedRecords.get(1).getData().get("message2", Object.class), notNullValue());
        assertThat(truncatedRecords.get(0).getData().get("message1", Object.class), equalTo(truncatedMessage1));
        assertThat(truncatedRecords.get(1).getData().get("message2", Object.class), equalTo(truncatedMessage2));
    }

    @Test
    void test_event_is_the_same_when_truncateWhen_condition_returns_false() {
        final String truncateWhen = UUID.randomUUID().toString();
        final String message = UUID.randomUUID().toString();

        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry(List.of("message"), null, 5, truncateWhen, false)));

        final TruncateProcessor truncateProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent("message", message);
        when(expressionEvaluator.evaluateConditional(truncateWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateProcessor.doExecute(Collections.singletonList(record));

        assertThat(truncatedRecords.get(0).getData().toMap(), equalTo(record.getData().toMap()));
    }

    @Test
    void test_event_with_all_fields_truncated() {
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry(List.of("*"), null, 5, null, false)));
        final TruncateProcessor truncateProcessor = createObjectUnderTest();
        final Record<Event> record = createEventWithMultipleKeys(Map.of("key1", "aaaaa12345", "key2", "bbbbb12345", "key3", "ccccccc12345"));
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateProcessor.doExecute(Collections.singletonList(record));
        Event event = truncatedRecords.get(0).getData();
        assertThat(event.get("key1", String.class), equalTo("aaaaa"));
        assertThat(event.get("key2", String.class), equalTo("bbbbb"));
        assertThat(event.get("key3", String.class), equalTo("ccccc"));
    }

    @Test
    void test_event_with_all_fields_truncated_recursively() {
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry(List.of("*"), null, 5, null, true)));
        final TruncateProcessor truncateProcessor = createObjectUnderTest();
        final Record<Event> record = createEventWithMultipleKeys(ImmutableMap.of("key1", "aaaaa12345", "key2", ImmutableMap.of("key3", "bbbbb12345", "key4", ImmutableMap.of("key5", "ccccccc12345"))));
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateProcessor.doExecute(Collections.singletonList(record));
        Event event = truncatedRecords.get(0).getData();
        assertThat(event.get("key1", String.class), equalTo("aaaaa"));
        assertThat(event.get("key2/key3", String.class), equalTo("bbbbb"));
        assertThat(event.get("key2/key4/key5", String.class), equalTo("ccccc"));
    }

    private TruncateProcessorConfig.Entry createEntry(final List<String> sourceKeys, final Integer startAt, final Integer length, final String truncateWhen, final boolean recurse) {
        return new TruncateProcessorConfig.Entry(sourceKeys, startAt, length, truncateWhen, recurse);
    }

    private Record<Event> createEvent(final String key, final Object value) {
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put(key, value);
        return new Record<>(JacksonEvent.builder()
                .withEventType("event")
                .withData(eventData)
                .build());
    }

    private Record<Event> createEventWithMultipleKeys(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withEventType("event")
                .withData(data)
                .build());
    }

    static class TruncateArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    arguments("hello,world,no-truncate", 0, 100, "hello,world,no-truncate"),
                    arguments("hello,world,no-truncate", 6, 100, "world,no-truncate"),
                    arguments("hello,world,no-truncate", 6, 16, "world,no-truncat"),
                    arguments("hello,world,no-truncate", 6, 17, "world,no-truncate"),
                    arguments("hello,world,no-truncate", 6, 18, "world,no-truncate"),
                    arguments("hello,world,no-truncate", 6, 5, "world"),
                    arguments("hello,world,no-truncate", 6, null, "world,no-truncate"),

                    arguments("hello,world,no-truncate", null, 100, "hello,world,no-truncate"),
                    arguments("hello,world,truncate", null, 11, "hello,world"),
                    arguments("hello,world", null, 1, "h"),
                    arguments("hello", null, 0, ""),
                    arguments(List.of("hello_one", "hello_two", "hello_three"), null, 5, List.of("hello", "hello", "hello")),
                    arguments(List.of("hello_one", 2, "hello_three"), null, 5, List.of("hello", 2, "hello"))
            );
        }
    }

    static class MultipleTruncateArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    arguments("hello,world,no-truncate", 0, 100, 1, 10, "hello,world,no-truncate", "ello,world"),
                    arguments("hello,world,no-truncate", 6, 100, null, 5, "world,no-truncate", "hello"),
                    arguments("hello,world,no-truncate", 6, 16, 12, null, "world,no-truncat", "no-truncate")
            );
        }
    }

}

