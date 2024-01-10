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

        when(config.getSourceKeys()).thenReturn(List.of("message"));
        when(config.getStartAt()).thenReturn(startAt);
        when(config.getLength()).thenReturn(truncateLength);
        when(config.getTruncateWhen()).thenReturn(null);

        final TruncateProcessor truncateProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(messageValue);
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateProcessor.doExecute(Collections.singletonList(record));
        assertThat(truncatedRecords.get(0).getData().get("message", Object.class), notNullValue());
        assertThat(truncatedRecords.get(0).getData().get("message", Object.class), equalTo(truncatedMessage));
    }

    @Test
    void test_event_is_the_same_when_truncateWhen_condition_returns_false() {
        final String truncateWhen = UUID.randomUUID().toString();
        final String message = UUID.randomUUID().toString();

        when(config.getSourceKeys()).thenReturn(List.of("message"));
        when(config.getStartAt()).thenReturn(null);
        when(config.getLength()).thenReturn(5);
        when(config.getTruncateWhen()).thenReturn(truncateWhen);

        final TruncateProcessor truncateProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        when(expressionEvaluator.evaluateConditional(truncateWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateProcessor.doExecute(Collections.singletonList(record));

        assertThat(truncatedRecords.get(0).getData().toMap(), equalTo(record.getData().toMap()));
    }


    private Record<Event> createEvent(final Object message) {
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message", message);
        return new Record<>(JacksonEvent.builder()
                .withEventType("event")
                .withData(eventData)
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

}

