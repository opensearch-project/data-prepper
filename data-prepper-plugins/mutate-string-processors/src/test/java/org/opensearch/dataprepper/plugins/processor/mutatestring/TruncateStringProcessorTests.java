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
class TruncateStringProcessorTests {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private TruncateStringProcessorConfig config;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private TruncateStringProcessor createObjectUnderTest() {
        return new TruncateStringProcessor(pluginMetrics, config, expressionEvaluator);
    }

    @ParameterizedTest
    @ArgumentsSource(TruncateStringArgumentsProvider.class)
    void testTruncateStringProcessor(final String message, final int truncateLength, final String truncatedMessage) {

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", truncateLength, null)));

        final TruncateStringProcessor truncateStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateStringProcessor.doExecute(Collections.singletonList(record));
        assertThat(truncatedRecords.get(0).getData().get("message", Object.class), notNullValue());
        assertThat(truncatedRecords.get(0).getData().get("message", Object.class), equalTo(truncatedMessage));
    }

    public void testLengthNotDefinedThrowsError() {
        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", null, null)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", null, null)));

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest());
    }

    @Test
    void test_event_is_the_same_when_truncateWhen_condition_returns_false() {
        final String truncateWhen = UUID.randomUUID().toString();
        final String message = UUID.randomUUID().toString();

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", 5, truncateWhen)));

        final TruncateStringProcessor truncateStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        when(expressionEvaluator.evaluateConditional(truncateWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateStringProcessor.doExecute(Collections.singletonList(record));

        assertThat(truncatedRecords.get(0).getData().toMap(), equalTo(record.getData().toMap()));
    }


    private TruncateStringProcessorConfig.Entry createEntry(final String source, final Integer length, final String truncateWhen) {
        return new TruncateStringProcessorConfig.Entry(source, length, truncateWhen);
    }

    private Record<Event> createEvent(final String message) {
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message", message);
        return new Record<>(JacksonEvent.builder()
                .withEventType("event")
                .withData(eventData)
                .build());
    }

    static class TruncateStringArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.arguments("hello,world,no-truncate", 100, "hello,world,no-truncate"),
                    Arguments.arguments("hello,world,truncate", 11, "hello,world"),
                    Arguments.arguments("hello,world", 1, "h"),
                    Arguments.arguments("hello", 0, "")
            );
        }
    }

}

