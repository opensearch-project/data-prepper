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
    void testTruncateStringProcessor(final String message, final Integer startAt, final Integer truncateLength, final String truncatedMessage) {

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", startAt, truncateLength, null)));

        final TruncateStringProcessor truncateStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateStringProcessor.doExecute(Collections.singletonList(record));
        assertThat(truncatedRecords.get(0).getData().get("message", Object.class), notNullValue());
        assertThat(truncatedRecords.get(0).getData().get("message", Object.class), equalTo(truncatedMessage));
    }

    @Test
    public void testInputValidation() {
        assertThat(createEntry("message", null, null, null).hasStartAtOrLength(), equalTo(false));
        assertThat(createEntry("message", null, -5, null).hasStartAtOrLength(), equalTo(false));
        assertThat(createEntry("message", -5, null, null).hasStartAtOrLength(), equalTo(false));
        assertThat(createEntry("message", -5, -6, null).hasStartAtOrLength(), equalTo(false));

    }

    @Test
    void test_event_is_the_same_when_truncateWhen_condition_returns_false() {
        final String truncateWhen = UUID.randomUUID().toString();
        final String message = UUID.randomUUID().toString();

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", null, 5, truncateWhen)));

        final TruncateStringProcessor truncateStringProcessor = createObjectUnderTest();
        final Record<Event> record = createEvent(message);
        when(expressionEvaluator.evaluateConditional(truncateWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> truncatedRecords = (List<Record<Event>>) truncateStringProcessor.doExecute(Collections.singletonList(record));

        assertThat(truncatedRecords.get(0).getData().toMap(), equalTo(record.getData().toMap()));
    }


    private TruncateStringProcessorConfig.Entry createEntry(final String source, final Integer startAt, final Integer length, final String truncateWhen) {
        return new TruncateStringProcessorConfig.Entry(source, startAt, length, truncateWhen);
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
                    arguments("hello", null, 0, "")
            );
        }
    }

}

