package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConvertEntryTypeProcessor_NullValueTests {

    static final String TEST_KEY = UUID.randomUUID().toString();

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ConvertEntryTypeProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private ConvertEntryTypeProcessor nullValuesProcessor;

    @BeforeEach
    private void setup() {
        lenient().when(mockConfig.getKey()).thenReturn(TEST_KEY);
        lenient().when(mockConfig.getKeys()).thenReturn(null);
        lenient().when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        lenient().when(mockConfig.getConvertWhen()).thenReturn(null);
    }

    private Event executeAndGetProcessedEvent(final Object testValue) {
        final Record<Event> record = getMessage(UUID.randomUUID().toString(), TEST_KEY, testValue);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) nullValuesProcessor.doExecute(Collections.singletonList(record));
        assertThat(processedRecords.size(), equalTo(1));
        assertThat(processedRecords.get(0), notNullValue());
        Event event = processedRecords.get(0).getData();
        assertThat(event, notNullValue());
        return event;
    }

    private Record<Event> getMessage(String message, String key, Object value) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put(key, value);
        return buildRecordWithEvent(testData);
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    @Test
    void testNoNullValues() {
        int testValue = 5432;
        when(mockConfig.getNullValues()).thenReturn(Optional.empty());
        nullValuesProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, Integer.class), equalTo(testValue));
    }

    @Test
    void testEmptyListNullValues() {
        int testValue = 5432;
        when(mockConfig.getNullValues()).thenReturn(Optional.of(List.of()));
        nullValuesProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, Integer.class), equalTo(testValue));
    }

    @Test
    void testOneElementNullValues() {
        String testValue = "-";
        when(mockConfig.getNullValues()).thenReturn(Optional.of(List.of("-")));
        nullValuesProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        Object keyValue = event.get(TEST_KEY, Object.class);
        assertThat(keyValue, nullValue());
    }

    @Test
    void testMultipleElementNullValues() {
        String testValue = "-";
        when(mockConfig.getNullValues()).thenReturn(Optional.of(List.of("-", "null")));

        nullValuesProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, Integer.class), nullValue());

        testValue = "null";
        event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, Integer.class), nullValue());

        int testNumber = 5432;
        event = executeAndGetProcessedEvent(testNumber);
        assertThat(event.get(TEST_KEY, Integer.class), equalTo(testNumber));
    }

    @Test
    void testMultipleKeysNullValues() {
        String testValue = "-";
        String testKey1 = UUID.randomUUID().toString();
        String testKey2 = UUID.randomUUID().toString();
        when(mockConfig.getKey()).thenReturn(null);
        when(mockConfig.getKeys()).thenReturn(List.of(testKey1, testKey2));
        when(mockConfig.getNullValues()).thenReturn(Optional.of(List.of("-")));
        final Map<String, Object> testData = new HashMap();
        testData.put("message", "testMessage");
        testData.put(testKey1, testValue);
        testData.put(testKey2, testValue);
        Record record = buildRecordWithEvent(testData);
        nullValuesProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey1, String.class), nullValue());
        assertThat(event.get(testKey2, String.class), nullValue());
    }

}
