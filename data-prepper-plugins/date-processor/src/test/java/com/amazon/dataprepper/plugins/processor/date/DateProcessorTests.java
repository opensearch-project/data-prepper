/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class DateProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DateProcessorConfig mockDateProcessorConfig;

    private DateProcessor dateProcessor;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};
    private static final String TIMESTAMP_KEY = "@timestamp";

    private final String messageInput =  UUID.randomUUID().toString();
    private Map<String, Object> testData;
    private Map<String, Object> resultData;
    private final String pattern1 = "yyyy-MM-dd";
    private final String pattern2 = "yyyy-MMM-dd HH:mm:ss.SSS";
    private final String logDate1 = LocalDate.now().toString();
    private final String logDate2 = LocalDateTime.now().format(DateTimeFormatter.ofPattern(pattern2));

    @BeforeEach
    void setup() {
        final DateProcessorConfig dateProcessorConfig = new DateProcessorConfig();
        lenient().when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(dateProcessorConfig.getFromTimeReceived());
        lenient().when(mockDateProcessorConfig.getMatch()).thenReturn(dateProcessorConfig.getMatch());
        lenient().when(mockDateProcessorConfig.getDestination()).thenReturn(dateProcessorConfig.getDestination());
        lenient().when(mockDateProcessorConfig.getTimezone()).thenReturn(dateProcessorConfig.getTimezone());
        lenient().when(mockDateProcessorConfig.getLocale()).thenReturn(dateProcessorConfig.getLocale());
    }

    private DateProcessor createObjectUnderTest() {
        return new DateProcessor(pluginMetrics, mockDateProcessorConfig);
    }

    @Test
    void both_from_time_received_and_match_configured_throws_IllegalArgumentException_test() {
        when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(true);
        when(mockDateProcessorConfig.getMatch()).thenReturn(Collections.singletonMap("logtime", Collections.singletonList("yyyy-mm")));

        Exception exception = assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
        assertThat(exception.getMessage(), equalTo("from_time_received and match are mutually exclusive options."));
    }

    @Test
    void match_with_empty_map_throws_IllegalArgumentException_test() {
        HashMap<String, List<String>> match = new HashMap<>();
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);

        Exception exception = assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
        assertThat(exception.getMessage(), equalTo("match can have a minimum and maximum of 1 entry."));
    }

    @Test
    void match_with_more_than_one_key_throws_IllegalArgumentException_test() {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate1", Collections.emptyList());
        match.put("logdate2", Collections.emptyList());
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);

        Exception exception = assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
        assertThat(exception.getMessage(), equalTo("match can have a minimum and maximum of 1 entry."));
    }

    @Test
    void match_with_no_patterns_throws_IllegalArgumentException_test() {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.emptyList());
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);

        Exception exception = assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
        assertThat(exception.getMessage(), equalTo("At least 1 pattern is required."));
    }

    @Test
    void from_time_received_with_default_destination_test() throws JsonProcessingException {
        when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(true);

        dateProcessor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(getTestData());
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        resultData = getTestData();
        this.resultData.put(TIMESTAMP_KEY, ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DateProcessor.OUTPUT_FORMAT)));
        final List<Record<Event>> expectedRecords = Collections.singletonList(buildRecordWithEvent(this.resultData));

        assertThatRecordsAreEqual(processedRecords.get(0), expectedRecords.get(0), TIMESTAMP_KEY);
    }

    @Test
    void from_time_received_with_custom_destination_test() throws JsonProcessingException {
        when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(true);
        when(mockDateProcessorConfig.getDestination()).thenReturn("custom_field");

        dateProcessor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(getTestData());
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        resultData = getTestData();
        this.resultData.put("custom_field", ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DateProcessor.OUTPUT_FORMAT)));
        final List<Record<Event>> expectedRecords = Collections.singletonList(buildRecordWithEvent(this.resultData));

        assertThat(processedRecords.get(0).getData().get("custom_field", ZonedDateTime.class),
                lessThanOrEqualTo(expectedRecords.get(0).getData().get("custom_field", ZonedDateTime.class)));
        assertThatRecordsAreEqual(processedRecords.get(0), expectedRecords.get(0), "custom_field");
    }

    @Test
    void match_with_default_destination_test() throws JsonProcessingException {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", logDate2);

        Map<String, Object> resultData = getTestData();
        resultData.put("logdate", logDate2);
        resultData.put(TIMESTAMP_KEY, ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DateProcessor.OUTPUT_FORMAT)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        final List<Record<Event>> expectedRecords = Collections.singletonList(buildRecordWithEvent(resultData));

        assertThatRecordsAreEqual(processedRecords.get(0), expectedRecords.get(0), TIMESTAMP_KEY);
    }

    @Test
    void match_with_custom_destination_test() throws JsonProcessingException {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getDestination()).thenReturn("custom_field");

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", logDate2);

        Map<String, Object> resultData = getTestData();
        resultData.put("logdate", logDate2);
        resultData.put("custom_field", ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DateProcessor.OUTPUT_FORMAT)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        final List<Record<Event>> expectedRecords = Collections.singletonList(buildRecordWithEvent(resultData));

        assertThatRecordsAreEqual(processedRecords.get(0), expectedRecords.get(0), "custom_field");
    }

    @Test
    void match_with_missing_hours_minutes_seconds_test() throws JsonProcessingException {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.singletonList(pattern1));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getTimezone()).thenReturn(ZoneId.systemDefault().toString());

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", logDate1);

        Map<String, Object> resultData = getTestData();
        resultData.put("logdate", logDate1);
        resultData.put(TIMESTAMP_KEY, ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DateProcessor.OUTPUT_FORMAT)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        final List<Record<Event>> expectedRecords = Collections.singletonList(buildRecordWithEvent(resultData));

        assertThatRecordsAreEqual(processedRecords.get(0), expectedRecords.get(0), TIMESTAMP_KEY);
    }

    @Test
    void match_with_unsupported_timezone_throws_IllegalArgumentException_test() {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getTimezone()).thenReturn(UUID.randomUUID().toString());

        Exception exception = assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
        assertThat(exception.getMessage(), equalTo("Unsupported timezone provided."));
    }

    @Test
    void match_with_custom_timezone_test() throws JsonProcessingException {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Arrays.asList(pattern2, pattern1));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getTimezone()).thenReturn("America/New_York");

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", logDate2);

        Map<String, Object> resultData = getTestData();
        resultData.put("logdate", logDate2);
        resultData.put(TIMESTAMP_KEY, ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DateProcessor.OUTPUT_FORMAT)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        final List<Record<Event>> expectedRecords = Collections.singletonList(buildRecordWithEvent(resultData));

        assertThatRecordsAreEqual(processedRecords.get(0), expectedRecords.get(0), TIMESTAMP_KEY);
    }

    @Test
    void match_with_unsupported_locale_throws_IllegalArgumentException_test() {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.emptyList());
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getLocale()).thenReturn(String.valueOf(UUID.randomUUID()));

        Exception exception = assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);

         assertThat(exception.getMessage(), equalTo("Invalid locale format. Only language, country and variant are supported."));
    }

    @Test
    void match_with_BCP47_locale_test() throws JsonProcessingException {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Arrays.asList(pattern2, pattern1));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getLocale()).thenReturn("en-US");

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", logDate2);

        Map<String, Object> resultData = getTestData();
        resultData.put("logdate", logDate2);
        resultData.put(TIMESTAMP_KEY, ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DateProcessor.OUTPUT_FORMAT)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        final List<Record<Event>> expectedRecords = Collections.singletonList(buildRecordWithEvent(resultData));

        assertThatRecordsAreEqual(processedRecords.get(0), expectedRecords.get(0), TIMESTAMP_KEY);
    }

    @Test
    void match_with_POSIX_locale_test() throws JsonProcessingException {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Arrays.asList(pattern2, pattern1));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getLocale()).thenReturn("en_US");

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", logDate2);

        Map<String, Object> resultData = getTestData();
        resultData.put("logdate", logDate2);
        resultData.put(TIMESTAMP_KEY, ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DateProcessor.OUTPUT_FORMAT)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        final List<Record<Event>> expectedRecords = Collections.singletonList(buildRecordWithEvent(resultData));

        assertThatRecordsAreEqual(processedRecords.get(0), expectedRecords.get(0), TIMESTAMP_KEY);
    }

    @Test
    void match_with_wrong_patterns_return_same_record_test() {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy MM dd HH:mm.ss")));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertFalse(processedRecords.get(0).getData().containsKey(TIMESTAMP_KEY));
    }

    private void assertThatRecordsAreEqual(final Record<Event> first, final Record<Event> second, final String key) throws JsonProcessingException {
        final Map<String, Object> recordMapFirst = OBJECT_MAPPER.readValue(first.getData().toJsonString(), MAP_TYPE_REFERENCE);
        final Map<String, Object> recordMapSecond = OBJECT_MAPPER.readValue(second.getData().toJsonString(), MAP_TYPE_REFERENCE);

        ZonedDateTime firstZonedDateTime = first.getData().get(key, ZonedDateTime.class);
        ZonedDateTime secondZonedDateTime = second.getData().get(key, ZonedDateTime.class);

        recordMapFirst.remove(key);
        recordMapSecond.remove(key);

        assertThat(recordMapFirst.size(), equalTo(recordMapSecond.size()));
        assertThatTimestampsAreEqual(firstZonedDateTime, secondZonedDateTime);
        assertThat(recordMapFirst, equalTo(recordMapSecond));
    }

    private void assertThatTimestampsAreEqual(final ZonedDateTime resultTimestamp, final ZonedDateTime expectedTimestamp) {
        assertThat(resultTimestamp, lessThanOrEqualTo(expectedTimestamp));
        assertThat(resultTimestamp, greaterThanOrEqualTo(expectedTimestamp.minusDays(1)));
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private Map<String, Object> getTestData() {
        testData = new HashMap<>();
        testData.put("message", messageInput);

        return testData;
    }

}