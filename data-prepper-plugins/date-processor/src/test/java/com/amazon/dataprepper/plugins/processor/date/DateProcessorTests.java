/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.apache.commons.lang3.LocaleUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class DateProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DateProcessorConfig mockDateProcessorConfig;
    private DateProcessor dateProcessor;
    private Map<String, Object> testData;
    private LocalDateTime expectedDateTime;
    private Instant expectedInstant;

    private static final String TIMESTAMP_KEY = "@timestamp";
    private final String messageInput =  UUID.randomUUID().toString();
    private final String pattern1 = "yyyy-MM-dd";
    private final String pattern2 = "yyyy-MMM-dd HH:mm:ss.SSS";
    private final String pattern3 = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS";

    @BeforeEach
    void setup() {
        final DateProcessorConfig dateProcessorConfig = new DateProcessorConfig();
        lenient().when(mockDateProcessorConfig.getDestination()).thenReturn(dateProcessorConfig.getDestination());

        expectedDateTime = LocalDateTime.now();
    }

    private DateProcessor createObjectUnderTest() {
        return new DateProcessor(pluginMetrics, mockDateProcessorConfig);
    }

    @Test
    void from_time_received_with_default_destination_test() {
        when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(true);

        expectedInstant = Instant.now();
        dateProcessor = createObjectUnderTest();

        Map<String, Object> testData = getTestData();

        final Record<Event> record = new Record<>(JacksonEvent.builder()
                .withData(testData)
                .withEventType("event")
                .withTimeReceived(expectedInstant)
                .build());

        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        ZonedDateTime actualZonedDateTime = processedRecords.get(0).getData().get(TIMESTAMP_KEY, ZonedDateTime.class);

        Assertions.assertEquals(0, actualZonedDateTime.toInstant().compareTo(expectedInstant.truncatedTo(ChronoUnit.MILLIS)));
    }

    @Test
    void from_time_received_with_custom_destination_test() {
        String destination = "new_field";
        when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(true);
        when(mockDateProcessorConfig.getDestination()).thenReturn(destination);

        expectedInstant = Instant.now();
        dateProcessor = createObjectUnderTest();

        Map<String, Object> testData = getTestData();

        final Record<Event> record = new Record<>(JacksonEvent.builder()
                .withData(testData)
                .withEventType("event")
                .withTimeReceived(expectedInstant)
                .build());

        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        ZonedDateTime actualZonedDateTime = processedRecords.get(0).getData().get(destination, ZonedDateTime.class);

        Assertions.assertEquals(0, actualZonedDateTime.toInstant().compareTo(expectedInstant.truncatedTo(ChronoUnit.MILLIS)));
    }

    @Test
    void match_with_default_destination_test() {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logDate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getZonedId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern2)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getZonedId(), TIMESTAMP_KEY);
    }

    @Test
    void match_with_custom_destination_test() {
        String destination = "new_field";
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logDate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getDestination()).thenReturn(destination);
        when(mockDateProcessorConfig.getZonedId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern2)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getZonedId(), destination);
    }

    @Test
    void match_with_missing_hours_minutes_seconds_adds_zeros_test() {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logDate", Collections.singletonList(pattern1));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getZonedId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        LocalDate localDate = LocalDate.now(ZoneId.of("UTC"));
        testData = getTestData();
        testData.put("logDate", localDate.toString());

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        ZonedDateTime actualZonedDateTime =  processedRecords.get(0).getData().get(TIMESTAMP_KEY, ZonedDateTime.class);
        ZonedDateTime expectedZonedDateTime = localDate.atStartOfDay().atZone(ZoneId.of("UTC"));

        Assertions.assertTrue(actualZonedDateTime.isEqual(expectedZonedDateTime));
    }

    @Test
    void match_with_wrong_patterns_return_same_record_test_without_timestamp() {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logDate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);
        when(mockDateProcessorConfig.getZonedId()).thenReturn(ZoneId.of("UTC"));

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("loDdate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern3)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        Assertions.assertFalse(processedRecords.get(0).getData().containsKey(TIMESTAMP_KEY));
    }

    @ParameterizedTest
    @ValueSource(strings = { "America/New_York", "America/Los_Angeles", "Australia/Adelaide", "Japan" } )
    void match_with_custom_timezone_test(String timezone) {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.singletonList(pattern3));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getZonedId()).thenReturn(ZoneId.of(timezone));
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        Map<String, Object> testData = getTestData();
        testData.put("logdate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern3)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getZonedId(), TIMESTAMP_KEY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"en-US", "zh-CN", "it-IT"})
    void match_with_BCP47_locale_test(String locale) {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.forLanguageTag(locale));
        when(mockDateProcessorConfig.getZonedId()).thenReturn(ZoneId.of("UTC"));

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern2).withLocale(Locale.forLanguageTag(locale))));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getZonedId(), TIMESTAMP_KEY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"en_US", "fr_FR", "ja_JP"})
    void match_with_POSIX_locale_test(String locale) {
        HashMap<String, List<String>> match = new HashMap<>();
        match.put("logdate", Collections.singletonList(pattern2));
        when(mockDateProcessorConfig.getMatch()).thenReturn(match);
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(LocaleUtils.toLocale(locale));
        when(mockDateProcessorConfig.getZonedId()).thenReturn(ZoneId.of("UTC"));

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logdate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern2).withLocale(LocaleUtils.toLocale(locale))));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getZonedId(), TIMESTAMP_KEY);
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

    private void assertTimestampsAreEqual(Record<Event> record, ZoneId zoneId, String destination) {
        ZonedDateTime actualZonedDateTime =  record.getData().get(destination, ZonedDateTime.class);
        ZonedDateTime expectedZonedDatetime = expectedDateTime.atZone(zoneId).truncatedTo(ChronoUnit.MILLIS);

        Assertions.assertTrue(actualZonedDateTime.isEqual(expectedZonedDatetime));
    }

}
