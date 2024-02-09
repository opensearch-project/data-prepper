/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.date;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import org.apache.commons.lang3.LocaleUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class DateProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DateProcessorConfig mockDateProcessorConfig;

    @Mock
    private DateProcessorConfig.DateMatch mockDateMatch;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private Counter dateProcessingMatchSuccessCounter;

    @Mock
    private Counter dateProcessingMatchFailureCounter;

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
        lenient().when(pluginMetrics.counter(DateProcessor.DATE_PROCESSING_MATCH_SUCCESS)).thenReturn(dateProcessingMatchSuccessCounter);
        lenient().when(pluginMetrics.counter(DateProcessor.DATE_PROCESSING_MATCH_FAILURE)).thenReturn(dateProcessingMatchFailureCounter);
        when(mockDateProcessorConfig.getDateWhen()).thenReturn(null);
        lenient().when(mockDateProcessorConfig.getOutputFormat()).thenReturn(DateProcessorConfig.DEFAULT_OUTPUT_FORMAT);
        expectedInstant = Instant.now();
        expectedDateTime = LocalDateTime.ofInstant(expectedInstant, ZoneId.systemDefault());
    }

    @AfterEach
    void cleanup() {
        verifyNoMoreInteractions(dateProcessingMatchSuccessCounter, dateProcessingMatchFailureCounter);
    }

    private DateProcessor createObjectUnderTest() {
        return new DateProcessor(pluginMetrics, mockDateProcessorConfig, expressionEvaluator);
    }

    @Test
    void from_time_received_with_default_destination_test() {
        when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(true);
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());

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
    void date_when_does_not_run_date_processor_for_event_with_date_when_as_false() {
        when(mockDateProcessorConfig.getDateWhen()).thenReturn(UUID.randomUUID().toString());
        dateProcessor = createObjectUnderTest();

        Map<String, Object> testData = getTestData();

        final Record<Event> record = new Record<>(JacksonEvent.builder()
                .withData(testData)
                .withEventType("event")
                .withTimeReceived(expectedInstant)
                .build());

        when(expressionEvaluator.evaluateConditional(mockDateProcessorConfig.getDateWhen(), record.getData())).thenReturn(false);

        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));
        assertThat(processedRecords.size(), equalTo(1));
        assertThat(processedRecords.get(0), equalTo(record));
    }

    @Test
    void from_time_received_with_custom_destination_test() {
        String destination = "new_field";
        when(mockDateProcessorConfig.getFromTimeReceived()).thenReturn(true);
        when(mockDateProcessorConfig.getDestination()).thenReturn(destination);
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());

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
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern2));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);

        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern2)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getSourceZoneId(), TIMESTAMP_KEY);
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @Test
    void match_with_custom_destination_test() {
        String destination = "new_field";
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern2));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getDestination()).thenReturn(destination);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern2)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getSourceZoneId(), destination);
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @Test
    void match_with_epoch_second_pattern() {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        String epochSecondsPattern = "epoch_second";
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(epochSecondsPattern));
        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());

        dateProcessor = createObjectUnderTest();

        LocalDate localDate = LocalDate.now(ZoneId.of("UTC"));
        testData = getTestData();
        long epochSeconds = Instant.now().getEpochSecond();
        testData.put("logDate", epochSeconds);

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));
        ZonedDateTime actualZonedDateTime =  processedRecords.get(0).getData().get(TIMESTAMP_KEY, ZonedDateTime.class);
        LocalDateTime localDateTime = localDate.atTime(LocalTime.now());
        ZonedDateTime expectedZonedDateTime = localDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneId.systemDefault()).atZone(ZoneId.systemDefault());
        actualZonedDateTime = actualZonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));

        Assertions.assertTrue(actualZonedDateTime.isEqual(expectedZonedDateTime));
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @Test
    void match_with_missing_hours_minutes_seconds_adds_zeros_test() {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern1));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());
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
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    private static Stream<Arguments> getInputOutputFormats() {
        Instant now = Instant.now();
        long epochSeconds = now.getEpochSecond();
        Random random = new Random();
        long millis = random.nextInt(1000);
        long nanos = random.nextInt(1000_000_000);
        long micros = random.nextInt(1000_000);
        long epochMillis = epochSeconds * 1000L + millis;
        long epochNanos = epochSeconds * 1000_000_000L + nanos;
        long epochMicros = epochSeconds * 1000_000L + micros;

        ZonedDateTime zdtSeconds = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), java.time.ZoneId.systemDefault());
        ZonedDateTime zdtMillis = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), java.time.ZoneId.systemDefault());
        ZonedDateTime zdtNanos = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanos), java.time.ZoneId.systemDefault());
        ZonedDateTime zdtMicros = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, micros * 1000), java.time.ZoneId.systemDefault());
        String testFormat = "yyyy-MMM-dd HH:mm:ss.SSS";
        String testNanosFormat = "yyyy-MMM-dd HH:mm:ss.nnnnnnnnnXXX";
        String defaultFormat = DateProcessorConfig.DEFAULT_OUTPUT_FORMAT;
        return Stream.of(
                arguments("epoch_second", epochSeconds, "epoch_milli", Long.toString(epochSeconds * 1000L)),
                arguments("epoch_second", epochSeconds, "epoch_nano", Long.toString(epochSeconds * 1000_000_000L)),
                arguments("epoch_second", epochSeconds, testFormat, zdtSeconds.format(DateTimeFormatter.ofPattern(testFormat))),
                arguments("epoch_second", epochSeconds, defaultFormat, zdtSeconds.format(DateTimeFormatter.ofPattern(defaultFormat))),
                arguments("epoch_milli", epochMillis, "epoch_second", Long.toString(epochSeconds)),
                arguments("epoch_milli", epochMillis, "epoch_nano", Long.toString(epochMillis * 1000_000)),
                arguments("epoch_milli", epochMillis, testFormat, zdtMillis.format(DateTimeFormatter.ofPattern(testFormat))),
                arguments("epoch_milli", epochMillis, defaultFormat, zdtMillis.format(DateTimeFormatter.ofPattern(defaultFormat))),
                arguments("epoch_nano", epochNanos, "epoch_second", Long.toString(epochSeconds)),
                arguments("epoch_nano", epochNanos, "epoch_milli", Long.toString(epochNanos/1000_000)),
                arguments("epoch_nano", epochNanos, testNanosFormat, zdtNanos.format(DateTimeFormatter.ofPattern(testNanosFormat))),
                arguments("epoch_nano", epochNanos, defaultFormat, zdtNanos.format(DateTimeFormatter.ofPattern(defaultFormat))),
                arguments("epoch_micro", epochMicros, "epoch_second", Long.toString(epochSeconds)),
                arguments("epoch_micro", epochMicros, "epoch_milli", Long.toString(epochMicros/1000)),
                arguments("epoch_micro", epochMicros, testFormat, zdtMicros.format(DateTimeFormatter.ofPattern(testFormat))),
                arguments("epoch_micro", epochMicros, defaultFormat, zdtMicros.format(DateTimeFormatter.ofPattern(defaultFormat))),
                arguments(testNanosFormat, zdtNanos.format(DateTimeFormatter.ofPattern(testNanosFormat)), "epoch_second", Long.toString(epochSeconds)),
                arguments(testNanosFormat, zdtNanos.format(DateTimeFormatter.ofPattern(testNanosFormat)), "epoch_milli", Long.toString(epochNanos/1000_000)),
                arguments(testNanosFormat, zdtNanos.format(DateTimeFormatter.ofPattern(testNanosFormat)), "epoch_nano", Long.toString(epochNanos)),
                arguments(testNanosFormat, zdtNanos.format(DateTimeFormatter.ofPattern(testNanosFormat)), "epoch_micro", Long.toString(epochNanos/1000)),
                arguments(testNanosFormat, zdtNanos.format(DateTimeFormatter.ofPattern(testNanosFormat)), defaultFormat, zdtNanos.format(DateTimeFormatter.ofPattern(defaultFormat)))
        );
    }
    
    @ParameterizedTest
    @MethodSource("getInputOutputFormats")
    void match_with_different_input_output_formats(String inputFormat, Object input, String outputFormat, Object expectedOutput) {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(inputFormat));
        when(mockDateProcessorConfig.getOutputFormat()).thenReturn(outputFormat);

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);

        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());
        if (!inputFormat.startsWith("epoch_")) {
            when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);
        }
        dateProcessor = createObjectUnderTest();
        testData = getTestData();
        testData.put("logDate", input);
        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));
        String actualOutput = processedRecords.get(0).getData().get(TIMESTAMP_KEY, String.class);
        assertThat(actualOutput, equalTo(expectedOutput));
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @Test
    void match_with_wrong_patterns_return_same_record_test_without_timestamp() {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern2));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.of("UTC"));

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("loDdate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern3)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        Assertions.assertFalse(processedRecords.get(0).getData().containsKey(TIMESTAMP_KEY));
        verify(dateProcessingMatchFailureCounter, times(1)).increment();
    }

    @ParameterizedTest
    @ValueSource(strings = { "America/New_York", "America/Los_Angeles", "Australia/Adelaide", "Japan" } )
    void match_with_custom_timezone_test(String timezone) {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern3));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.of(timezone));
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        Map<String, Object> testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern3)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getSourceZoneId(), TIMESTAMP_KEY);
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @ParameterizedTest
    @ValueSource(strings = {"en-US", "zh-CN", "it-IT"})
    void match_with_BCP47_locale_test(String locale) {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern2));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.forLanguageTag(locale));
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern2).withLocale(Locale.forLanguageTag(locale))));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getSourceZoneId(), TIMESTAMP_KEY);
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @ParameterizedTest
    @ValueSource(strings = {"en_US", "fr_FR", "ja_JP"})
    void match_with_POSIX_locale_test(String locale) {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern2));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(LocaleUtils.toLocale(locale));
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.of("UTC"));
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());

        dateProcessor = createObjectUnderTest();

        testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern2).withLocale(LocaleUtils.toLocale(locale))));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        assertTimestampsAreEqual(processedRecords.get(0), mockDateProcessorConfig.getSourceZoneId(), TIMESTAMP_KEY);
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @ParameterizedTest
    @ValueSource(strings = {"MMM/dd/uuuu", "yyyy MM dd"})
    void match_with_different_year_formats_test(String pattern) {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        Map<String, Object> testData = getTestData();
        testData.put("logDate", expectedDateTime.minus(10, ChronoUnit.YEARS).format(DateTimeFormatter.ofPattern(pattern)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        //The timezone from 'record' instance is UTC instead of local one. We need convert it to local.
        ZonedDateTime actualZonedDateTime =  record.getData().get(TIMESTAMP_KEY, ZonedDateTime.class).withZoneSameInstant(mockDateProcessorConfig.getSourceZoneId());
        ZonedDateTime expectedZonedDatetime = expectedDateTime.minus(10, ChronoUnit.YEARS).atZone(mockDateProcessorConfig.getSourceZoneId()).truncatedTo(ChronoUnit.SECONDS);

        Assertions.assertTrue(actualZonedDateTime.toLocalDate().isEqual(expectedZonedDatetime.toLocalDate()));
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @ParameterizedTest
    @ValueSource(strings = {"yyyy MM dd HH mm ss"})
    void match_with_to_origination_metadata(String pattern) {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);
        when(mockDateProcessorConfig.getToOriginationMetadata()).thenReturn(true);

        dateProcessor = createObjectUnderTest();

        Map<String, Object> testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        Event event = (Event)processedRecords.get(0).getData();
        Assertions.assertTrue(event.getMetadata().getExternalOriginationTime() != null);
        Assertions.assertTrue(event.getEventHandle().getExternalOriginationTime() != null);
        ZonedDateTime expectedZonedDatetime = expectedDateTime.atZone(mockDateProcessorConfig.getSourceZoneId()).truncatedTo(ChronoUnit.SECONDS);
        Assertions.assertTrue(expectedZonedDatetime.equals(event.getMetadata().getExternalOriginationTime().atZone(mockDateProcessorConfig.getSourceZoneId())));
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
    }

    @ParameterizedTest
    @ValueSource(strings = {"MMM/dd", "MM dd"})
    void match_without_year_test(String pattern) {
        when(mockDateMatch.getKey()).thenReturn("logDate");
        when(mockDateMatch.getPatterns()).thenReturn(Collections.singletonList(pattern));

        List<DateProcessorConfig.DateMatch> dateMatches = Collections.singletonList(mockDateMatch);
        when(mockDateProcessorConfig.getMatch()).thenReturn(dateMatches);
        when(mockDateProcessorConfig.getSourceZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getDestinationZoneId()).thenReturn(ZoneId.systemDefault());
        when(mockDateProcessorConfig.getSourceLocale()).thenReturn(Locale.ROOT);

        dateProcessor = createObjectUnderTest();

        Map<String, Object> testData = getTestData();
        testData.put("logDate", expectedDateTime.format(DateTimeFormatter.ofPattern(pattern)));

        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) dateProcessor.doExecute(Collections.singletonList(record));

        //The timezone from record is UTC instead of local one. We need convert it to local.
        ZonedDateTime actualZonedDateTime =  record.getData().get(TIMESTAMP_KEY, ZonedDateTime.class).withZoneSameInstant(mockDateProcessorConfig.getSourceZoneId());
        ZonedDateTime expectedZonedDatetime = expectedDateTime.atZone(mockDateProcessorConfig.getSourceZoneId()).truncatedTo(ChronoUnit.SECONDS);

        Assertions.assertTrue(actualZonedDateTime.toLocalDate().isEqual(expectedZonedDatetime.toLocalDate()));
        verify(dateProcessingMatchSuccessCounter, times(1)).increment();
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
