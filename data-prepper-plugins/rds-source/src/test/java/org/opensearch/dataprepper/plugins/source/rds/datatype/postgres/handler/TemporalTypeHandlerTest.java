package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TemporalTypeHandlerTest {

    private TemporalTypeHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TemporalTypeHandler();
    }

    private static long getEpochMillisFromDate(final int year, final int month, final int day) {
        return LocalDate.of(year, month, day)
                .atStartOfDay(ZoneOffset.UTC)  // Ensure UTC
                .toInstant()
                .toEpochMilli();
    }

    private static long getEpochMillis(int year, int month, int day, int hour, int minute, int second, int nanoSeconds) {
        return LocalDateTime.of(year, month, day, hour, minute, second, nanoSeconds)
                .atZone(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
    }

    @Test
    void handle_whenValueIsNull_returnsNull() {
        assertNull(handler.process(PostgresDataType.DATE, "test_column", null));
    }

    @ParameterizedTest
    @MethodSource("provideDateTestCases")
    void handle_withDateType_returnsCorrectEpochMillis(String input, Long expected) {
        Object result = handler.process(PostgresDataType.DATE, "date_column", input);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideDateTestCases() {
        return Stream.of(
                Arguments.of("2023-12-25", getEpochMillisFromDate(2023, 12, 25)),
                Arguments.of("-infinity", getEpochMillisFromDate(1970, 1, 1)),
                Arguments.of("2024-02-29", getEpochMillisFromDate(2024, 2, 29)), // Leap year
                Arguments.of("infinity", getEpochMillisFromDate(9999, 12, 31))
        );
    }

    @ParameterizedTest
    @MethodSource("provideTimeWithoutTimeZoneTestCases")
    void handle_withTimeWithoutTimeZoneType_returnsCorrectEpochMillis(String input, long expected) {
        Object result = handler.process(PostgresDataType.TIME, "time_column", input);
        assertEquals(result, expected);
    }

    private static Stream<Arguments> provideTimeWithoutTimeZoneTestCases() {
        return Stream.of(
                Arguments.of("14:30:00",
                        getEpochMillis(1970, 1, 1, 14, 30, 0, 0)),
                Arguments.of("00:00:00",
                        getEpochMillis(1970, 1, 1, 0, 0, 0, 0)),
                Arguments.of("23:59:59",
                        getEpochMillis(1970, 1, 1, 23, 59, 59, 0)),
                Arguments.of("23:59:59.123456",
                        getEpochMillis(1970, 1, 1, 23, 59, 59, 123456000)),
                Arguments.of("23:59:59.123",
                        getEpochMillis(1970, 1, 1, 23, 59, 59, 123000000))
        );
    }

    @ParameterizedTest
    @MethodSource("provideTimeStampWithoutTimeZoneTestCases")
    void handle_withTimeStampWithoutTimeZoneType_returnsCorrectEpochMillis(String input, long expected) {
        Object result = handler.process(PostgresDataType.TIMESTAMP, "timestamp_column", input);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideTimeStampWithoutTimeZoneTestCases() {
        return Stream.of(
                Arguments.of("2023-12-25 14:30:00.123456", getEpochMillis(2023, 12, 25, 14, 30, 0, 123456000)),
                Arguments.of("2023-12-25 14:30:00.123", getEpochMillis(2023, 12, 25, 14, 30, 0, 123000000)),
                Arguments.of("1970-01-01 00:00:00", getEpochMillis(1970, 1, 1, 0, 0, 0, 0)),
                Arguments.of("-infinity", getEpochMillis(1970, 1, 1, 0, 0, 0, 0)),
                Arguments.of("infinity", getEpochMillis(9999, 12, 31, 23, 59, 59, 0))
        );
    }

    @ParameterizedTest
    @MethodSource("provideTimeWithTimeZoneTestCases")
    void handle_withTimeWithTimeZoneType_returnsCorrectEpochMillis(String input, long expected) {
        Object result = handler.process(PostgresDataType.TIMETZ, "timetz_column", input);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideTimeWithTimeZoneTestCases() {
        return Stream.of(
                Arguments.of("14:30:00+01", getEpochMillis(1970, 1, 1, 13, 30, 0, 0)),
                Arguments.of("23:59:59.999999+05:30", getEpochMillis(1970, 1, 1, 18, 29, 59, 999999000)),
                Arguments.of("00:00:00-08:00", getEpochMillis(1970, 1, 1, 8, 0, 0, 0)),
                Arguments.of("12:34:56.789+01:00", getEpochMillis(1970, 1, 1, 11, 34, 56, 789000000))
        );
    }

    @ParameterizedTest
    @MethodSource("provideTimeStampWithTimeZoneTestCases")
    void handle_withTimeStampWithTimeZoneType_returnsCorrectEpochMillis(String input, long expected) {
        Object result = handler.process(PostgresDataType.TIMESTAMPTZ, "timestamptz_column", input);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideTimeStampWithTimeZoneTestCases() {
        return Stream.of(
                Arguments.of("2023-12-25 14:30:00+00:00", getEpochMillis(2023, 12, 25, 14, 30, 0, 0)),
                Arguments.of("2023-12-25 23:59:59.999999+05:30", getEpochMillis(2023, 12, 25, 18, 29, 59, 999999000)),
                Arguments.of("1970-01-01 00:00:00-08:00", getEpochMillis(1970, 1, 1, 8, 0, 0, 0)),
                Arguments.of("2024-02-29 12:34:56.789+01:00", getEpochMillis(2024, 2, 29, 11, 34, 56, 789000000)),
                Arguments.of("-infinity", getEpochMillis(1970, 1, 1, 0, 0, 0, 0)),
                Arguments.of("infinity", getEpochMillis(9999, 12, 31, 23, 59, 59, 0))
        );
    }


    @ParameterizedTest
    @MethodSource("provideIntervalTestCases")
    void handle_withInterval_returnsCorrectISO8601Format(String input, String expected) {
        Object result = handler.process(PostgresDataType.INTERVAL, "interval_column", input);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideIntervalTestCases() {
        return Stream.of(
                Arguments.of("1 year 2 mons 3 days 04:05:06", "P1Y2M3DT4H5M6S"),
                Arguments.of("3 days 04:05:06", "P3DT4H5M6S" ),
                Arguments.of("1 year", "P1Y")
        );
    }

    @Test
    void handle_withInvalidFormat_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
                handler.process(PostgresDataType.DATE, "date_column", "invalid-date"));
    }

    @Test
    void handle_withUnsupportedType_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
                handler.process(PostgresDataType.VARCHAR, "varchar_column", "2023-12-25"));
    }

    @Test
    void handle_withEmptyString_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
                handler.process(PostgresDataType.DATE, "date_column", ""));
    }
}
