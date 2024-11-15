package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNull;

class TemporalTypeHandlerTest {
    private TemporalTypeHandler temporalTypeHandler;

    @BeforeEach
    void setUp() {
        temporalTypeHandler = new TemporalTypeHandler();
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
        assertNull(temporalTypeHandler.handle(MySQLDataType.DATE, "test_column", null, null));
    }

    @ParameterizedTest
    @MethodSource("provideDateTestCases")
    void handle_withDateType_returnsCorrectEpochMillis(String input, Long expected) {
        Long result = temporalTypeHandler.handle(MySQLDataType.DATE, "date_column", input, null);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideDateTestCases() {
        return Stream.of(
                Arguments.of("2023-12-25", getEpochMillisFromDate(2023, 12, 25)),
                Arguments.of("1970-01-01", getEpochMillisFromDate(1970, 1, 1)),
                Arguments.of("2024-02-29", getEpochMillisFromDate(2024, 2, 29)), // Leap year
                Arguments.of("0000-00-00", null),
                Arguments.of("1684108800000", getEpochMillisFromDate(2023, 5, 15))
        );
    }

    @ParameterizedTest
    @MethodSource("provideTimeTestCases")
    void handle_withTimeType_returnsCorrectEpochMillis(String input, long expected) {
        Long result = temporalTypeHandler.handle(MySQLDataType.TIME, "time_column", input, null);
        assertEquals(result, expected);
    }

    private static Stream<Arguments> provideTimeTestCases() {
        return Stream.of(
                Arguments.of("14:30:00",
                        getEpochMillis(1970, 1, 1, 14, 30, 0, 0)),
                Arguments.of("00:00:00",
                        getEpochMillis(1970, 1, 1, 0, 0, 0, 0)),
                Arguments.of("23:59:59",
                        getEpochMillis(1970, 1, 1, 23, 59, 59, 0)),
                Arguments.of("85647000",
                        getEpochMillis(1970, 1, 1, 23, 47, 27, 0)),
                Arguments.of("52200000",
                        getEpochMillis(1970, 1, 1, 14, 30, 0, 0)),
                Arguments.of("52200123",
                        getEpochMillis(1970, 1, 1, 14, 30, 0, 123456000)),
                Arguments.of("16:30:00.000000",
                        getEpochMillis(1970, 1, 1, 16, 30, 0, 0)),
                Arguments.of("07:17:00.456789",
                        getEpochMillis(1970, 1, 1, 7, 17, 0, 456789000))
        );
    }

    @ParameterizedTest
    @MethodSource("provideDateTimeTestCases")
    void handle_withDateTimeType_returnsCorrectEpochMillis(String input, long expected) {
        Long result = temporalTypeHandler.handle(MySQLDataType.DATETIME, "datetime_column", input, null);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideDateTimeTestCases() {
        return Stream.of(
            Arguments.of("2023-12-25 14:30:00.123456", getEpochMillis(2023, 12, 25, 14, 30, 0, 123456000)),
            Arguments.of("1970-01-01 00:00:00", getEpochMillis(1970, 1, 1, 0, 0, 0, 0)),
            Arguments.of("1703509900000", 1703509900000L),
            Arguments.of("1784161123456789", 1784161123456L)
        );
    }

    @ParameterizedTest
    @MethodSource("provideTimestampTestCases")
    void handle_withTimestampType_returnsCorrectEpochMillis(String input, long expected) {
        Long result = temporalTypeHandler.handle(MySQLDataType.TIMESTAMP, "timestamp_column", input, null);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideTimestampTestCases() {
        return Stream.of(
                Arguments.of("1703509800000", 1703509800000L),
                Arguments.of("2023-12-25 14:30:00", getEpochMillis(2023, 12, 25, 14, 30, 0, 0))
        );
    }

    @ParameterizedTest
    @MethodSource("provideYearTestCases")
    void handle_withYearType_returnsCorrectEpochMillis(String input, long expected) {
        Long result = temporalTypeHandler.handle(MySQLDataType.YEAR, "year_column", input, null);
        assertEquals(expected, result);
    }

    private static Stream<Arguments> provideYearTestCases() {
        return Stream.of(
                Arguments.of("2023", 2023),
                Arguments.of("1900", 0),
                Arguments.of("1997", 1997),
                Arguments.of("1889", 0),
                Arguments.of("1901", 1901),
                Arguments.of("2155", 2155),
                Arguments.of("2156", 0),
                Arguments.of("3015", 0)
        );
    }

    @Test
    void handle_withInvalidFormat_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
                temporalTypeHandler.handle(MySQLDataType.DATE, "date_column", "invalid-date", null));
    }

    @Test
    void handle_withUnsupportedType_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
                temporalTypeHandler.handle(MySQLDataType.VARCHAR, "varchar_column", "2023-12-25", null));
    }

    @Test
    void handle_withEmptyString_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
                temporalTypeHandler.handle(MySQLDataType.DATE, "date_column", "", null));
    }

    @Test
    void handle_withWhitespaceString_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () ->
                temporalTypeHandler.handle(MySQLDataType.DATE, "date_column", "   ", null));
    }

    @Test
    void handle_withLeapYearDate_returnsCorrectEpochMillis() {
        Long result = temporalTypeHandler.handle(MySQLDataType.DATE, "date_column", "2024-02-29", null);
        assertEquals(getEpochMillisFromDate(2024, 2, 29), result);
    }

    @Test
    void handle_withMaxDateTime_returnsCorrectEpochMillis() {
        Long result = temporalTypeHandler.handle(MySQLDataType.DATETIME, "datetime_column",
                "9999-12-31 23:59:59", null);
        assertEquals(getEpochMillis(9999, 12, 31, 23, 59, 59, 0), result);
    }

    @Test
    void handle_withMinDateTime_returnsCorrectEpochMillis() {
        Long result = temporalTypeHandler.handle(MySQLDataType.DATETIME, "datetime_column",
                "1000-01-01 00:00:00", null);
        assertEquals(getEpochMillis(1000, 1, 1, 0, 0, 0, 0), result);
    }
}

