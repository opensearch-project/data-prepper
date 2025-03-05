package org.opensearch.dataprepper.plugins.source.confluence.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.DateTimeException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimezoneHelperTest {

    private static Stream<Arguments> provideTimezoneTestCases() {
        return Stream.of(
                // zone1, zone2, expectedOffset1, expectedOffset2 (accounting for DST)
                Arguments.of("America/New_York", "America/Los_Angeles", 10800, 14400),  // 3 or 4 hours
                Arguments.of("Asia/Tokyo", "UTC", 32400, 32400),  // Always 9 hours
                Arguments.of("Europe/London", "UTC", 0, 3600),    // 0 or 1 hour depending on BST
                Arguments.of("Australia/Sydney", "Asia/Tokyo", 7200, 10800)  // 2 or 3 hours
        );
    }

    @Test
    @DisplayName("Test UTC to PST offset")
    void testUtcToPstOffset() {
        int offset = TimezoneHelper.getUTCTimezoneOffsetSeconds("America/Los_Angeles");
        // UTC to PST is either -7 or -8 hours depending on DST
        assertTrue(offset == -28800 || offset == -25200);  // -8 hours or -7 hours in seconds
    }

    @Test
    @DisplayName("Test same timezone should return zero offset")
    void testSameTimezoneOffset() {
        int offset = TimezoneHelper.getTimezoneOffsetSeconds("UTC", "UTC");
        assertEquals(0, offset);
    }

    @Test
    @DisplayName("Test invalid timezone should throw exception")
    void testInvalidTimezone() {
        assertThrows(DateTimeException.class, () ->
                TimezoneHelper.getTimezoneOffsetSeconds("Invalid/Zone", "UTC"));
    }

    @ParameterizedTest
    @MethodSource("provideTimezoneTestCases")
    @DisplayName("Test various timezone combinations")
    void testMultipleTimezones(String zone1, String zone2, int expectedOffsetRange1,
                               int expectedOffsetRange2) {
        int offset = TimezoneHelper.getTimezoneOffsetSeconds(zone1, zone2);
        assertTrue(offset == expectedOffsetRange1 || offset == expectedOffsetRange2,
                String.format("Offset %d should be either %d or %d",
                        offset, expectedOffsetRange1, expectedOffsetRange2));
    }

    @Test
    @DisplayName("Test null timezone parameters")
    void testNullTimezone() {
        assertThrows(NullPointerException.class, () ->
                TimezoneHelper.getTimezoneOffsetSeconds(null, "UTC"));
        assertThrows(NullPointerException.class, () ->
                TimezoneHelper.getTimezoneOffsetSeconds("UTC", null));
    }

    @Test
    @DisplayName("Test empty timezone parameters")
    void testEmptyTimezone() {
        assertThrows(DateTimeException.class, () ->
                TimezoneHelper.getTimezoneOffsetSeconds("", "UTC"));
        assertThrows(DateTimeException.class, () ->
                TimezoneHelper.getTimezoneOffsetSeconds("UTC", ""));
    }


    @Test
    @DisplayName("Test extreme timezone differences")
    void testExtremeTimezones() {
        int offset = TimezoneHelper.getTimezoneOffsetSeconds("Pacific/Kiritimati", "Pacific/Niue");
        // Maximum timezone difference is around 26 hours
        assertTrue(Math.abs(offset) <= 26 * 3600);
    }

}
