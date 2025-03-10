package org.opensearch.dataprepper.plugins.source.confluence.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TimezoneHelperTest {

    static Stream<Arguments> timezoneOffsetTestCases() {
        return Stream.of(
                // Format: sourceZone, targetZone, expectedDuration

                // Same timezone (should return zero)
                arguments(ZoneId.of("Europe/Paris"), ZoneId.of("Europe/Paris"), Duration.ofHours(0), Duration.ofHours(0)),

                // Sydney to London (Sydney is ahead, so positive offset)
                arguments(ZoneId.of("Australia/Sydney"), ZoneId.of("Europe/London"), Duration.ofHours(11), Duration.ofHours(11)),

                // New York to Tokyo (New York is behind, so negative offset)
                arguments(ZoneId.of("America/New_York"), ZoneId.of("Asia/Tokyo"), Duration.ofHours(-13), Duration.ofHours(-14)),

                // Los Angeles to Berlin
                arguments(ZoneId.of("America/Los_Angeles"), ZoneId.of("Europe/Berlin"), Duration.ofHours(-8), Duration.ofHours(-9)),

                // Auckland to Hawaii (crossing international date line)
                arguments(ZoneId.of("Pacific/Auckland"), ZoneId.of("Pacific/Honolulu"), Duration.ofHours(23), Duration.ofHours(23))
        );
    }

    @ParameterizedTest
    @MethodSource("timezoneOffsetTestCases")
    void testGetTimezoneOffset(ZoneId sourceZone, ZoneId targetZone, Duration minDuration, Duration maxDuration) {
        Duration reverseOffset = TimezoneHelper.getTimezoneOffset(sourceZone, targetZone);
        // The Range check is because of the Day light saving time changes. Otherwise, this would have been equals check
        assertAll(
                () -> assertTrue(reverseOffset.compareTo(maxDuration) >= 0, "Timezone Offset should be at least " + maxDuration),
                () -> assertTrue(reverseOffset.compareTo(minDuration) <= 0, "Timezone Offset should be at most " + minDuration)
        );
    }


    @Test
    public void testEdgeCases() {
        // Test with extreme timezone differences
        Duration samoaToHonoluluOffset = TimezoneHelper.getTimezoneOffset(
                ZoneId.of("Pacific/Apia"),     // UTC+13/+14
                ZoneId.of("Pacific/Honolulu")  // UTC-10
        );
        // The difference should be around 23-24 hours
        assertTrue(Math.abs(samoaToHonoluluOffset.toHours()) >= 23,
                "Samoa to Honolulu offset should be at least 23 hours");
    }

    @Test
    public void testNullTimezone() {
        // Test with null timezone (should throw NullPointerException)
        assertThrows(NullPointerException.class, () -> TimezoneHelper.getUTCTimezoneOffset(null));
        assertThrows(NullPointerException.class, () -> TimezoneHelper.getTimezoneOffset(null, ZoneId.of("UTC")));
        assertThrows(NullPointerException.class, () -> TimezoneHelper.getTimezoneOffset(ZoneId.of("UTC"), null));
    }

    @Test
    public void testConsistencyWithCurrentTime() {
        // This test verifies that the offset calculation is consistent with the current time
        ZoneId zone1 = ZoneId.of("Europe/Berlin");
        ZoneId zone2 = ZoneId.of("America/Los_Angeles");

        // Calculate offset using our helper
        Duration calculatedOffset = TimezoneHelper.getTimezoneOffset(zone1, zone2);

        // Calculate offset manually for verification
        LocalDateTime now = LocalDateTime.now();
        ZonedDateTime berlin = now.atZone(zone1);
        ZonedDateTime la = now.atZone(zone2);
        Duration expectedOffset = Duration.between(berlin, la);

        assertEquals(expectedOffset.getSeconds(), calculatedOffset.getSeconds(),
                "Calculated offset should match expected offset");
    }
}
