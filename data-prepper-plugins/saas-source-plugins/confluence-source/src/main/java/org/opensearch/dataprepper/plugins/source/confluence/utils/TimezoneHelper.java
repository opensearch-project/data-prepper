package org.opensearch.dataprepper.plugins.source.confluence.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimezoneHelper {

    public static int getUTCTimezoneOffset(String timezone) {
        return getTimezoneOffsetSeconds(timezone, "UTC");
    }

    public static int getTimezoneOffsetSeconds(String timezone1, String timezone2) {
        ZoneId zone1 = ZoneId.of(timezone1);
        ZoneId zone2 = ZoneId.of(timezone2);

        // Get current instant
        LocalDateTime now = LocalDateTime.now();

        // Get offsets for both zones
        ZonedDateTime zone1DateTime = now.atZone(zone1);
        ZonedDateTime zone2DateTime = now.atZone(zone2);

        // Calculate difference
        return (zone1DateTime.getOffset().getTotalSeconds() -
                zone2DateTime.getOffset().getTotalSeconds());
    }
}
