package org.opensearch.dataprepper.plugins.source.confluence.utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimezoneHelper {

    public static Duration getUTCTimezoneOffset(ZoneId timezone) {
        return getTimezoneOffset(timezone, ZoneId.of("UTC"));
    }

    public static Duration getTimezoneOffset(ZoneId timezone1, ZoneId timezone2) {
        // Get current instant
        LocalDateTime now = LocalDateTime.now();

        // Get offsets for both zones
        ZonedDateTime zone1DateTime = now.atZone(timezone1);
        ZonedDateTime zone2DateTime = now.atZone(timezone2);

        // Calculate difference
        return Duration.between(zone1DateTime, zone2DateTime);
    }
}
