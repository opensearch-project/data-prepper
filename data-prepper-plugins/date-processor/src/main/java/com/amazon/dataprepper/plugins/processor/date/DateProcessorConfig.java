/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DateProcessorConfig {
    static final Boolean DEFAULT_FROM_TIME_RECEIVED = false;
    static final String DEFAULT_DESTINATION = "@timestamp";
    static final String DEFAULT_TIMEZONE = "UTC";

    @JsonProperty("from_time_received")
    private Boolean fromTimeReceived = DEFAULT_FROM_TIME_RECEIVED;

    @JsonProperty("match")
    private Map<String, List<String>> match;

    @JsonProperty("destination")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("timezone")
    private String timezone = DEFAULT_TIMEZONE;

    @JsonProperty("locale")
    private String locale;

    @JsonIgnore
    private ZoneId zoneId;

    @JsonIgnore
    private Locale sourceLocale;

    public Boolean getFromTimeReceived() {
        return fromTimeReceived;
    }

    public Map<String, List<String>> getMatch() {
        return match;
    }

    public String getDestination() {
        return destination;
    }

    public ZoneId getZonedId() {
        return zoneId;
    }

    public Locale getSourceLocale() {
        return sourceLocale;
    }

    private ZoneId buildZoneId(String timezone) {
        try {
            return ZoneId.of(timezone);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid timezone.");
        }
    }

    private Locale buildLocale(String locale) {
        Locale currentLocale;
        if (locale == null || locale.equalsIgnoreCase("ROOT")) {
            return Locale.ROOT;
        }

        boolean isBCP47Format = locale.contains("-");

        final String[] localeFields;
        if (isBCP47Format) {
            localeFields = locale.split("-");
        }
        else
            localeFields = locale.split("_");

        switch (localeFields.length) {
            case 1:
                currentLocale = new Locale(localeFields[0]);
                break;
            case 2:
                currentLocale = new Locale(localeFields[0], localeFields[1]);
                break;
            case 3:
                currentLocale = new Locale(localeFields[0], localeFields[1], localeFields[2]);
                break;
            default:
                throw new IllegalArgumentException("Invalid locale format. Only language, country and variant are supported.");
        }

        if (currentLocale.getISO3Language() != null && currentLocale.getISO3Country() != null)
            return currentLocale;
        else
            throw new IllegalArgumentException("Unknown locale provided.");
    }

    @AssertTrue(message = "match and from_time_received are mutually exclusive options.")
    boolean isValidMatchAndFromTimestampReceived() {
        return !Boolean.TRUE.equals(fromTimeReceived) || match == null;
    }

    @AssertTrue(message = "match can have a minimum and maximum of 1 entry and at least one pattern.")
    boolean isValidMatch() {
        if (match != null) {
            if (match.size() != 1)
                return false;

            Map.Entry<String, List<String>> firstEntry = match.entrySet().iterator().next();
            return firstEntry.getValue() != null && !firstEntry.getValue().isEmpty();
        }
        return true;
    }

    @AssertTrue(message = "Invalid timezone provided.")
    boolean isTimezoneValid() {
        try {
            zoneId = buildZoneId(timezone);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @AssertTrue(message = "Invalid locale provided.")
    boolean isLocaleValid() {
        try {
            sourceLocale = buildLocale(locale);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
