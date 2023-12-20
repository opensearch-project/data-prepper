/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.date;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.time.format.DateTimeFormatter;

public class DateProcessorConfig {
    static final Boolean DEFAULT_FROM_TIME_RECEIVED = false;
    static final Boolean DEFAULT_TO_ORIGINATION_METADATA = false;
    static final String DEFAULT_DESTINATION = "@timestamp";
    static final String DEFAULT_OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    static final String DEFAULT_SOURCE_TIMEZONE = ZoneId.systemDefault().toString();
    static final String DEFAULT_DESTINATION_TIMEZONE = ZoneId.systemDefault().toString();

    public static class DateMatch {
        @JsonProperty("key")
        private String key;
        @JsonProperty("patterns")
        private List<String> patterns;

        public DateMatch() {
        }

        public DateMatch(String key, List<String> patterns) {
            this.key = key;
            this.patterns = patterns;
        }

        public String getKey() {
            return key;
        }

        public List<String> getPatterns() {
            return patterns;
        }

        @JsonIgnore
        public boolean isValidPatterns() {
            // For now, allow only one of the three "epoch_" pattern
            int count = 0;
            for (final String pattern: patterns) {
                if (pattern.startsWith("epoch_")) {
                    count++;
                }
                if (count > 1) {
                    return false;
                }
            }
            for (final String pattern: patterns) {
                if (!isValidPattern(pattern)) {
                    return false;
                }
            }
            return true;
        }

        public static boolean isValidPattern(final String pattern) {
            if (pattern.equals("epoch_second") ||
                pattern.equals("epoch_milli") ||
                pattern.equals("epoch_nano")) {
                    return true;
            }
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
                return true;
            } catch (Exception e) {
                return false;
            }
        }

    }

    @JsonProperty("from_time_received")
    private Boolean fromTimeReceived = DEFAULT_FROM_TIME_RECEIVED;

    @JsonProperty("to_origination_metadata")
    private Boolean toOriginationMetadata = DEFAULT_TO_ORIGINATION_METADATA;

    @JsonProperty("match")
    private List<DateMatch> match;

    @JsonProperty("destination")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("output_format")
    private String outputFormat = DEFAULT_OUTPUT_FORMAT;

    @JsonProperty("source_timezone")
    private String sourceTimezone = DEFAULT_SOURCE_TIMEZONE;

    @JsonProperty("destination_timezone")
    private String destinationTimezone = DEFAULT_DESTINATION_TIMEZONE;

    @JsonProperty("locale")
    private String locale;

    @JsonProperty("date_when")
    private String dateWhen;

    @JsonIgnore
    private ZoneId sourceZoneId;

    @JsonIgnore
    private ZoneId destinationZoneId;

    @JsonIgnore
    private Locale sourceLocale;

    public String getOutputFormat() {
        return outputFormat;
    }

    public Boolean getFromTimeReceived() {
        return fromTimeReceived;
    }

    public Boolean getToOriginationMetadata() {
        return toOriginationMetadata;
    }

    public List<DateMatch> getMatch() {
        return match;
    }

    public String getDestination() {
        return destination;
    }

    public ZoneId getSourceZoneId() {
        return sourceZoneId;
    }

    public ZoneId getDestinationZoneId() {
        return destinationZoneId;
    }

    public Locale getSourceLocale() {
        return sourceLocale;
    }

    public String getDateWhen() { return dateWhen; }

    private ZoneId buildZoneId(final String timezone) {
        try {
            return ZoneId.of(timezone);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid timezone.");
        }
    }

    private Locale buildLocale(final String locale) {
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

    @AssertTrue(message = "match and from_time_received are mutually exclusive options. match or from_time_received is required.")
    boolean isValidMatchAndFromTimestampReceived() {
        return Boolean.TRUE.equals(fromTimeReceived) ^ match != null;
    }

    @AssertTrue(message = "match can have a minimum and maximum of 1 entry and at least one pattern.")
    boolean isValidMatch() {
        if (match != null) {
            if (match.size() != 1)
                return false;

            return match.get(0).getPatterns() != null && !match.get(0).getPatterns().isEmpty() && match.get(0).isValidPatterns();
        }
        return true;
    }

    @AssertTrue(message = "Invalid output format.")
    boolean isValidOutputFormat() {
        return DateMatch.isValidPattern(outputFormat);
    }

    @AssertTrue(message = "Invalid source_timezone provided.")
    boolean isSourceTimezoneValid() {
        try {
            sourceZoneId  = buildZoneId(sourceTimezone);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @AssertTrue(message = "Invalid destination_timezone provided.")
    boolean isDestinationTimezoneValid() {
        try {
            destinationZoneId = buildZoneId(destinationTimezone);
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
