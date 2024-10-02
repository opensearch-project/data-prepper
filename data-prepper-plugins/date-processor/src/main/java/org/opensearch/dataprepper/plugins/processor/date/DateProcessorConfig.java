/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.date;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.AssertTrue;

import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.time.format.DateTimeFormatter;

@JsonPropertyOrder
@JsonClassDescription("The `date` processor adds a default timestamp to an event, parses timestamp fields, " +
        "and converts timestamp information to the International Organization for Standardization (ISO) 8601 format. " +
        "This timestamp information can be used as an event timestamp.")
public class DateProcessorConfig {
    static final Boolean DEFAULT_FROM_TIME_RECEIVED = false;
    static final Boolean DEFAULT_TO_ORIGINATION_METADATA = false;
    static final String DEFAULT_DESTINATION = "@timestamp";
    static final String DEFAULT_OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    static final String DEFAULT_SOURCE_TIMEZONE = ZoneId.systemDefault().toString();
    static final String DEFAULT_DESTINATION_TIMEZONE = ZoneId.systemDefault().toString();

    public static class DateMatch {
        @JsonProperty("key")
        @JsonPropertyDescription("Represents the event key against which to match patterns. " +
                "Required if <code>match</code> is configured.")
        private String key;
        @JsonProperty("patterns")
        @JsonPropertyDescription("A list of possible patterns that the timestamp value of the key can have. The patterns " +
                "are based on a sequence of letters and symbols. The <code>patterns</code> support all the patterns listed in the " +
                "Java DateTimeFormatter (https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) reference. " +
                "The timestamp value also supports <code>epoch_second</code>, <code>epoch_milli</code>, and <code>epoch_nano</code> values, " +
                "which represent the timestamp as the number of seconds, milliseconds, and nanoseconds since the epoch. " +
                "Epoch values always use the UTC time zone.")
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
        @AssertTrue
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
                pattern.equals("epoch_micro") ||
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
    @JsonPropertyDescription("When <code>true</code>, the timestamp from the event metadata, " +
            "which is the time at which the source receives the event, is added to the event data. " +
            "This option cannot be defined at the same time as <code>match</code>. Default is <code>false</code>.")
    private Boolean fromTimeReceived = DEFAULT_FROM_TIME_RECEIVED;

    @JsonProperty("to_origination_metadata")
    @JsonPropertyDescription("When <code>true</code>, the matched time is also added to the event's metadata as an instance of " +
            "<code>Instant</code>. Default is <code>false</code>.")
    private Boolean toOriginationMetadata = DEFAULT_TO_ORIGINATION_METADATA;

    @JsonProperty("match")
    @JsonPropertyDescription("The date match configuration. " +
            "This option cannot be defined at the same time as <code>from_time_received</code>. There is no default value.")
    private List<DateMatch> match;

    @JsonProperty("destination")
    @JsonPropertyDescription("The field used to store the timestamp parsed by the date processor. " +
            "Can be used with both <code>match</code> and <code>from_time_received</code>. Default is <code>@timestamp</code>.")
    private String destination = DEFAULT_DESTINATION;

    @JsonProperty("output_format")
    @JsonPropertyDescription("Determines the format of the timestamp added to an event. " +
            "Default is <code>yyyy-MM-dd'T'HH:mm:ss.SSSXXX</code>.")
    private String outputFormat = DEFAULT_OUTPUT_FORMAT;

    @JsonProperty("source_timezone")
    @JsonPropertyDescription("The time zone used to parse dates, including when the zone or offset cannot be extracted " +
            "from the value. If the zone or offset are part of the value, then the time zone is ignored. " +
            "A list of all the available time zones is contained in the TZ database name column of " +
            "(https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List).")
    private String sourceTimezone = DEFAULT_SOURCE_TIMEZONE;

    @JsonProperty("destination_timezone")
    @JsonPropertyDescription("The time zone used for storing the timestamp in the <code>destination</code> field. " +
            "A list of all the available time zones is contained in the TZ database name column of " +
            "(https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List).")
    private String destinationTimezone = DEFAULT_DESTINATION_TIMEZONE;

    @JsonProperty("locale")
    @JsonPropertyDescription("The location used for parsing dates. Commonly used for parsing month names (<code>MMM</code>). " +
            "The value can contain language, country, or variant fields in IETF BCP 47, such as <code>en-US</code>, " +
            "or a string representation of the " +
            "locale (https://docs.oracle.com/javase/8/docs/api/java/util/Locale.html) object, such as <code>en_US</code>. " +
            "A full list of locale fields, including language, country, and variant, can be found in " +
            "(https://www.iana.org/assignments/language-subtag-registry/language-subtag-registry). " +
            "Default is <code>Locale.ROOT</code>.")
    private String locale;

    @JsonProperty("date_when")
    @JsonPropertyDescription("Specifies under what condition the <code>date</code> processor should perform matching. " +
            "Default is no condition.")
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
