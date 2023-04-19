/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3keyindex;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class responsible for creation of s3 key pattern based on date time stamp
 */
public class S3ObjectIndex {

    private static final String TIME_PATTERN_STARTING_SYMBOLS = "\\%{";

    // For matching a string that begins with a "%{" and ends with a "}".
    // For a string like "data-prepper-%{yyyy-MM-dd}", "%{yyyy-MM-dd}" is matched.
    private static final String TIME_PATTERN_REGULAR_EXPRESSION = "\\%\\{.*?\\}";

    // For matching a string enclosed by "%{" and "}".
    // For a string like "data-prepper-%{yyyy-MM}", "yyyy-MM" is matched.
    private static final String TIME_PATTERN_INTERNAL_EXTRACTOR_REGULAR_EXPRESSION = "\\%\\{(.*?)\\}";

    private static final ZoneId UTC_ZONE_ID = ZoneId.of(TimeZone.getTimeZone("UTC").getID());

    S3ObjectIndex() {
    }

    /**
     * Create Object Name with date,time and UniqueID prepended.
     */
    public static String getObjectNameWithDateTimeId(final String indexAlias) {
        DateTimeFormatter dateFormatter = getDatePatternFormatter(indexAlias);
        String suffix = (dateFormatter != null) ? dateFormatter.format(getCurrentUtcTime()) : "";
        return indexAlias.replaceAll(TIME_PATTERN_REGULAR_EXPRESSION, "") + suffix + "-" + getTimeNanos() + "-"
                + UUID.randomUUID();
    }

    /**
     * Create Object path prefix.
     */
    public static String getObjectPathPrefix(final String indexAlias) {
        DateTimeFormatter dateFormatter = getDatePatternFormatter(indexAlias);
        String suffix = (dateFormatter != null) ? dateFormatter.format(getCurrentUtcTime()) : "";
        return indexAlias.replaceAll(TIME_PATTERN_REGULAR_EXPRESSION, "") + suffix;
    }

    /**
     * Creates epoch seconds.
     */
    public static long getTimeNanos() {
        Instant time = Instant.now();
        final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
        long currentTimeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        return currentTimeNanos;
    }

    /**
     * Validate the index with the regular expression pattern. Throws exception if validation fails
     */
    public static DateTimeFormatter getDatePatternFormatter(final String indexAlias) {
        final Pattern pattern = Pattern.compile(TIME_PATTERN_INTERNAL_EXTRACTOR_REGULAR_EXPRESSION);
        final Matcher timePatternMatcher = pattern.matcher(indexAlias);
        if (timePatternMatcher.find()) {
            final String timePattern = timePatternMatcher.group(1);
            if (timePatternMatcher.find()) { // check if there is a one more match.
                throw new IllegalArgumentException("An index only allows one date-time pattern.");
            }
            if (timePattern.contains(TIME_PATTERN_STARTING_SYMBOLS)) { // check if it is a nested pattern such as
                                                                       // "data-prepper-%{%{yyyy.MM.dd}}"
                throw new IllegalArgumentException("An index doesn't allow nested date-time patterns.");
            }
            validateTimePatternIsAtTheEnd(indexAlias, timePattern);
            validateNoSpecialCharsInTimePattern(timePattern);
            validateTimePatternGranularity(timePattern);
            return DateTimeFormatter.ofPattern(timePattern);
        }
        return null;
    }

    /**
     * Data Prepper only allows time pattern as a suffix.
     */
    private static void validateTimePatternIsAtTheEnd(final String indexAlias, final String timePattern) {
        if (!indexAlias.endsWith(timePattern + "}")) {
            throw new IllegalArgumentException("Time pattern can only be a suffix of an index.");
        }
    }

    /**
     * Special characters can cause failures in creating indexes.
     */
    private static final Set<Character> INVALID_CHARS = Set.of('#', '\\', '/', '*', '?', '"', '<', '>', '|', ',', ':');

    public static void validateNoSpecialCharsInTimePattern(String timePattern) {
        boolean containsInvalidCharacter = timePattern.chars().mapToObj(c -> (char) c)
                .anyMatch(character -> INVALID_CHARS.contains(character));
        if (containsInvalidCharacter) {
            throw new IllegalArgumentException(
                    "Index time pattern contains one or multiple special characters: " + INVALID_CHARS);
        }
    }

    /**
     * Validates the time pattern, support creating indexes with time patterns that are too granular
     * hour, minute and second
     */
    private static final Set<Character> UNSUPPORTED_TIME_GRANULARITY_CHARS = Set.of('A', 'n', 'N');

    public static void validateTimePatternGranularity(String timePattern) {
        boolean containsUnsupportedTimeSymbol = timePattern.chars().mapToObj(c -> (char) c)
                .anyMatch(character -> UNSUPPORTED_TIME_GRANULARITY_CHARS.contains(character));
        if (containsUnsupportedTimeSymbol) {
            throw new IllegalArgumentException("Index time pattern contains time patterns that are less than one hour: "
                    + UNSUPPORTED_TIME_GRANULARITY_CHARS);
        }
    }

    /**
     * Returns the current UTC Date and Time
     */
    public static ZonedDateTime getCurrentUtcTime() {
        return LocalDateTime.now().atZone(ZoneId.systemDefault()).withZoneSameInstant(UTC_ZONE_ID);
    }
}