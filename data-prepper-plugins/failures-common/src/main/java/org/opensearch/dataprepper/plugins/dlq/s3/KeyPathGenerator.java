package org.opensearch.dataprepper.plugins.dlq.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KeyPathGenerator {

    private static final String TIME_PATTERN_INTERNAL_EXTRACTOR_REGULAR_EXPRESSION  = "%\\{(.*?)\\}";
    private static final String TIME_PATTERN_STARTING_SYMBOLS = "%{";
    private static final String TIME_PATTERN_REGULAR_EXPRESSION  = "%\\{.*?\\}";
    static final ZoneId UTC_ZONE_ID = ZoneId.of(TimeZone.getTimeZone("UTC").getID());
    private static final Set<Character> INVALID_CHARS = ImmutableSet.of('#', '\\', '/', '*', '?', '"', '<', '>', '|', ',', ':');
    private static final Set<Character> UNSUPPORTED_TIME_GRANULARITY_CHARS = ImmutableSet.of('m', 's', 'S', 'A', 'n', 'N');

    private final String keyPathPrefix;
    private List<DateTimeFormatter> dateFormatters;
    public KeyPathGenerator(final String keyPathPrefix) {
        this.keyPathPrefix = keyPathPrefix;
        this.dateFormatters = keyPathPrefix == null ? Collections.emptyList() : getDatePatternFormatter(keyPathPrefix);
    }

    public String generate() {
        if (!dateFormatters.isEmpty()) {
            final ZonedDateTime time = getCurrentUtcTime();
            final AtomicReference<String> kpp = new AtomicReference<>(keyPathPrefix);
            dateFormatters.forEach(dateFormatter -> {
                final String replacement = dateFormatter.format(time);
                final String temp = kpp.get().replaceFirst(TIME_PATTERN_REGULAR_EXPRESSION, replacement);
                kpp.set(temp);
            });
            return kpp.get();
        } else {
            return keyPathPrefix;
        }
    }

    private static ZonedDateTime getCurrentUtcTime() {
        return LocalDateTime.now().atZone(ZoneId.systemDefault()).withZoneSameInstant(UTC_ZONE_ID);
    }

    private List<DateTimeFormatter> getDatePatternFormatter(final String keyPathPrefix) {
        final Pattern pattern = Pattern.compile(TIME_PATTERN_INTERNAL_EXTRACTOR_REGULAR_EXPRESSION);
        final Matcher timePatternMatcher = pattern.matcher(keyPathPrefix);
        final ImmutableList.Builder<DateTimeFormatter> patterns = new ImmutableList.Builder();
        while (timePatternMatcher.find()) {
            final String timePattern = timePatternMatcher.group(1);
            if (timePattern.contains(TIME_PATTERN_STARTING_SYMBOLS)) { //check if it is a nested pattern such as "%{%{yyyy}}"
                throw new IllegalArgumentException("key_path_prefix doesn't allow nested date-time patterns.");
            }
            validateNoSpecialCharsInTimePattern(timePattern);
            validateTimePatternGranularity(timePattern);
            patterns.add(DateTimeFormatter.ofPattern(timePattern));
        }
        return patterns.build();
    }

    private void validateNoSpecialCharsInTimePattern(final String timePattern) {
        final boolean containsInvalidCharacter = timePattern.chars()
            .mapToObj(c -> (char) c)
            .anyMatch(INVALID_CHARS::contains);
        if (containsInvalidCharacter) {
            throw new IllegalArgumentException("key_path_prefix date-time pattern contains one or multiple special characters: " + INVALID_CHARS);
        }
    }

    private static void validateTimePatternGranularity(final String timePattern) {
        final boolean containsUnsupportedTimeSymbol = timePattern.chars()
            .mapToObj(c -> (char) c)
            .anyMatch(UNSUPPORTED_TIME_GRANULARITY_CHARS::contains);
        if (containsUnsupportedTimeSymbol) {
            throw new IllegalArgumentException("key_path_prefix pattern contains date-time patterns that are less than one day: " + UNSUPPORTED_TIME_GRANULARITY_CHARS);
        }
    }
}
