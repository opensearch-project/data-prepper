/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.dataprepper.model.types.ByteCount;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

/**
 *   An implementation class of s3 index configuration Options
 */
public class ThresholdOptions {
	
	private static final String SIMPLE_DURATION_REGEX = "^(0|[1-9]\\d*)(s|ms)$";
    private static final Pattern SIMPLE_DURATION_PATTERN = Pattern.compile(SIMPLE_DURATION_REGEX);
    
    static final int DEFAULT_EVENT_COUNT = 2000;
    private static final String DEFAULT_BYTE_CAPACITY = "50mb";
    private static final String DEFAULT_TIMEOUT = "180s";

    @JsonProperty("event_count")
    @NotNull
    private int eventCount = DEFAULT_EVENT_COUNT;

    @JsonProperty("byte_capacity")
    @NotNull
    private String byteCapacity = DEFAULT_BYTE_CAPACITY;

    @JsonProperty("event_collection_duration")
    private String eventCollectionDuration = DEFAULT_TIMEOUT;

    /**
     *   Read event collection duration configuration
     */
    public Duration getEventCollectionDuration() {

        Duration duration;
        try {
            duration = Duration.parse(eventCollectionDuration);
        } catch (final DateTimeParseException e) {
            duration = parseSimpleDuration(eventCollectionDuration);
            if (duration == null) {
                throw new IllegalArgumentException("Durations must use either ISO 8601 notation or simple notations for seconds (60s) or milliseconds (100ms). Whitespace is ignored.");
            }
        }
        return duration;
    }

    /**
     *  Read byte capacity configuration
     */
    public ByteCount getByteCapacity() {
        return ByteCount.parse(byteCapacity);
    }

    /**
     *  Read the event count configuration
     */
    public int getEventCount() {
        return eventCount;
    }
    
    /**
     *   parse event duration configuration
     */
    private Duration parseSimpleDuration(final String durationString) throws IllegalArgumentException {
        final String durationStringNoSpaces = durationString.replaceAll("\\s", "");
        final Matcher matcher = SIMPLE_DURATION_PATTERN.matcher(durationStringNoSpaces);
        if (!matcher.find()) {
            return null;
        }

        final long durationNumber = Long.parseLong(matcher.group(1));
        final String durationUnit = matcher.group(2);

        return getDurationFromUnitAndNumber(durationNumber, durationUnit);
    }

    /**
     *   Return Duration in seconds/milliseconds of  configuration Event Collection Duration
     */
    private Duration getDurationFromUnitAndNumber(final long durationNumber, final String durationUnit) {
        switch (durationUnit) {
            case "s":
                return Duration.ofSeconds(durationNumber);
            case "ms":
                return Duration.ofMillis(durationNumber);
        }
        return null;
    }
}
