/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This deserializer is used for configurations that use a {@link Duration} type when deserialized by Jackson
 * It supports ISO 8601 notation ("PT20.345S", "PT15M", etc.) and simple durations for
 * seconds (60s) and milliseconds (100ms). It does not support combining the units for simple durations ("60s 100ms" is not allowed).
 * Whitespace is ignored and leading zeroes are not allowed.
 * @since 1.3
 */
public class DataPrepperDurationDeserializer extends StdDeserializer<Duration> {

    private static final String SIMPLE_DURATION_REGEX = "^(0|[1-9]\\d*)(s|ms)$";
    private static final Pattern SIMPLE_DURATION_PATTERN = Pattern.compile(SIMPLE_DURATION_REGEX);

    public DataPrepperDurationDeserializer() {
        this(null);
    }
    protected DataPrepperDurationDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        final String durationString = p.getValueAsString();

        Duration duration;

        try {
            duration = Duration.parse(durationString);
        } catch (final DateTimeParseException e) {
            duration = parseSimpleDuration(durationString);
            if (duration == null) {
                throw new IllegalArgumentException("Durations must use either ISO 8601 notation or simple notations for seconds (60s) or milliseconds (100ms). Whitespace is ignored.");
            }
        }

        return duration;
    }

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
