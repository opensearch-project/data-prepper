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

    public DataPrepperDurationDeserializer() {
        this(null);
    }
    protected DataPrepperDurationDeserializer(final Class<?> vc) {
        super(vc);
    }

    @Override
    public Duration deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
        final String durationString = p.getValueAsString();

        return DataPrepperDurationParser.parse(durationString);
    }
}
