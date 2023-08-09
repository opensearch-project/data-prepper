/*
 * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */
package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

public class CustomLocalDateTimeDeserializer extends StdDeserializer<LocalDateTime> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomLocalDateTimeDeserializer.class);
    static final String CURRENT_LOCAL_DATE_TIME_STRING = "now";

    public CustomLocalDateTimeDeserializer() {
        this(null);
    }

    public CustomLocalDateTimeDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public LocalDateTime deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        final String valueAsString = parser.getValueAsString();

        if (valueAsString.equals(CURRENT_LOCAL_DATE_TIME_STRING)) {
            return LocalDateTime.now();
        } else {
            try {
                return LocalDateTime.parse(valueAsString);
            } catch (final DateTimeParseException e) {
                LOG.error("Unable to parse {} to LocalDateTime.", valueAsString, e);
                throw new IllegalArgumentException("Unable to obtain instance of LocalDateTime from " + valueAsString, e);
            }
        }
    }
}
