/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.io.IOException;

/**
 * Deserializes {@link ByteCount} values using Jackson.
 *
 * @since 2.1
 */
public class ByteCountDeserializer extends StdDeserializer<ByteCount> {
    public ByteCountDeserializer() {
        this(ByteCount.class);
    }

    protected ByteCountDeserializer(final Class<?> valueClass) {
        super(valueClass);
    }

    @Override
    public ByteCount deserialize(final JsonParser parser, final DeserializationContext context) throws IOException, JacksonException {
        final String byteString = parser.getValueAsString();

        try {
            return ByteCount.parse(byteString);
        } catch (final Exception ex) {
            throw new IllegalArgumentException(ex.getMessage());
        }
    }
}
