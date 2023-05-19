/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonParser;
import org.opensearch.client.json.JsonpDeserializerBase;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonpParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;

public class JacksonValueParser<T> extends JsonpDeserializerBase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JacksonValueParser.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> clazz;
    protected JacksonValueParser(Class<T> clazz) {
        super(EnumSet.allOf(JsonParser.Event.class));
        this.clazz = clazz;
    }
    @Override
    public T deserialize(JsonParser parser, JsonpMapper mapper, JsonParser.Event event) {

        if (!(parser instanceof JacksonJsonpParser)) {
            throw new IllegalArgumentException("Jackson's ObjectMapper can only be used with the JacksonJsonpProvider");
        }
        com.fasterxml.jackson.core.JsonParser jkParser = ((JacksonJsonpParser) parser).jacksonParser();

        try {
            return objectMapper.readValue(jkParser, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}