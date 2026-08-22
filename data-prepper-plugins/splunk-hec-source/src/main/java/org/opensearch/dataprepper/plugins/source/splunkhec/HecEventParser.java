/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HecEventParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = new JsonFactory(OBJECT_MAPPER);

    List<Map<String, Object>> parse(final String data) throws HecParseException {
        Objects.requireNonNull(data, "data must not be null");
        final List<Map<String, Object>> events = new ArrayList<>();
        try (JsonParser parser = JSON_FACTORY.createParser(data)) {
            parser.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
            while (parser.nextToken() != null) {
                if (parser.currentToken() == JsonToken.START_OBJECT) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> event = OBJECT_MAPPER.readValue(parser, Map.class);
                    events.add(event);
                }
            }
        } catch (final IOException e) {
            throw new HecParseException(events.size(), e);
        }
        return events;
    }
}
