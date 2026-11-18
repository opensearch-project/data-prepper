/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.json.JsonException;
import jakarta.json.stream.JsonParser;
import org.junit.jupiter.api.Test;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.dataprepper.model.codec.JsonFactories;

import java.io.StringReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OpenSearchClientFactoryJsonpMapperTest {

    @Test
    void default_JacksonJsonpMapper_rejects_string_values_over_the_jackson_default_limit() {
        final String json = jsonWithStringValueOfLength(20_000_001);

        final JsonException exception = assertThrows(JsonException.class,
                () -> deserialize(new JacksonJsonpMapper(), json));

        assertThat(exception.getCause(), instanceOf(StreamConstraintsException.class));
    }

    @Test
    void createJacksonJsonpMapper_deserializes_string_values_above_the_jackson_default_limit() {
        final int length = 32 * 1024 * 1024;
        final String json = jsonWithStringValueOfLength(length);

        final ObjectNode result = deserialize(OpenSearchClientFactory.createJacksonJsonpMapper(), json);

        assertThat(result.get("largeField").textValue().length(), equalTo(length));
    }

    @Test
    void createJacksonJsonpMapper_rejects_string_values_above_the_raised_limit() {
        final String json = jsonWithStringValueOfLength(JsonFactories.MAX_JSON_STRING_LENGTH + 1);

        final JsonException exception = assertThrows(JsonException.class,
                () -> deserialize(OpenSearchClientFactory.createJacksonJsonpMapper(), json));

        assertThat(exception.getCause(), instanceOf(StreamConstraintsException.class));
    }

    @Test
    void createElasticsearchJacksonJsonpMapper_raises_the_max_string_length() {
        final co.elastic.clients.json.jackson.JacksonJsonpMapper mapper = OpenSearchClientFactory.createElasticsearchJacksonJsonpMapper();

        assertThat(mapper.objectMapper().getFactory().streamReadConstraints().getMaxStringLength(),
                equalTo(JsonFactories.MAX_JSON_STRING_LENGTH));
    }

    private static ObjectNode deserialize(final JacksonJsonpMapper mapper, final String json) {
        try (final JsonParser parser = mapper.jsonProvider().createParser(new StringReader(json))) {
            return mapper.deserialize(parser, ObjectNode.class);
        }
    }

    private static String jsonWithStringValueOfLength(final int length) {
        return "{\"largeField\":\"" + "a".repeat(length) + "\"}";
    }
}
