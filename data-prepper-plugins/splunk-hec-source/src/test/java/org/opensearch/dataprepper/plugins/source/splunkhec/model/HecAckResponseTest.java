/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;

class HecAckResponseTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void constructor_and_getter() {
        final Map<String, Boolean> acks = Map.of("1", true, "2", false);
        final HecAckResponse response = new HecAckResponse(acks);

        assertThat(response.getAcks(), notNullValue());
        assertThat(response.getAcks(), hasEntry("1", true));
        assertThat(response.getAcks(), hasEntry("2", false));
    }

    @Test
    void serialization_produces_correct_json() throws IOException {
        final Map<String, Boolean> acks = Map.of("0", true, "1", false);
        final HecAckResponse response = new HecAckResponse(acks);
        final String json = OBJECT_MAPPER.writeValueAsString(response);

        @SuppressWarnings("unchecked")
        final Map<String, Object> parsed = OBJECT_MAPPER.readValue(json, Map.class);
        @SuppressWarnings("unchecked")
        final Map<String, Boolean> parsedAcks = (Map<String, Boolean>) parsed.get("acks");

        assertThat(parsedAcks, notNullValue());
        assertThat(parsedAcks, hasEntry("0", true));
        assertThat(parsedAcks, hasEntry("1", false));
    }
}
