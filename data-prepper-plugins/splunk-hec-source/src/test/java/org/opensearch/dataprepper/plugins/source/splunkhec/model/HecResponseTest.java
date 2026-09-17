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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

class HecResponseTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void success_creates_response_with_code_0() {
        final HecResponse response = HecResponse.success();
        assertThat(response.getCode(), equalTo(0));
        assertThat(response.getText(), equalTo("Success"));
        assertThat(response.getAckId(), nullValue());
        assertThat(response.getInvalidEventNumber(), nullValue());
    }

    @Test
    void successWithAckId_creates_response_with_ackId() {
        final HecResponse response = HecResponse.successWithAckId(42L);
        assertThat(response.getCode(), equalTo(0));
        assertThat(response.getText(), equalTo("Success"));
        assertThat(response.getAckId(), equalTo(42L));
    }

    @Test
    void error_creates_response_with_error_code() {
        final HecResponse response = HecResponse.error(HecResponseCode.TOKEN_INVALID);
        assertThat(response.getCode(), equalTo(3));
        assertThat(response.getText(), equalTo("Invalid authorization"));
    }

    @Test
    void errorWithInvalidEventNumber_includes_event_number() {
        final HecResponse response = HecResponse.errorWithInvalidEventNumber(HecResponseCode.EVENT_FIELD_REQUIRED, 5);
        assertThat(response.getCode(), equalTo(12));
        assertThat(response.getInvalidEventNumber(), equalTo(5));
    }

    @Test
    void serialization_excludes_null_fields() throws IOException {
        final HecResponse response = HecResponse.success();
        final String json = OBJECT_MAPPER.writeValueAsString(response);

        assertThat(json, not(containsString("ackId")));
        assertThat(json, not(containsString("invalid-event-number")));
    }

    @Test
    void serialization_includes_ackId_when_present() throws IOException {
        final HecResponse response = HecResponse.successWithAckId(7L);
        final String json = OBJECT_MAPPER.writeValueAsString(response);
        @SuppressWarnings("unchecked")
        final Map<String, Object> map = OBJECT_MAPPER.readValue(json, Map.class);

        assertThat(map.get("ackId"), equalTo(7));
        assertThat(map.get("code"), equalTo(0));
        assertThat(map.get("text"), equalTo("Success"));
    }
}
