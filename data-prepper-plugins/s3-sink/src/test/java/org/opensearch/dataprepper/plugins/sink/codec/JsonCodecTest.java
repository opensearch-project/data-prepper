/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.codec;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import com.fasterxml.jackson.databind.ObjectMapper;

class JsonCodecTest {

    @Test
    void parse_with_events_output_stream_json_codec() throws IOException {

        final Map<String, String> eventData = new HashMap<>();
        String value1 = UUID.randomUUID().toString();
        eventData.put("key1", value1);
        String value2 = UUID.randomUUID().toString();
        eventData.put("key2", value2);
        final JacksonEvent event = JacksonLog.builder().withData(eventData).withEventType("LOG").build();
        String output = createObjectUnderTest().parse(event);
        assertNotNull(output);

        ObjectMapper objectMapper = new ObjectMapper();
        Map deserializedData = objectMapper.readValue(output, Map.class);
        assertThat(deserializedData, notNullValue());
        assertThat(deserializedData.get("key1"), notNullValue());
        assertThat(deserializedData.get("key1"), equalTo(value1));
        assertThat(deserializedData.get("key2"), notNullValue());
        assertThat(deserializedData.get("key2"), equalTo(value2));
    }

    private JsonCodec createObjectUnderTest() {
        return new JsonCodec();
    }
}