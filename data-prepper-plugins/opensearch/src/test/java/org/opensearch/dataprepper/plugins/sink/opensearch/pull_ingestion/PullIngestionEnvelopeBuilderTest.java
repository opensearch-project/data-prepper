/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class PullIngestionEnvelopeBuilderTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private String documentId;
    private long version;
    private String opType;

    @BeforeEach
    void setUp() {
        documentId = UUID.randomUUID().toString();
        version = ThreadLocalRandom.current().nextLong(1, 100000);
        opType = UUID.randomUUID().toString();
    }

    private PullIngestionEnvelopeBuilder createObjectUnderTest() {
        return new PullIngestionEnvelopeBuilder(objectMapper);
    }

    @Test
    void build_creates_correct_envelope() throws IOException {
        final String sourceJson = "{\"key\":\"val\"}";

        final byte[] result = createObjectUnderTest().build(documentId, version, sourceJson, opType);

        @SuppressWarnings("unchecked")
        final Map<String, Object> envelope = objectMapper.readValue(result, Map.class);
        assertThat(envelope.get("_id"), equalTo(documentId));
        assertThat(envelope.get("_version"), equalTo(String.valueOf(version)));
        assertThat(envelope.get("_op_type"), equalTo(opType));

        @SuppressWarnings("unchecked")
        final Map<String, Object> source = (Map<String, Object>) envelope.get("_source");
        assertThat(source.get("key"), equalTo("val"));
    }

    @Test
    void build_handles_non_json_source_as_raw_string() throws IOException {
        final String rawSource = UUID.randomUUID().toString();

        final byte[] result = createObjectUnderTest().build(documentId, version, rawSource, opType);

        @SuppressWarnings("unchecked")
        final Map<String, Object> envelope = objectMapper.readValue(result, Map.class);
        assertThat(envelope.get("_source"), equalTo(rawSource));
    }

    @Test
    void build_sets_version_as_string() throws IOException {
        final byte[] result = createObjectUnderTest().build(documentId, version, "{}", opType);

        @SuppressWarnings("unchecked")
        final Map<String, Object> envelope = objectMapper.readValue(result, Map.class);
        assertThat(envelope.get("_version"), equalTo(String.valueOf(version)));
    }
}
