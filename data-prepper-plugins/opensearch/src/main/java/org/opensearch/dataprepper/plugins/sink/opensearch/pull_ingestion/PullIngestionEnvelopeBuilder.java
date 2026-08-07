/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PullIngestionEnvelopeBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PullIngestionEnvelopeBuilder.class);

    private final ObjectMapper objectMapper;

    public PullIngestionEnvelopeBuilder(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public byte[] build(final String id, final long version, final String source, final String opType) throws JsonProcessingException {
        final Map<String, Object> envelope = new HashMap<>();
        envelope.put("_id", id);
        envelope.put("_version", String.valueOf(version));
        envelope.put("_op_type", opType);

        try {
            envelope.put("_source", objectMapper.readTree(source));
        } catch (final JsonProcessingException e) {
            LOG.error("Failed to parse source document as JSON, writing as raw string", e);
            envelope.put("_source", source);
        }

        return objectMapper.writeValueAsBytes(envelope);
    }
}
