/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws.sqs.common.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.util.Map;

/**
 * JsonCodec parses the sqs message into json.
 * <p>
 */
public class JsonCodec implements Codec {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Record<Event> parse(final String message) throws IOException {
        return new Record<>(JacksonLog.builder().withData(mapper.readValue(message, Map.class)).build());
    }
}
