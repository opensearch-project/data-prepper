/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class RawSqsMessageHandler implements SqsMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RawSqsMessageHandler.class);
    private final MessageFieldStrategy messageFieldStrategy;

    public RawSqsMessageHandler(final MessageFieldStrategy messageFieldStrategy) {
        this.messageFieldStrategy = messageFieldStrategy;
    }

    @Override
    public void handleMessage(final Message message,
                              final String url,
                              final Buffer<Record<Event>> buffer,
                              final int bufferTimeoutMillis,
                              final AcknowledgementSet acknowledgementSet) {
        try {
            List<Event> events = messageFieldStrategy.parseEvents(message.body());
            Map<String, String> metadataMap = AttributeHandler.collectMetadataAttributes(message, url);
            List<Record<Event>> records = new ArrayList<>();
            for (Event event : events) {
                AttributeHandler.applyMetadataAttributes(event, metadataMap);
                if (acknowledgementSet != null) {
                    acknowledgementSet.add(event);
                }
                records.add(new Record<>(event));
            }
            buffer.writeAll(records, bufferTimeoutMillis);

        } catch (Exception e) {
            LOG.error("Error processing SQS message: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
