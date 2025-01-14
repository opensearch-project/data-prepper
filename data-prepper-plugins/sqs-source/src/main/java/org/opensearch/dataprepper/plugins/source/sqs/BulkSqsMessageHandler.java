/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class BulkSqsMessageHandler implements SqsMessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BulkSqsMessageHandler.class);
    private final InputCodec codec;

    public BulkSqsMessageHandler(final InputCodec codec) {
        this.codec = codec;
    }

    @Override
    public void handleMessage(final Message message,
                              final String url,
                              final Buffer<Record<Event>> buffer,
                              final int bufferTimeoutMillis,
                              final AcknowledgementSet acknowledgementSet) {
        try {
            final String sqsBody = message.body();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(sqsBody.getBytes(StandardCharsets.UTF_8));
            codec.parse(inputStream, record -> {
                final Event event = record.getData();
                final EventMetadata eventMetadata = event.getMetadata();
                eventMetadata.setAttribute("queueUrl", url);
                for (Map.Entry<MessageSystemAttributeName, String> entry : message.attributes().entrySet()) {
                    final String originalKey = entry.getKey().toString();
                    final String lowerCamelCaseKey = originalKey.substring(0, 1).toLowerCase() + originalKey.substring(1);;
                    eventMetadata.setAttribute(lowerCamelCaseKey, entry.getValue());
                }

                for (Map.Entry<String, MessageAttributeValue> entry : message.messageAttributes().entrySet()) {
                    final String originalKey = entry.getKey();
                    final String lowerCamelCaseKey = originalKey.substring(0, 1).toLowerCase() + originalKey.substring(1);;
                    eventMetadata.setAttribute(lowerCamelCaseKey, entry.getValue().stringValue());
                }

                if (acknowledgementSet != null) {
                    acknowledgementSet.add(event);
                }

                try {
                    buffer.write(record, bufferTimeoutMillis);
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (final Exception e) {
            LOG.error("Error processing SQS message: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
