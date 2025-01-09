/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.util.Collections;
import java.util.Map;

public class RawSqsMessageHandler implements SqsMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RawSqsMessageHandler.class);

    @Override
    public void handleMessage(final Message message,
                              final String url,
                              final Buffer<Record<Event>> buffer,
                              final int bufferTimeoutMillis,
                              final AcknowledgementSet acknowledgementSet) {
        try {
            final Map<MessageSystemAttributeName, String> systemAttributes = message.attributes();
            final Map<String, MessageAttributeValue> customAttributes = message.messageAttributes();
            final Event event = JacksonEvent.builder()
                    .withEventType("DOCUMENT")
                    .withData(Collections.singletonMap("message", message.body()))
                    .build();

            final EventMetadata eventMetadata = event.getMetadata();
            eventMetadata.setAttribute("queueUrl", url);

            for (Map.Entry<MessageSystemAttributeName, String> entry : systemAttributes.entrySet()) {
                String originalKey = entry.getKey().toString();
                String lowerCamelCaseKey = originalKey.substring(0, 1).toLowerCase() + originalKey.substring(1);
                eventMetadata.setAttribute(lowerCamelCaseKey, entry.getValue());
            }

            for (Map.Entry<String, MessageAttributeValue> entry : customAttributes.entrySet()) {
                String originalKey = entry.getKey();
                String lowerCamelCaseKey = originalKey.substring(0, 1).toLowerCase() + originalKey.substring(1);
                eventMetadata.setAttribute(lowerCamelCaseKey, entry.getValue().stringValue());
            }

            if (acknowledgementSet != null) {
                acknowledgementSet.add(event);
            }
            buffer.write(new Record<>(event), bufferTimeoutMillis);
        } catch (Exception e) {
            LOG.error("Error processing SQS message: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
