package org.opensearch.dataprepper.plugins.source.sqs;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.time.Instant;
import java.util.Objects;

/**
 * Implements the SqsMessageHandler to read and parse SQS messages generically and push to buffer.
 */
public class RawSqsMessageHandler implements SqsMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RawSqsMessageHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Processes the SQS message, attempting to parse it as JSON, and adds it to the buffer.
     *
     * @param message            - the SQS message for processing
     * @param url                - the SQS queue url
     * @param bufferAccumulator  - the buffer accumulator
     * @param acknowledgementSet - the acknowledgement set for end-to-end acknowledgements
     */
    @Override
    public void handleMessage(final Message message,
                              final String url, 
                              final BufferAccumulator<Record<Event>> bufferAccumulator,
                              final AcknowledgementSet acknowledgementSet) {
        try {
            ObjectNode dataNode = objectMapper.createObjectNode();
            dataNode.set("message", parseMessageBody(message.body()));
            dataNode.put("queueUrl", url);

            Instant now = Instant.now();
            int unixTimestamp = (int) now.getEpochSecond(); 
            dataNode.put("sentTimestamp", unixTimestamp);

            final Record<Event> event = new Record<Event>(JacksonEvent.builder()
                .withEventType("sqs-event")
                .withData(dataNode)
                .build());
        

            if (Objects.nonNull(acknowledgementSet)) {
                acknowledgementSet.add(event.getData());
            }

            bufferAccumulator.add(new Record<>(event.getData()));

        } catch (Exception e) {
            LOG.error("Error processing SQS message: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode parseMessageBody(String messageBody) {
        try {
            return objectMapper.readTree(messageBody);
        } catch (Exception e) {
            return objectMapper.getNodeFactory().textNode(messageBody);
        }
    }
}