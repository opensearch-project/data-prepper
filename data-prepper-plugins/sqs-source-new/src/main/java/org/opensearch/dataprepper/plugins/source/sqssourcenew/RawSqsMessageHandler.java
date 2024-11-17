package org.opensearch.dataprepper.plugins.source.sqssourcenew;

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
import java.util.Map;
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
     * @param bufferAccumulator  - the buffer accumulator
     * @param acknowledgementSet - the acknowledgement set for end-to-end acknowledgements
     */
    @Override
    public void handleMessage(final Message message,
                              final BufferAccumulator<Record<Event>> bufferAccumulator,
                              final AcknowledgementSet acknowledgementSet) {
        try {
            Event event;

            String messageBody = message.body();

            // Default is to try to parse the message body as JSON
            try {
                JsonNode jsonNode = objectMapper.readTree(messageBody);

                event = JacksonEvent.builder()
                        .withEventType("sqs-event")
                        .withData(jsonNode)
                        .build();

            } catch (Exception e) {
                // Treat the message as plain text if json parsing doesn't work
                LOG.debug("Message body is not valid JSON. Treating as plain text.");
                event = JacksonEvent.builder()
                        .withEventType("sqs-event")
                        .withData(Map.of("message", messageBody))
                        .build();
            }

            if (Objects.nonNull(acknowledgementSet)) {
                acknowledgementSet.add(event);
            }

            bufferAccumulator.add(new Record<>(event));

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
}
