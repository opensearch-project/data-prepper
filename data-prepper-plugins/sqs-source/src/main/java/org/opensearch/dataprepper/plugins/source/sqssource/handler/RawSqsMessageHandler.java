/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.handler;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.Objects;

/**
 *  implements the SqsMessageHandler to read and parse the sqs message and push to buffer.
 *
 */
public class RawSqsMessageHandler implements SqsMessageHandler {

    private final SqsService sqsService;

    public RawSqsMessageHandler(final SqsService sqsService){
        this.sqsService = sqsService;
    }

    /**
     *  helps to send end to end acknowledgements after successful processing.
     *
     * @param messages  - list of sqs messages for processing
     * @return AcknowledgementSet - will generate the AcknowledgementSet if endToEndAcknowledgementsEnabled is true else null
     */
    @Override
    public List<DeleteMessageBatchRequestEntry> handleMessages(final List<Message> messages,
                                                               final BufferAccumulator<Record<Event>> bufferAccumulator,
                                                               final AcknowledgementSet acknowledgementSet) {
        messages.forEach(message -> {
            final Record<Event> eventRecord = new Record<Event>(JacksonEvent.fromMessage(message.body()));
            try {
                // Always add record to acknowledgementSet before adding to
                // buffer because another thread may take and process
                // buffer contents before the event record is added
                // to acknowledgement set
                if(Objects.nonNull(acknowledgementSet)){
                    acknowledgementSet.add(eventRecord.getData());
                }
                bufferAccumulator.add(eventRecord);
            } catch (Exception e) {
                // Exception may occur when we failed to flush. In which
                // case, not sending acknowledgement would be correct because
                // we need to retry
                throw new RuntimeException(e);
            }
        });
        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return sqsService.getDeleteMessageBatchRequestEntryList(messages);
    }
}
