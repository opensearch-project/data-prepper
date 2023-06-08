/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.handler;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *  implements the SqsMessageHandler to read and parse the sqs message and push to buffer.
 *
 */
public class RawSqsMessageHandler implements SqsMessageHandler {

    private final SqsService sqsService;

    private Buffer<Record<Event>> buffer;

    public RawSqsMessageHandler(final Buffer<Record<Event>> buffer,
                                final SqsService sqsService){
        this.buffer = buffer;
        this.sqsService = sqsService;
    }

    /**
     *  helps to send end to end acknowledgements after successful processing.
     *
     * @param messages  - list of sqs messages for processing
     * @return AcknowledgementSet - will generate the AcknowledgementSet if endToEndAcknowledgementsEnabled is true else null
     */
    @Override
    public List<DeleteMessageBatchRequestEntry> handleMessage(final List<Message> messages,
                                                              final AcknowledgementSet acknowledgementSet) {
        List<Record<Event>> events = new ArrayList<>(messages.size());
        messages.forEach(message -> {
            final Record<Event> eventRecord = new Record<Event>(JacksonEvent.fromMessage(message.body()));
            events.add(eventRecord);
            if(Objects.nonNull(acknowledgementSet)){
                acknowledgementSet.add(eventRecord.getData());
            }
        });
        try {
            if(!events.isEmpty())
                buffer.writeAll(events, (int) Duration.ofSeconds(10).toMillis());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return sqsService.getDeleteMessageBatchRequestEntryList(messages);
    }
}
