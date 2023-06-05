/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource.handler;

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

public class RawSqsMessageHandler implements SqsMessageHandler {

    private final SqsService sqsService;
    
    private Buffer<Record<Event>> buffer;

    public RawSqsMessageHandler(final Buffer<Record<Event>> buffer,
                                final SqsService sqsService){
        this.buffer = buffer;
        this.sqsService = sqsService;
    }

    @Override
    public void handleMessage(final List<Message> messages,
                              final String queueUrl) {
        List<Record<Event>> events = new ArrayList<>(messages.size());
        List<DeleteMessageBatchRequestEntry> deleteMsgBatchReqList = new ArrayList<>(messages.size());
        messages.forEach(message -> {
            events.add(new Record<>(JacksonEvent.fromMessage(message.body())));
            deleteMsgBatchReqList.add(DeleteMessageBatchRequestEntry.builder()
                    .id(message.messageId()).receiptHandle(message.receiptHandle()).build());
        });
        try {
            if(!events.isEmpty())
                buffer.writeAll(events,(int)Duration.ofSeconds(10).toMillis());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        sqsService.deleteMessagesFromQueue(deleteMsgBatchReqList,queueUrl);
    }
}
