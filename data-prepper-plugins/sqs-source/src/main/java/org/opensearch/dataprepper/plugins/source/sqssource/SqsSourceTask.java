/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Class responsible for processing the sqs message with the help of <code>SqsMessageHandler</code>
 *
 */
public class SqsSourceTask implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(SqsSourceTask.class);

    private final SqsService sqsService;

    private final SqsOptions sqsOptions;

    private final SqsMetrics sqsMetrics;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private final boolean endToEndAcknowledgementsEnabled;

    private final SqsMessageHandler sqsHandler;

    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    public SqsSourceTask(final Buffer<Record<Event>> buffer,
                         final int noOfRecordsToAccumulate,
                         final Duration bufferTimeout,
                         final SqsService sqsService,
                         final SqsOptions sqsOptions,
                         final SqsMetrics sqsMetrics,
                         final AcknowledgementSetManager acknowledgementSetManager,
                         final boolean endToEndAcknowledgementsEnabled,
                         final SqsMessageHandler sqsHandler) {
        this.sqsService = sqsService;
        this.sqsOptions = sqsOptions;
        this.sqsMetrics = sqsMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.endToEndAcknowledgementsEnabled = endToEndAcknowledgementsEnabled;
        this.sqsHandler = sqsHandler;
        this.bufferAccumulator = BufferAccumulator.create(buffer,noOfRecordsToAccumulate,bufferTimeout);
    }

    /**
     *  read the messages from sqs queue and push the message into buffer in a loop.
     */
    @Override
    public void run() {
            try {
                processSqsMessages();
            } catch (final Exception e) {
                LOG.error("Unable to process SQS messages. Processing error due to: {}", e.getMessage());
                sqsService.applyBackoff();
            }
    }

    /**
     * read the messages from sqs queue and push the message into buffer and finally will delete
     * the sqs message from queue after successful buffer push.
     */
    void processSqsMessages() {
        AcknowledgementSet acknowledgementSet = null;
        List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = null;
        final List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = new ArrayList<>();

       if(endToEndAcknowledgementsEnabled)
           acknowledgementSet = sqsService.createAcknowledgementSet(sqsOptions.getSqsUrl(),
                acknowledgementSetManager,
                waitingForAcknowledgements);

       final List<Message> messages = sqsService.getMessagesFromSqs(sqsOptions);

       if(!messages.isEmpty()) {
           LOG.info("Thread Name : {} , messages processed: {}",Thread.currentThread().getName(),messages.size());
           sqsMetrics.getSqsMessagesReceivedCounter().increment();
           try {
               deleteMessageBatchRequestEntries = sqsHandler.handleMessages(messages, bufferAccumulator, acknowledgementSet);
           } catch(final Exception e) {
               LOG.error("Error while processing handleMessages : ",e);
               sqsService.applyBackoff();
           }
           if(deleteMessageBatchRequestEntries != null) {
               if (endToEndAcknowledgementsEnabled)
                   waitingForAcknowledgements.addAll(deleteMessageBatchRequestEntries);
               else
                   sqsService.deleteMessagesFromQueue(deleteMessageBatchRequestEntries, sqsOptions.getSqsUrl());
           }
       }
    }
}
