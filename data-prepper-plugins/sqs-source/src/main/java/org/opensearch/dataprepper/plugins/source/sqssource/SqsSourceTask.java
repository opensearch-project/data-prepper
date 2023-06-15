/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;

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

    public SqsSourceTask(final SqsService sqsService,
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
       final List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = new ArrayList<>();
       AcknowledgementSet acknowledgementSet = sqsService.doEndToEndAcknowledgements(sqsOptions.getSqsUrl(),
                acknowledgementSetManager,
                endToEndAcknowledgementsEnabled,
                waitingForAcknowledgements);
       final List<Message> messages = sqsService.getMessagesFromSqs(sqsOptions);
        List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = null;
       if(!messages.isEmpty()) {
           LOG.info("Thread Name : {} , messages processed: {}",Thread.currentThread().getName(),messages.size());
           sqsMetrics.getSqsMessagesReceivedCounter().increment();
           try {
               deleteMessageBatchRequestEntries = sqsHandler.handleMessage(messages, acknowledgementSet);
           } catch(final Exception e) {
               LOG.error("Exception while handleMessage : ",e);
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
