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
        LOG.info("task called for - {} ", sqsOptions.getSqsUrl());
        while (!Thread.currentThread().isInterrupted()) {
            processSqsMessages();
        }
    }

    /**
     * read the messages from sqs queue and push the message into buffer and finally will delete
     * the sqs message from queue after successful buffer push.
     */
    void processSqsMessages() {
       List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = new ArrayList<>();
       AcknowledgementSet acknowledgementSet = doEndToEndAcknowledgements(waitingForAcknowledgements);
       final List<Message> messages = sqsService.getMessagesFromSqs(sqsOptions);
       List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = null;
       if(!messages.isEmpty()) {
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

    /**
     *  helps to send end to end acknowledgements after successful processing.
     *
     * @param waitingForAcknowledgements  - will pass the processed messages batch in Delete message batch request.
     * @return AcknowledgementSet - will generate the AcknowledgementSet if endToEndAcknowledgementsEnabled is true.
     */
    private AcknowledgementSet doEndToEndAcknowledgements(List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements) {
        AcknowledgementSet acknowledgementSet = null;
        if (endToEndAcknowledgementsEnabled) {
            acknowledgementSet = acknowledgementSetManager.create(result -> {
                sqsMetrics.getAcknowledgementSetCallbackCounter().increment();
                if (result == true) {
                    sqsService.deleteMessagesFromQueue(waitingForAcknowledgements,sqsOptions.getSqsUrl());
                }
            }, Duration.ofSeconds(10));
        }
        return acknowledgementSet;
    }
}
