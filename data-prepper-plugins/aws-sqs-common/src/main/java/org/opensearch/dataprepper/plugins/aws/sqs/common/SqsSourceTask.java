/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class SqsSourceTask implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(SqsSourceTask.class);

    private final SqsService sqsService;

    private final SqsOptions sqsOptions;

    private final SqsMetrics sqsMetrics;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private final boolean endToEndAcknowledgementsEnabled;

    public SqsSourceTask(final SqsService sqsService,
                         final SqsOptions sqsOptions,
                         final SqsMetrics sqsMetrics,
                         final AcknowledgementSetManager acknowledgementSetManager,
                         final boolean endToEndAcknowledgementsEnabled) {
        this.sqsService = sqsService;
        this.sqsOptions = sqsOptions;
        this.sqsMetrics = sqsMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.endToEndAcknowledgementsEnabled = endToEndAcknowledgementsEnabled;
    }
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            int messagesProcessed = 0;
            try {
                messagesProcessed = processSqsMessages();
            } catch (final Exception e) {
                LOG.error("Unable to process SQS messages. Processing error due to: {}", e.getMessage());
                sqsService.applyBackoff();
            }
            if (messagesProcessed > 0 && sqsOptions.getPollDelay().toMillis() > 0) {
                try {
                    Thread.sleep(sqsOptions.getPollDelay().toMillis());
                } catch (final InterruptedException e) {
                    LOG.error("Thread is interrupted while polling SQS.", e);
                }
            }
        }
    }
    public int processSqsMessages() {
        final List<Message> sqsMessages = sqsService.getMessagesFromSqs(sqsOptions);
        // TODO: Apply Codec
        sqsMessages.forEach(message -> {

            AcknowledgementSet acknowledgementSet = null;
            if (endToEndAcknowledgementsEnabled) {
                acknowledgementSet = acknowledgementSetManager.create(result -> {
                    sqsMetrics.getAcknowledgementSetCallbackCounter().increment();
                    if (result.booleanValue()) {
                        sqsService.deleteMessageFromQueue(message.receiptHandle(),sqsOptions);
                    }
                }, Duration.ofSeconds(10));
            }
            Optional<String> parsedMessage = Optional.empty();
            try {
                parsedMessage = sqsService.parseMessage(message,acknowledgementSet);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if(endToEndAcknowledgementsEnabled){
                // TODO: end to end ack code pending
            }else{
                sqsService.deleteMessageFromQueue(parsedMessage.get(),sqsOptions);
            }
        });
        return sqsMessages.size();
    }

}
