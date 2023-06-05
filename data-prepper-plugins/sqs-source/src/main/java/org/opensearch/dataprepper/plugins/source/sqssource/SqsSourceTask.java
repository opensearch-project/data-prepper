/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;

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
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final List<Message> messages = sqsService.getMessagesFromSqs(sqsOptions);
                if(!messages.isEmpty()) {
                    sqsMetrics.getSqsMessagesReceivedCounter().increment();
                    sqsHandler.handleMessage(messages,sqsOptions.getSqsUrl());
                }
            } catch (final Exception e) {
                LOG.error("Unable to process SQS messages. Processing error due to: ", e);
                sqsService.applyBackoff();
            }
        }
    }
}
