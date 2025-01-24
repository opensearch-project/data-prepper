/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsWorkerCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;

import java.time.Duration;
import java.util.*;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);
    static final String ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME = "acknowledgementSetCallbackCounter";
    private final SqsWorkerCommon sqsWorkerCommon;
    private final SqsEventProcessor sqsEventProcessor;
    private final QueueConfig queueConfig;
    private final boolean endToEndAcknowledgementsEnabled;
    private final Buffer<Record<Event>> buffer;
    private final int bufferTimeoutMillis;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Counter acknowledgementSetCallbackCounter;
    private int failedAttemptCount;
    private volatile boolean isStopped = false;
    private final Map<Message, Integer> messageVisibilityTimesMap;

    public SqsWorker(final Buffer<Record<Event>> buffer,
                     final AcknowledgementSetManager acknowledgementSetManager,
                     final SqsClient sqsClient,
                     final SqsSourceConfig sqsSourceConfig,
                     final QueueConfig queueConfig,
                     final PluginMetrics pluginMetrics,
                     final SqsEventProcessor sqsEventProcessor,
                     final Backoff backoff) {
        this.sqsWorkerCommon = new SqsWorkerCommon(sqsClient, backoff, pluginMetrics, acknowledgementSetManager);
        this.queueConfig = queueConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sqsEventProcessor = sqsEventProcessor;
        this.buffer = buffer;
        this.bufferTimeoutMillis = (int) sqsSourceConfig.getBufferTimeout().toMillis();
        this.endToEndAcknowledgementsEnabled = sqsSourceConfig.getAcknowledgements();
        this.messageVisibilityTimesMap = new HashMap<>();
        this.failedAttemptCount = 0;
        this.acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME);
    }

    @Override
    public void run() {
        while (!isStopped) {
            int messagesProcessed = 0;
            try {
                messagesProcessed = processSqsMessages();
            } catch (final Exception e) {
                LOG.error("Unable to process SQS messages. Processing error due to: {}", e.getMessage());
                sqsWorkerCommon.applyBackoff();
            }

            if (messagesProcessed > 0 && queueConfig.getPollDelay().toMillis() > 0) {
                try {
                    Thread.sleep(queueConfig.getPollDelay().toMillis());
                } catch (final InterruptedException e) {
                    LOG.error("Thread is interrupted while polling SQS.", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    int processSqsMessages() {
        List<Message> messages = sqsWorkerCommon.pollSqsMessages(queueConfig.getUrl(),
                queueConfig.getMaximumMessages(),
                queueConfig.getWaitTime(),
                queueConfig.getVisibilityTimeout());
        if (!messages.isEmpty()) {
            final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = processSqsEvents(messages);
            if (!deleteMessageBatchRequestEntries.isEmpty()) {
                sqsWorkerCommon.deleteSqsMessages(queueConfig.getUrl(), deleteMessageBatchRequestEntries);
            }
        }
        return messages.size();
    }

    private List<DeleteMessageBatchRequestEntry> processSqsEvents(final List<Message> messages) {
        final List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntryCollection = new ArrayList<>();
        final Map<Message, AcknowledgementSet> messageAcknowledgementSetMap = new HashMap<>();
        final Map<Message, List<DeleteMessageBatchRequestEntry>> messageWaitingForAcknowledgementsMap = new HashMap<>();

        for (Message message : messages) {
            List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = new ArrayList<>();
            AcknowledgementSet acknowledgementSet = null;

            final int visibilityTimeout = queueConfig.getVisibilityTimeout() != null
                    ? (int) queueConfig.getVisibilityTimeout().getSeconds()
                    : 30;

            final int maxVisibilityTimeout = (int) queueConfig.getVisibilityDuplicateProtectionTimeout().getSeconds();
            final int progressCheckInterval = visibilityTimeout / 2 - 1;
            if (endToEndAcknowledgementsEnabled) {
                int expiryTimeout = queueConfig.getVisibilityDuplicateProtection()
                        ? maxVisibilityTimeout
                        : visibilityTimeout - 2;
                acknowledgementSet = acknowledgementSetManager.create(result -> {
                    acknowledgementSetCallbackCounter.increment();
                    if (queueConfig.getVisibilityDuplicateProtection()) {
                        messageVisibilityTimesMap.remove(message);
                    }
                    if (result) {
                        sqsWorkerCommon.deleteSqsMessages(queueConfig.getUrl(), waitingForAcknowledgements);
                    }
                }, Duration.ofSeconds(expiryTimeout));
                if (queueConfig.getVisibilityDuplicateProtection()) {
                    acknowledgementSet.addProgressCheck(ratio -> {
                        int newValue = messageVisibilityTimesMap.getOrDefault(message, visibilityTimeout) + progressCheckInterval;
                        if (newValue >= maxVisibilityTimeout) {
                            return;
                        }
                        messageVisibilityTimesMap.put(message, newValue);
                        sqsWorkerCommon.increaseVisibilityTimeout(queueConfig.getUrl(),
                                message.receiptHandle(),
                                visibilityTimeout,
                                message.messageId());
                    }, Duration.ofSeconds(progressCheckInterval));
                }
                messageAcknowledgementSetMap.put(message, acknowledgementSet);
                messageWaitingForAcknowledgementsMap.put(message, waitingForAcknowledgements);
            }
        }

        for (Message message : messages) {
            final AcknowledgementSet acknowledgementSet = messageAcknowledgementSetMap.get(message);
            final List<DeleteMessageBatchRequestEntry> waitingForAcknowledgements = messageWaitingForAcknowledgementsMap.get(message);
            final Optional<DeleteMessageBatchRequestEntry> deleteEntry = processSqsObject(message, acknowledgementSet);
            if (endToEndAcknowledgementsEnabled) {
                deleteEntry.ifPresent(waitingForAcknowledgements::add);
                if (acknowledgementSet != null) {
                    acknowledgementSet.complete();
                }
            } else {
                deleteEntry.ifPresent(deleteMessageBatchRequestEntryCollection::add);
            }
        }
        return deleteMessageBatchRequestEntryCollection;
    }

    private Optional<DeleteMessageBatchRequestEntry> processSqsObject(final Message message,
                                                                      final AcknowledgementSet acknowledgementSet) {
        try {
            sqsEventProcessor.addSqsObject(message, queueConfig.getUrl(), buffer, bufferTimeoutMillis, acknowledgementSet);
            return Optional.of(sqsWorkerCommon.buildDeleteMessageBatchRequestEntry(message.messageId(), message.receiptHandle()));
        } catch (final Exception e) {
            sqsWorkerCommon.getSqsMessagesFailedCounter().increment();
            LOG.error("Error processing from SQS: {}. Retrying with exponential backoff.", e.getMessage());
            sqsWorkerCommon.applyBackoff();
            return Optional.empty();
        }
    }

    void stop() {
        isStopped = true;
        sqsWorkerCommon.stop();
    }
}
