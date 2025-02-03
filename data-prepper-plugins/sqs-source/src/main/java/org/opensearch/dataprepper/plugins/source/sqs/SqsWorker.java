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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.plugins.source.sqs.common.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsWorkerCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SqsWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SqsWorker.class);
    static final String SQS_MESSAGE_DELAY_METRIC_NAME = "sqsMessageDelay";
    private final Timer sqsMessageDelayTimer;
    static final String ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME = "acknowledgementSetCallbackCounter";
    private final SqsWorkerCommon sqsWorkerCommon;
    private final SqsEventProcessor sqsEventProcessor;
    private final SqsClient sqsClient;
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
                     final SqsWorkerCommon sqsWorkerCommon,
                     final SqsSourceConfig sqsSourceConfig,
                     final QueueConfig queueConfig,
                     final PluginMetrics pluginMetrics,
                     final SqsEventProcessor sqsEventProcessor) {

        this.sqsWorkerCommon = sqsWorkerCommon;
        this.queueConfig = queueConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sqsEventProcessor = sqsEventProcessor;
        this.sqsClient = sqsClient;
        this.buffer = buffer;
        this.bufferTimeoutMillis = (int) sqsSourceConfig.getBufferTimeout().toMillis();
        this.endToEndAcknowledgementsEnabled = sqsSourceConfig.getAcknowledgements();
        this.messageVisibilityTimesMap = new HashMap<>();
        this.failedAttemptCount = 0;
        acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME);
        sqsMessageDelayTimer = pluginMetrics.timer(SQS_MESSAGE_DELAY_METRIC_NAME);

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
        try {
            List<Message> messages = sqsWorkerCommon.pollSqsMessages(
                    queueConfig.getUrl(),
                    sqsClient,
                    queueConfig.getMaximumMessages(),
                    queueConfig.getWaitTime(),
                    queueConfig.getVisibilityTimeout());

            if (!messages.isEmpty()) {
                final List<DeleteMessageBatchRequestEntry> deleteEntries = processSqsEvents(messages);
                if (!deleteEntries.isEmpty()) {
                    sqsWorkerCommon.deleteSqsMessages(queueConfig.getUrl(), sqsClient, deleteEntries);
                }
            } else {
                sqsMessageDelayTimer.record(Duration.ZERO);
            }
            return messages.size();
        } catch (SqsException e) {
            sqsWorkerCommon.applyBackoff();
            return 0;
        }
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
                        sqsWorkerCommon.deleteSqsMessages(queueConfig.getUrl(), sqsClient, waitingForAcknowledgements);
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
                                sqsClient,
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
            Duration duration = Duration.between(Instant.ofEpochMilli(Long.parseLong(message.attributes().get(MessageSystemAttributeName.SENT_TIMESTAMP))), Instant.now());
            sqsMessageDelayTimer.record(duration.isNegative() ? Duration.ZERO : duration); // Negative durations can occur if messages are processed immediately

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
            if (queueConfig.getOnErrorOption().equals(OnErrorOption.DELETE_MESSAGES)) {
                return Optional.of(sqsWorkerCommon.buildDeleteMessageBatchRequestEntry(message.messageId(), message.receiptHandle()));
            } else {
                return Optional.empty();
            }
        }
    }

    void stop() {
        isStopped = true;
        sqsWorkerCommon.stop();
    }
}
