/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.configuration.SqsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SqsDynamicVisibilityTimeoutManager {

    private static final Logger LOG = LoggerFactory.getLogger(SqsDynamicVisibilityTimeoutManager.class);
    static final int VISIBILITY_TIMEOUT_SECONDS_PER_QUARTER_GB = 30;
    static final int QUARTER_GB_BYTES = 250_000_000;
    static final int MAX_VISIBILITY_TIMEOUT_SECONDS = 43_200;
    static final int MINIMUM_WAIT_BEFORE_CHANGE_VISIBILITY_TIMEOUT_SECONDS = 2;
    static final int TIME_BETWEEN_CHANGE_MESSAGE_CALL_AND_VISIBILITY_TIMEOUT_SECONDS = 10;

    private final SqsClient sqsClient;
    private final ScheduledExecutorService scheduledExecutorService;
    private final String queueUrl;
    private final int initialMessageVisibilityTimeout;
    private final int initialWaitBeforeIncrease;

    private final Map<String, ScheduledFuture<?>> messageToScheduledFuture;

    public SqsDynamicVisibilityTimeoutManager(final SqsClient sqsClient,
                                              final SqsOptions sqsOptions) {
        this.sqsClient = sqsClient;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(sqsOptions.getMaximumMessages());
        this.queueUrl = sqsOptions.getSqsUrl();
        this.messageToScheduledFuture = new HashMap<>();
        this.initialMessageVisibilityTimeout = (int) sqsOptions.getVisibilityTimeout().getSeconds();
        this.initialWaitBeforeIncrease = Math.max(MINIMUM_WAIT_BEFORE_CHANGE_VISIBILITY_TIMEOUT_SECONDS,
                initialMessageVisibilityTimeout - TIME_BETWEEN_CHANGE_MESSAGE_CALL_AND_VISIBILITY_TIMEOUT_SECONDS);
    }



    public void startDynamicVisibilityForMessage(final Message message, final long objectSizeBytes) {
        final int visibilityTimeoutIncrease = calculateVisibilityTimeoutFromObjectSize(objectSizeBytes);
        final int waitBeforeIncreasingAgain = Math.min(MINIMUM_WAIT_BEFORE_CHANGE_VISIBILITY_TIMEOUT_SECONDS,
                visibilityTimeoutIncrease - TIME_BETWEEN_CHANGE_MESSAGE_CALL_AND_VISIBILITY_TIMEOUT_SECONDS);

        final ScheduledFuture<?> scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
                () -> increaseVisibilityTimeout(visibilityTimeoutIncrease, message),
                initialWaitBeforeIncrease,
                waitBeforeIncreasingAgain, TimeUnit.SECONDS);

        messageToScheduledFuture.put(message.messageId(), scheduledFuture);
    }

    public void stopDynamicVisibilityForMessage(final String messageId) {
        final ScheduledFuture<?> scheduledFutureToCancel = messageToScheduledFuture.get(messageId);
        if (Objects.nonNull(scheduledFutureToCancel)) {
            boolean cancelled = scheduledFutureToCancel.cancel(true);

            if (!cancelled) {
                LOG.error("Unable to cancel dynamic visibility execution for message {}", messageId);
            } else {
                messageToScheduledFuture.remove(messageId);
            }
        }
    }

    public int getRemainingTimeBeforeNextChangeMessageIncrease(final String messageId) {
        final ScheduledFuture<?> scheduledFutureToCancel = messageToScheduledFuture.get(messageId);
        if (Objects.nonNull(scheduledFutureToCancel)) {
            return (int) scheduledFutureToCancel.getDelay(TimeUnit.SECONDS);
        }

        return initialMessageVisibilityTimeout;
    }

    public void shutdownDynamicMessaging() {
        try {
            scheduledExecutorService.shutdown();
            scheduledExecutorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Dynamic SQS messaging was interrupted during shutdown");
        } finally {
            if (!scheduledExecutorService.isTerminated()) {
                scheduledExecutorService.shutdown();
            }
        }
    }

    private void increaseVisibilityTimeout(final int visibilityTimeoutIncrease,
                                           final Message message) {

        final ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder()
                .receiptHandle(message.receiptHandle())
                .visibilityTimeout(visibilityTimeoutIncrease)
                .queueUrl(queueUrl)
                .build();

        try {
            sqsClient.changeMessageVisibility(changeMessageVisibilityRequest);
        } catch (final Exception e) {
            LOG.error("An exception occurred while changing the visibility timeout for message {}", message.messageId(), e);
        }
    }

    private int calculateVisibilityTimeoutFromObjectSize(final long objectSizeBytes) {
        // simple implementation to allocate 30 seconds per 0.25 GB of object, which means a minimum of 30 seconds added
        final int visibilityTimeoutSeconds = (int) Math.ceil((double) objectSizeBytes / QUARTER_GB_BYTES) * VISIBILITY_TIMEOUT_SECONDS_PER_QUARTER_GB;

        return Math.min(visibilityTimeoutSeconds, MAX_VISIBILITY_TIMEOUT_SECONDS);
    }
}
