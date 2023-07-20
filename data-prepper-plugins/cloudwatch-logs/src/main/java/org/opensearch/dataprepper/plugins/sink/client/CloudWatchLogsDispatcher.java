/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.packaging.ThreadTaskEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class CloudWatchLogsDispatcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsDispatcher.class);
    private final BlockingQueue<ThreadTaskEvents> taskQueue;
    private final CloudWatchLogsClient cloudWatchLogsClient;
    private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final String logGroup;
    private final String logStream;
    final int retryCount;
    final long backOffTimeBase;
    public CloudWatchLogsDispatcher(final BlockingQueue<ThreadTaskEvents> taskQueue,
                                    final CloudWatchLogsClient cloudWatchLogsClient,
                                    final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                    final String logGroup, final String logStream,
                                    final int retryCount, final int backOffTimeBase) {
        this.taskQueue = taskQueue;
        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.retryCount = retryCount;
        this.backOffTimeBase = backOffTimeBase;
    }

    private List<InputLogEvent> prepareInputLogEvents(final ThreadTaskEvents eventData) {
        List<InputLogEvent> logEventList = new ArrayList<>();

        for (byte[] data: eventData.getEventMessages()) {
            InputLogEvent tempLogEvent = InputLogEvent.builder()
                    .message(new String(data))
                    .timestamp(System.currentTimeMillis())
                    .build();
            logEventList.add(tempLogEvent);
        }

        return logEventList;
    }

    /**
     * Flush function to handle the flushing of logs to CloudWatchLogs services;
     * @param inputLogEvents Collection of inputLogEvents to be flushed
     * @return true if successful, false otherwise
     */
    public boolean dispatchLogs(List<InputLogEvent> inputLogEvents, Collection<EventHandle> eventHandles) {
        boolean failedPost = true;
        int failCounter = 0;

        PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                .logEvents(inputLogEvents)
                .logGroupName(logGroup)
                .logStreamName(logStream)
                .build();

        //TODO: Could also continue to retry even with InterruptedException instead of directly pushing to DLQ.
        try {
            while (failedPost && (failCounter < retryCount)) {
                try {
                    cloudWatchLogsClient.putLogEvents(putLogEventsRequest);

                    cloudWatchLogsMetrics.increaseRequestSuccessCounter(1);
                    failedPost = false;

                    //TODO: When a log is rejected by the service, we cannot send it, can probably push to a DLQ here.

                } catch (CloudWatchLogsException | SdkClientException e) {
                    LOG.error("Failed to push logs with error: {}", e.getMessage());
                    cloudWatchLogsMetrics.increaseRequestFailCounter(1);
                    Thread.sleep(calculateBackOffTime(backOffTimeBase, failCounter));
                    LOG.warn("Trying to retransmit request... {Attempt: {} }", (++failCounter));
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Got interrupted while waiting!");
            //TODO: Push to DLQ.
        }


        if (failedPost) {
            cloudWatchLogsMetrics.increaseLogEventFailCounter(inputLogEvents.size());
            LOG.error("Error, timed out trying to push logs!");
            releaseEventHandles(false, eventHandles);
            return false;
        } else {
            cloudWatchLogsMetrics.increaseLogEventSuccessCounter(inputLogEvents.size());
            LOG.info("Succeeded in publishing logs!");
            releaseEventHandles(true, eventHandles);
            return true;
        }
    }

    //TODO: Can abstract this if clients want more choice.
    private long calculateBackOffTime(final long backOffTimeBase, final int failCounter) {
        return failCounter * backOffTimeBase;
    }

    @Override
    public void run() {
        try {
            ThreadTaskEvents taskData = taskQueue.take();
            List<InputLogEvent> inputLogEvents = prepareInputLogEvents(taskData);
            dispatchLogs(inputLogEvents, taskData.getEventHandles());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            //TODO: Implement back up to taskQueue read failure.
        }
    }

    private void releaseEventHandles(final boolean result, final Collection<EventHandle> eventHandles) {
        if (eventHandles.isEmpty()) {
            return;
        }

        for (EventHandle eventHandle : eventHandles) {
            eventHandle.release(result);
        }
    }
}
