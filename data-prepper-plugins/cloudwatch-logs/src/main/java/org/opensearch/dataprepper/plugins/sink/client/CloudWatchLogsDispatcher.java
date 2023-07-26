/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import lombok.Builder;
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
import java.util.concurrent.Executor;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class CloudWatchLogsDispatcher {
    private static final long UPPER_RETRY_TIME_BOUND_MILLISECONDS = 5000;
    private static final float EXP_TIME_SCALE = 1.5F;
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsDispatcher.class);
    private CloudWatchLogsClient cloudWatchLogsClient;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private Executor asyncExecutor;
    private String logGroup;
    private String logStream;
    private int retryCount;
    private long backOffTimeBase;
    public CloudWatchLogsDispatcher(final CloudWatchLogsClient cloudWatchLogsClient,
                                    final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                    final String logGroup, final String logStream,
                                    final int retryCount, final long backOffTimeBase) {
        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.retryCount = retryCount;
        this.backOffTimeBase = backOffTimeBase;

        asyncExecutor = newCachedThreadPool();
    }

    public List<InputLogEvent> prepareInputLogEvents(final ThreadTaskEvents eventData) {
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
     * @param eventHandles Collection of EventHandles for events
     */
    public void dispatchLogs(List<InputLogEvent> inputLogEvents, Collection<EventHandle> eventHandles) {
        PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                .logEvents(inputLogEvents)
                .logGroupName(logGroup)
                .logStreamName(logStream)
                .build();

        asyncExecutor.execute(Uploader.builder().cloudWatchLogsClient(cloudWatchLogsClient)
                .cloudWatchLogsMetrics(cloudWatchLogsMetrics)
                .putLogEventsRequest(putLogEventsRequest)
                .eventHandles(eventHandles)
                .backOffTimeBase(backOffTimeBase)
                .retryCount(retryCount)
                .build());
    }

    @Builder
    private static class Uploader implements Runnable {
        private CloudWatchLogsClient cloudWatchLogsClient;
        private CloudWatchLogsMetrics cloudWatchLogsMetrics;
        private PutLogEventsRequest putLogEventsRequest;
        private Collection<EventHandle> eventHandles;
        private int retryCount;
        private long backOffTimeBase;

        @Override
        public void run() {
            upload();
        }

        public void upload() {
            boolean failedPost = true;
            int failCounter = 0;

            try {
                while (failedPost && (failCounter < retryCount)) {
                    try {
                        cloudWatchLogsClient.putLogEvents(putLogEventsRequest);

                        cloudWatchLogsMetrics.increaseRequestSuccessCounter(1);
                        failedPost = false;

                        //TODO: When a log is rejected by the service, we cannot send it, can probably push to a DLQ here.

                    } catch (CloudWatchLogsException | SdkClientException e) {
                        LOG.error("Service-Worker {} Failed to push logs with error: {}", Thread.currentThread().getName(), e.getMessage());
                        cloudWatchLogsMetrics.increaseRequestFailCounter(1);
                        Thread.sleep(calculateBackOffTime(backOffTimeBase, failCounter));
                        LOG.warn("Service-Worker {} Trying to retransmit request... {Attempt: {} }", Thread.currentThread().getName(), (++failCounter));
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("Got interrupted while waiting!");
                //TODO: Push to DLQ.
                Thread.currentThread().interrupt();
            }


            if (failedPost) {
                cloudWatchLogsMetrics.increaseLogEventFailCounter(eventHandles.size());
                releaseEventHandles(false, eventHandles);
            } else {
                cloudWatchLogsMetrics.increaseLogEventSuccessCounter(eventHandles.size());
                releaseEventHandles(true, eventHandles);
            }
        }

        //TODO: Can abstract this if clients want more choice.
        private long calculateBackOffTime(final long backOffTimeBase, final int failCounter) {
            long scale = (long)Math.pow(EXP_TIME_SCALE, failCounter);

            if (scale >= UPPER_RETRY_TIME_BOUND_MILLISECONDS) {
                return UPPER_RETRY_TIME_BOUND_MILLISECONDS;
            }

            return scale * backOffTimeBase;
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
}
