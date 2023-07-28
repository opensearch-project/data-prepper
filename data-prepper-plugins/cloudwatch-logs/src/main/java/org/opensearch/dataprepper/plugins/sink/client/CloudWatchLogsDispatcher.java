/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import lombok.Builder;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

@Builder
public class CloudWatchLogsDispatcher {
    private static final long UPPER_RETRY_TIME_BOUND_MILLISECONDS = 2000;
    private static final float EXP_TIME_SCALE = 1.25F;
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsDispatcher.class);
    private CloudWatchLogsClient cloudWatchLogsClient;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private Executor executor;
    private String logGroup;
    private String logStream;
    private int retryCount;
    private long backOffTimeBase;
    public CloudWatchLogsDispatcher(final CloudWatchLogsClient cloudWatchLogsClient,
                                    final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                    final Executor executor,
                                    final String logGroup, final String logStream,
                                    final int retryCount, final long backOffTimeBase) {
        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.retryCount = retryCount;
        this.backOffTimeBase = backOffTimeBase;

        this.executor = executor;
    }

    /**
     * Will read in a collection of log messages in byte form and transform them into a collection of InputLogEvents.
     * @param eventMessageBytes Collection of byte arrays holding event messages.
     * @return List of InputLogEvents holding the wrapped event messages.
     */
    public List<InputLogEvent> prepareInputLogEvents(final Collection<byte[]> eventMessageBytes) {
        List<InputLogEvent> logEventList = new ArrayList<>();

        /**
         * In the current implementation, the timestamp is generated during transmission.
         * To properly extract timestamp we need to order the InputLogEvents. Can be done by
         * refactoring buffer class with timestamp param, or adding a sorting algorithm in between
         * making the PLE object (in prepareInputLogEvents).
         */

        for (byte[] data : eventMessageBytes) {
            InputLogEvent tempLogEvent = InputLogEvent.builder()
                    .message(new String(data, StandardCharsets.UTF_8))
                    .timestamp(System.currentTimeMillis())
                    .build();
            logEventList.add(tempLogEvent);
        }

        return logEventList;
    }

    public void dispatchLogs(List<InputLogEvent> inputLogEvents, Collection<EventHandle> eventHandles) {
        PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                .logEvents(inputLogEvents)
                .logGroupName(logGroup)
                .logStreamName(logStream)
                .build();

        executor.execute(Uploader.builder()
                .cloudWatchLogsClient(cloudWatchLogsClient)
                .cloudWatchLogsMetrics(cloudWatchLogsMetrics)
                .putLogEventsRequest(putLogEventsRequest)
                .eventHandles(eventHandles)
                .backOffTimeBase(backOffTimeBase)
                .retryCount(retryCount)
                .build());
    }

    @Builder
    protected static class Uploader implements Runnable {
        private final CloudWatchLogsClient cloudWatchLogsClient;
        private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
        private final PutLogEventsRequest putLogEventsRequest;
        private final Collection<EventHandle> eventHandles;
        private final int retryCount;
        private final long backOffTimeBase;

        @Override
        public void run() {
            upload();
        }

        public void upload() {
            boolean failedToTransmit = true;
            int failCount = 0;

            try {
                while (failedToTransmit && (failCount < retryCount)) {
                    try {
                        cloudWatchLogsClient.putLogEvents(putLogEventsRequest);

                        cloudWatchLogsMetrics.increaseRequestSuccessCounter(1);
                        failedToTransmit = false;

                    } catch (CloudWatchLogsException | SdkClientException e) {
                        LOG.error("Failed to push logs with error: {}", e.getMessage());
                        cloudWatchLogsMetrics.increaseRequestFailCounter(1);
                        Thread.sleep(calculateBackOffTime(backOffTimeBase, failCount));
                        failCount++;
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("Got interrupted while waiting!");
                //TODO: Push to DLQ.
                Thread.currentThread().interrupt();
            }


            if (failedToTransmit) {
                cloudWatchLogsMetrics.increaseLogEventFailCounter(eventHandles.size());
                releaseEventHandles(false, eventHandles);
            } else {
                cloudWatchLogsMetrics.increaseLogEventSuccessCounter(eventHandles.size());
                releaseEventHandles(true, eventHandles);
            }
        }

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
