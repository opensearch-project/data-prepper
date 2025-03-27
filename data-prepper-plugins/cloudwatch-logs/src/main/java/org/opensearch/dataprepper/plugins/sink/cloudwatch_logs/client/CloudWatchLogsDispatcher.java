/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import lombok.Builder;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsSinkUtils;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.failures.DlqObject;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

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
    private DlqPushHandler dlqPushHandler;
    private Executor executor;
    private String logGroup;
    private String logStream;
    private int retryCount;
    private long backOffTimeBase;
    public CloudWatchLogsDispatcher(final CloudWatchLogsClient cloudWatchLogsClient,
                                    final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                    final DlqPushHandler dlqPushHandler,
                                    final Executor executor,
                                    final String logGroup, final String logStream,
                                    final int retryCount, final long backOffTimeBase) {
        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.retryCount = retryCount;
        this.backOffTimeBase = backOffTimeBase;
        this.dlqPushHandler = dlqPushHandler;

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

    public void dispatchLogs(List<InputLogEvent> inputLogEvents, List<EventHandle> eventHandles) {
        PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                .logEvents(inputLogEvents)
                .logGroupName(logGroup)
                .logStreamName(logStream)
                .build();

        executor.execute(Uploader.builder()
                .cloudWatchLogsClient(cloudWatchLogsClient)
                .cloudWatchLogsMetrics(cloudWatchLogsMetrics)
                .dlqPushHandler(dlqPushHandler)
                .putLogEventsRequest(putLogEventsRequest)
                .eventHandles(eventHandles)
                .totalEventCount(inputLogEvents.size())
                .backOffTimeBase(backOffTimeBase)
                .retryCount(retryCount)
                .build());
    }

    @Builder
    protected static class Uploader implements Runnable {
        private final CloudWatchLogsClient cloudWatchLogsClient;
        private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
        private DlqPushHandler dlqPushHandler;
        private final PutLogEventsRequest putLogEventsRequest;
        private final List<EventHandle> eventHandles;
        private final int totalEventCount;
        private final int retryCount;
        private final long backOffTimeBase;

        @Override
        public void run() {
            upload();
        }

        public void upload() {
            boolean failedToTransmit = true;
            int failCount = 0;
            String failureMessage = "";
            PutLogEventsResponse putLogEventsResponse = null;
            List<DlqObject> dlqObjects = new ArrayList<>();

            try {
                while (failedToTransmit && (failCount < retryCount)) {
                    try {
                        putLogEventsResponse = cloudWatchLogsClient.putLogEvents(putLogEventsRequest);
                        cloudWatchLogsMetrics.increaseRequestSuccessCounter(1);
                        failedToTransmit = false;

                    } catch (CloudWatchLogsException | SdkClientException e) {
                        failureMessage = e.getMessage();
                        LOG.error(NOISY, "Failed to push logs with error: {}", e.getMessage());
                        cloudWatchLogsMetrics.increaseRequestFailCounter(1);
                        Thread.sleep(calculateBackOffTime(backOffTimeBase, failCount));
                        failCount++;
                    }
                }
            } catch (Exception e) {
                failureMessage = e.getMessage();
                LOG.warn(NOISY, "Uploader Thread got interrupted during retransmission with exception: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }


            if (failedToTransmit) {
                cloudWatchLogsMetrics.increaseLogEventFailCounter(totalEventCount);
                List<InputLogEvent> logEvents = putLogEventsRequest.logEvents();
                for (int i = 0; i < logEvents.size(); i++) {
                    DlqObject dlqObject = CloudWatchLogsSinkUtils.createDlqObject(0, eventHandles.get(i), logEvents.get(i).message(), failureMessage, dlqPushHandler);
                    if (dlqObject != null) {
                        dlqObjects.add(dlqObject);
                    }
                }
            } else {
                if (putLogEventsResponse != null) {
                    dlqObjects = getDlqObjectsFromResponse(putLogEventsResponse);
                }
                cloudWatchLogsMetrics.increaseLogEventSuccessCounter(totalEventCount - dlqObjects.size());
            }
            CloudWatchLogsSinkUtils.handleDlqObjects(dlqObjects, dlqPushHandler);
        }

        List<DlqObject> getDlqObjectsFromResponse(PutLogEventsResponse putLogEventsResponse) {
            List<DlqObject> dlqObjects = new ArrayList<>();
            RejectedLogEventsInfo rejectedLogEventsInfo = putLogEventsResponse.rejectedLogEventsInfo();
            List<InputLogEvent> logEvents = putLogEventsRequest.logEvents();
            List<InputLogEvent> failedLogEvents = new ArrayList<>();
            if (rejectedLogEventsInfo != null) {
                Integer endIndex = rejectedLogEventsInfo.tooOldLogEventEndIndex();
                if (endIndex != null) {
                    int i = 0;
                    for (InputLogEvent logEvent : logEvents.subList(0, endIndex)) {
                        DlqObject dlqObject = CloudWatchLogsSinkUtils.createDlqObject(0, eventHandles.get(i), logEvent.message(), "Too old log event", dlqPushHandler);
                        if (dlqObject != null) {
                            dlqObjects.add(dlqObject);
                        }
                        i++;
                    }
                }
                Integer startIndex = rejectedLogEventsInfo.tooNewLogEventStartIndex();
                if (startIndex != null) {
                    int i = 0;
                    for (InputLogEvent logEvent : logEvents.subList(startIndex, logEvents.size())) {
                        DlqObject dlqObject = CloudWatchLogsSinkUtils.createDlqObject(0, eventHandles.get(startIndex + i), logEvent.message(), "Too old log event", dlqPushHandler);
                        if (dlqObject != null) {
                            dlqObjects.add(dlqObject);
                        }
                        i++;
                    }
                }
            }
            return dlqObjects;
        }

        private long calculateBackOffTime(final long backOffTimeBase, final int failCounter) {
            long scale = (long)Math.pow(EXP_TIME_SCALE, failCounter);

            if (scale >= UPPER_RETRY_TIME_BOUND_MILLISECONDS) {
                return UPPER_RETRY_TIME_BOUND_MILLISECONDS;
            }

            return scale * backOffTimeBase;
        }

    }
}
