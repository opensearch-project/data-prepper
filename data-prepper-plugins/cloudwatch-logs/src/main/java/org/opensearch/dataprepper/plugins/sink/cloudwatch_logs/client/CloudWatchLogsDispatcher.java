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
import com.linecorp.armeria.client.retry.Backoff;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsSinkUtils;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.failures.DlqObject;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

@Builder
public class CloudWatchLogsDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsDispatcher.class);
    private CloudWatchLogsClient cloudWatchLogsClient;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private DlqPushHandler dlqPushHandler;
    private boolean dropIfDlqNotConfigured;
    private Executor executor;
    private String logGroup;
    private String logStream;
    private int retryCount;
    private boolean createLogGroupAndStream;

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
                .dropIfDlqNotConfigured(dropIfDlqNotConfigured)
                .totalEventCount(inputLogEvents.size())
                .retryCount(retryCount)
                .createLogGroupAndStream(createLogGroupAndStream)
                .build());
    }

    @Builder
    protected static class Uploader implements Runnable {
        static final long INITIAL_DELAY_MS = 50;
        static final int MULTIPLE_FAILURES_METRIC_COUNT = 5;
        static final long MAXIMUM_DELAY_MS = Duration.ofMinutes(10).toMillis();
        private final CloudWatchLogsClient cloudWatchLogsClient;
        private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
        private DlqPushHandler dlqPushHandler;
        private final PutLogEventsRequest putLogEventsRequest;
        private final List<EventHandle> eventHandles;
        private final int totalEventCount;
        private final int retryCount;
        private boolean dropIfDlqNotConfigured;
        private final boolean createLogGroupAndStream;

        @Override
        public void run() {
            upload();
        }

        public void upload() {
            boolean failedToTransmit = true;
            boolean resourceCreationAttempted = false;
            int failCount = 0;
            String failureMessage = "";
            PutLogEventsResponse putLogEventsResponse = null;
            List<DlqObject> dlqObjects = new ArrayList<>();
            final Backoff backoff = Backoff.exponential(INITIAL_DELAY_MS, MAXIMUM_DELAY_MS).withMaxAttempts(retryCount);

            try {
                while (failedToTransmit && (failCount < retryCount)) {
                    try {
                        putLogEventsResponse = cloudWatchLogsClient.putLogEvents(putLogEventsRequest);
                        cloudWatchLogsMetrics.increaseRequestSuccessCounter(1);
                        failedToTransmit = false;

                    } catch (ResourceNotFoundException e) {
                        // Must be caught before CloudWatchLogsException since RNF extends CWLException.
                        if (createLogGroupAndStream && !resourceCreationAttempted) {
                            resourceCreationAttempted = true;
                            createLogGroupAndStream();
                            // Loop continues; next iteration retries PLE without incrementing failCount.
                            // If PLE still throws RNF, the guard sends us to the else branch and
                            // normal retry/DLQ logic takes over.
                        } else {
                            failureMessage = e.getMessage();
                            failCount = handlePutLogEventsFailure(e, failCount, backoff);
                        }
                    } catch (CloudWatchLogsException | SdkClientException e) {
                        failureMessage = e.getMessage();
                        failCount = handlePutLogEventsFailure(e, failCount, backoff);
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
                    DlqObject dlqObject = CloudWatchLogsSinkUtils.createDlqObject(0, eventHandles.get(i), logEvents.get(i).message(), failureMessage, dlqPushHandler, dropIfDlqNotConfigured);
                    if (dlqObject != null) {
                        dlqObjects.add(dlqObject);
                    }
                }
            } else {
                if (putLogEventsResponse != null) {
                    dlqObjects = getDlqObjectsFromResponse(putLogEventsResponse);
                }
                cloudWatchLogsMetrics.increaseLogEventSuccessCounter(totalEventCount - dlqObjects.size());
                releaseEventHandles(putLogEventsResponse);
            }
            CloudWatchLogsSinkUtils.handleDlqObjects(dlqObjects, dlqPushHandler);
        }

        /**
         * Logs the failure, increments fail metrics, and sleeps using the backoff schedule.
         * Returns the new fail count so the caller can update its local. Extracted so the
         * RNF-fallback branch and the generic CWL/SDK catch don't drift apart.
         */
        private int handlePutLogEventsFailure(final Exception e, final int currentFailCount, final Backoff backoff)
                throws InterruptedException {
            LOG.error(NOISY, "Failed to push logs with error: {}", e.getMessage());
            cloudWatchLogsMetrics.increaseRequestFailCounter(1);
            final int newFailCount = currentFailCount + 1;
            if (newFailCount % MULTIPLE_FAILURES_METRIC_COUNT == 0) {
                cloudWatchLogsMetrics.increaseRequestMultiFailCounter(1);
            }
            final long delayMillis = backoff.nextDelayMillis(newFailCount);
            if (delayMillis > 0) {
                Thread.sleep(delayMillis);
            }
            return newFailCount;
        }

        /**
         * Attempts to create the configured log group and log stream. The helper never throws —
         * all SDK exceptions are caught inside so that a recovery failure does not interrupt the
         * Uploader. ResourceAlreadyExistsException is intentionally swallowed to make creation
         * idempotent. If creation fails, the next PutLogEvents call will hit ResourceNotFoundException
         * again and the guard in upload() will route it to the normal retry/DLQ path.
         */
        private void createLogGroupAndStream() {
            final String logGroupName = putLogEventsRequest.logGroupName();
            final String logStreamName = putLogEventsRequest.logStreamName();

            try {
                cloudWatchLogsClient.createLogGroup(
                        CreateLogGroupRequest.builder().logGroupName(logGroupName).build());
                LOG.info("Created log group: {}", logGroupName);
            } catch (ResourceAlreadyExistsException e) {
                LOG.debug("Log group already exists: {}", logGroupName);
            } catch (CloudWatchLogsException | SdkClientException e) {
                LOG.warn("Unable to create log group '{}': {}", logGroupName, e.getMessage());
            }

            try {
                cloudWatchLogsClient.createLogStream(
                        CreateLogStreamRequest.builder()
                                .logGroupName(logGroupName)
                                .logStreamName(logStreamName)
                                .build());
                LOG.info("Created log stream: {}/{}", logGroupName, logStreamName);
            } catch (ResourceAlreadyExistsException e) {
                LOG.debug("Log stream already exists: {}/{}", logGroupName, logStreamName);
            } catch (CloudWatchLogsException | SdkClientException e) {
                LOG.warn("Unable to create log stream '{}/{}': {}", logGroupName, logStreamName, e.getMessage());
            }
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
                        DlqObject dlqObject = CloudWatchLogsSinkUtils.createDlqObject(0, eventHandles.get(i), logEvent.message(), "Too old log event", dlqPushHandler, dropIfDlqNotConfigured);
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
                        DlqObject dlqObject = CloudWatchLogsSinkUtils.createDlqObject(0, eventHandles.get(startIndex + i), logEvent.message(), "Too old log event", dlqPushHandler, dropIfDlqNotConfigured);
                        if (dlqObject != null) {
                            dlqObjects.add(dlqObject);
                        }
                        i++;
                    }
                }
            }
            return dlqObjects;
        }

        private void releaseEventHandles(final PutLogEventsResponse putLogEventsResponse) {
            if (putLogEventsResponse == null || putLogEventsResponse.rejectedLogEventsInfo() == null) {
                eventHandles.forEach(eventHandle -> eventHandle.release(true));
                return;
            }

            final Integer tooOldEndIndex = putLogEventsResponse.rejectedLogEventsInfo().tooOldLogEventEndIndex();
            final Integer tooNewStartIndex = putLogEventsResponse.rejectedLogEventsInfo().tooNewLogEventStartIndex();

            for (int i = 0; i < eventHandles.size(); i++) {
                boolean isRejected = (tooOldEndIndex != null && i < tooOldEndIndex) ||
                        (tooNewStartIndex != null && i >= tooNewStartIndex);

                if (!isRejected) {
                    eventHandles.get(i).release(true);
                }
            }
        }
    }
}
