/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import io.micrometer.core.instrument.Counter;
import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.exception.RetransmissionLimitException;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

                /*TODO: Can add DLQ logic here for sending these logs to a particular DLQ for error checking. (Explicitly for bad formatted logs).
                    as currently the logs that are able to be published but rejected by CloudWatch Logs will simply be deleted if not deferred to
                    a backup storage.
                 */
//TODO: Must also consider if the customer makes the logEvent size bigger than the send request size.
//TODO: Can inject another class for the stopWatch functionality.

public class CloudWatchLogsService {
    public static final int LOG_EVENT_OVERHEAD_SIZE = 26; //Size of overhead for each log event message.
    public static final String NUMBER_OF_RECORDS_PUSHED_TO_CWL_SUCCESS = "cloudWatchLogsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_PUSHED_TO_CWL_FAIL = "cloudWatchLogsEventsFailed";
    public static final String REQUESTS_SUCCEEDED = "cloudWatchLogsRequestsSucceeded";
    public static final String REQUESTS_FAILED = "cloudWatchLogsRequestsFailed";
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsService.class);
    private final CloudWatchLogsClient cloudWatchLogsClient;
    private final Buffer buffer;
    private final ThresholdCheck thresholdCheck;
    private final List<EventHandle> bufferedEventHandles;
    private final String logGroup;
    private final String logStream;
    private final int retryCount;
    private final long backOffTimeBase;
    private final io.micrometer.core.instrument.Counter logEventSuccessCounter; //Counter to be used on the fly for counting successful transmissions. (Success per single event successfully published).
    private final Counter requestSuccessCount;
    private final io.micrometer.core.instrument.Counter logEventFailCounter;
    private final io.micrometer.core.instrument.Counter requestFailCount; //Counter to be used on the fly during error handling.
    private int failCounter = 0;
    private boolean failedPost;
    private final StopWatch stopWatch;
    private boolean stopWatchOn;
    private final ReentrantLock reentrantLock;

    public CloudWatchLogsService(final CloudWatchLogsClient cloudWatchLogsClient, final CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig, final Buffer buffer,
                                 final PluginMetrics pluginMetrics, final ThresholdCheck thresholdCheck, final int retryCount, final long backOffTimeBase) {

        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.buffer = buffer;
        this.logGroup = cloudWatchLogsSinkConfig.getLogGroup();
        this.logStream = cloudWatchLogsSinkConfig.getLogStream();
        this.thresholdCheck = thresholdCheck;

        this.retryCount = retryCount;
        this.backOffTimeBase = backOffTimeBase;

        this.bufferedEventHandles = new ArrayList<>();
        this.logEventSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_PUSHED_TO_CWL_SUCCESS);
        this.requestFailCount = pluginMetrics.counter(REQUESTS_FAILED);
        this.logEventFailCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_PUSHED_TO_CWL_FAIL);
        this.requestSuccessCount = pluginMetrics.counter(REQUESTS_SUCCEEDED);

        reentrantLock = new ReentrantLock();

        stopWatch = StopWatch.create();
        stopWatchOn = false;
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * Implements simple conditional buffer. (Sends once batch size, request size in bytes, or time limit is reached)
     * @param logs - Collection of Record events which hold log data.
     */
    public void output(final Collection<Record<Event>> logs) {
        reentrantLock.lock();

        try {
            if (!stopWatchOn) {
                startStopWatch();
            }

            for (Record<Event> singleLog: logs) {
                String logJsonString = singleLog.getData().toJsonString();
                int logLength = logJsonString.length();

                if (thresholdCheck.isGreaterThanMaxEventSize(logLength + LOG_EVENT_OVERHEAD_SIZE)) {
                    LOG.warn("Event blocked due to Max Size restriction! {Event Size: " + (logLength + LOG_EVENT_OVERHEAD_SIZE) + " bytes}");
                    continue;
                }

                int bufferSizeWithOverHead = (buffer.getBufferSize() + (buffer.getEventCount() * LOG_EVENT_OVERHEAD_SIZE));
                if ((thresholdCheck.isGreaterThanThresholdReached(getStopWatchTime(),  bufferSizeWithOverHead + logLength + LOG_EVENT_OVERHEAD_SIZE, buffer.getEventCount() + 1) && (buffer.getEventCount() > 0))) {
                    pushLogs();
                }

                if (singleLog.getData().getEventHandle() != null) {
                    bufferedEventHandles.add(singleLog.getData().getEventHandle());
                }
                buffer.writeEvent(logJsonString.getBytes());
            }

            runExitCheck();

        } catch (InterruptedException e) {
            LOG.error("Caught InterruptedException while attempting to publish logs!");
            reentrantLock.unlock();
        }
    }

    private void pushLogs() throws InterruptedException {
        LOG.info("Attempting to push logs! {Batch size: " + buffer.getEventCount() + "}");
        stopAndResetStopWatch();
        startStopWatch();

        ArrayList<InputLogEvent> logEventList = new ArrayList<>();
        failedPost = true;

        for (byte[] data: buffer.getBufferedData()) {
            InputLogEvent tempLogEvent = InputLogEvent.builder()
                    .message(new String(data))
                    .timestamp(System.currentTimeMillis())
                    .build();
            logEventList.add(tempLogEvent);
        }

        while (failedPost && (failCounter < retryCount)) {
            try {
                PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                        .logEvents(logEventList)
                        .logGroupName(logGroup)
                        .logStreamName(logStream)
                        .build();

                cloudWatchLogsClient.putLogEvents(putLogEventsRequest);

                requestSuccessCount.increment();
                failedPost = false;

                //TODO: When a log is rejected by the service, we cannot send it, can probably push to a DLQ here.

            } catch (AwsServiceException | SdkClientException e) {
                LOG.error("Failed to push logs with error: {}", e.getMessage());

                Thread.sleep(calculateBackOffTime(backOffTimeBase));

                LOG.warn("Trying to retransmit request... {Attempt: " + retryCount + "}");
                requestFailCount.increment();
                failCounter += 1;
            }
        }

        buffer.clearBuffer();

        if (failedPost) {
            logEventFailCounter.increment(logEventList.size());
            releaseEventHandles(false);
            LOG.error("Error, timed out trying to push logs!");
            throw new RetransmissionLimitException("Error, timed out trying to push logs! (Max retry_count reached: {" + retryCount + "})");
        } else {
            logEventSuccessCounter.increment(logEventList.size());
            releaseEventHandles(true);
            failCounter = 0;
        }
    }

    private long calculateBackOffTime(long backOffTimeBase) {
        return failCounter * backOffTimeBase;
    }

    private void runExitCheck() throws InterruptedException {
        int bufferSizeWithOverHead = (buffer.getBufferSize() + (buffer.getEventCount() * LOG_EVENT_OVERHEAD_SIZE));
        if (thresholdCheck.isEqualToThresholdReached(bufferSizeWithOverHead, buffer.getEventCount())) {
            pushLogs();
        }
    }

    private void releaseEventHandles(final boolean result) {
        if (bufferedEventHandles.size() == 0) {
            return;
        }

        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }

        bufferedEventHandles.clear();
    }

    private void startStopWatch() {
        stopWatchOn = true;
        stopWatch.start();
    }

    private void stopAndResetStopWatch() {
        stopWatchOn = false;
        stopWatch.stop();
        stopWatch.reset();
    }

    private long getStopWatchTime() {
        return stopWatch.getTime(TimeUnit.SECONDS);
    }
}