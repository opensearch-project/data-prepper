/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.config.CloudWatchLogsSinkConfig;
import org.opensearch.dataprepper.plugins.sink.exception.RetransmissionLimitException;
import org.opensearch.dataprepper.plugins.sink.threshold.ThresholdCheck;
import org.opensearch.dataprepper.plugins.sink.utils.LogPusher;
import org.opensearch.dataprepper.plugins.sink.utils.SinkStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/*
   TODO: Can add DLQ logic here for sending these logs to a particular DLQ for error checking. (Explicitly for bad formatted logs).
    as currently the logs that are able to be published but rejected by CloudWatch Logs will simply be deleted if not deferred to
    a backup storage.
*/
//TODO: Must also consider if the customer makes the logEvent size bigger than the send request size.
//TODO: Can inject another class for the stopWatch functionality.

public class CloudWatchLogsService {
    public static final int APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE = 26; //Size of overhead for each log event message.
    public static final String CLOUDWATCH_LOGS_EVENTS_SUCCEEDED = "cloudWatchLogsEventsSucceeded";
    public static final String CLOUDWATCH_LOGS_EVENTS_FAILED = "cloudWatchLogsEventsFailed";
    public static final String CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED = "cloudWatchLogsRequestsSucceeded";
    public static final String CLOUDWATCH_LOGS_REQUESTS_FAILED = "cloudWatchLogsRequestsFailed";
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsService.class);
    private static final int RETRY_THREAD_ERROR_CAP = 3;
    private final CloudWatchLogsClient cloudWatchLogsClient;
    private final Buffer buffer;
    private final ThresholdCheck thresholdCheck;
    private final List<EventHandle> bufferedEventHandles;
    private final String logGroup;
    private final String logStream;
    private final int retryCount;
    private final long backOffTimeBase;
    private final Counter logEventSuccessCounter; //Counter to be used on the fly for counting successful transmissions. (Success per single event successfully published).
    private final Counter requestSuccessCount;
    private final Counter logEventFailCounter;
    private final Counter requestFailCount; //Counter to be used on the fly during error handling.
    private final SinkStopWatch sinkStopWatch;
    private final ReentrantLock reentrantLock;
    private final LogPusher logPusher;

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
        this.logEventSuccessCounter = pluginMetrics.counter(CLOUDWATCH_LOGS_EVENTS_SUCCEEDED);
        this.requestFailCount = pluginMetrics.counter(CLOUDWATCH_LOGS_REQUESTS_FAILED);
        this.logEventFailCounter = pluginMetrics.counter(CLOUDWATCH_LOGS_EVENTS_FAILED);
        this.requestSuccessCount = pluginMetrics.counter(CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED);

        reentrantLock = new ReentrantLock();

        sinkStopWatch = new SinkStopWatch();

        this.logPusher = new LogPusher(logEventSuccessCounter, logEventFailCounter, requestSuccessCount, requestFailCount, retryCount, backOffTimeBase);
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * Implements simple conditional buffer. (Sends once batch size, request size in bytes, or time limit is reached)
     * @param logs - Collection of Record events which hold log data.
     */
    public void output(final Collection<Record<Event>> logs) {
        reentrantLock.lock();

        int threadRetries = 0;
        boolean processedLogsSuccessfully = false;

        while (threadRetries < RETRY_THREAD_ERROR_CAP) {
            processedLogsSuccessfully = processLogEvents(logs);

            if (processedLogsSuccessfully) {
                threadRetries = RETRY_THREAD_ERROR_CAP;
            } else {
                LOG.error("Thread threw InterruptedException!");
                threadRetries++;
            }
        }

        if (processedLogsSuccessfully) {
            LOG.info("Successfully processed logs.");
        } else {
            LOG.warn("Failed to process logs.");
            //TODO: Insert DLQ logic as a last resort if we cannot manage to process logs prior to this point.
        }

        reentrantLock.unlock();
    }

    private boolean processLogEvents(final Collection<Record<Event>> logs) {
        try {
            sinkStopWatch.startIfNotRunning();

            for (Record<Event> log: logs) {
                String logJsonString = log.getData().toJsonString();
                int logLength = logJsonString.length();

                if (thresholdCheck.isGreaterThanMaxEventSize(logLength + APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE)) {
                    LOG.warn("Event blocked due to Max Size restriction! {Event Size: " + (logLength + APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE) + " bytes}");
                    continue;
                }

                int bufferSizeWithOverhead = (buffer.getBufferSize() + (buffer.getEventCount() * APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
                if ((thresholdCheck.isGreaterThanThresholdReached(sinkStopWatch.getStopWatchTimeSeconds(),  bufferSizeWithOverhead + logLength + APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE, buffer.getEventCount() + 1) && (buffer.getEventCount() > 0))) {
                    pushLogs();
                }

                if (log.getData().getEventHandle() != null) {
                    bufferedEventHandles.add(log.getData().getEventHandle());
                }
                buffer.writeEvent(logJsonString.getBytes());
            }

            runExitCheck();

            return true;
        } catch (InterruptedException e) {
            LOG.error("Caught InterruptedException while attempting to publish logs!");
            return false;
        }
    }

    private void pushLogs() throws InterruptedException {
        LOG.info("Attempting to push logs! {Batch size: " + buffer.getEventCount() + "}");
        sinkStopWatch.stopAndResetStopWatch();
        sinkStopWatch.startStopWatch();

        boolean succeededTransmission = logPusher.pushLogs(buffer, cloudWatchLogsClient, logGroup, logStream);
        releaseEventHandles(succeededTransmission);

        if (!succeededTransmission) {
            throw new RetransmissionLimitException("Error, timed out trying to push logs! (Max retry_count reached: {" + retryCount + "})");
        }
    }

    private void runExitCheck() throws InterruptedException {
        int bufferSizeWithOverHead = (buffer.getBufferSize() + (buffer.getEventCount() * APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
        if ((thresholdCheck.isEqualToThresholdReached(bufferSizeWithOverHead, buffer.getEventCount()) && (buffer.getEventCount() > 0))) {
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
}