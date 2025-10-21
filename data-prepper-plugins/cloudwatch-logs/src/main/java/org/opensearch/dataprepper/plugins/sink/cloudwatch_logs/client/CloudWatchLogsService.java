/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.SinkStopWatch;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsSinkUtils;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.failures.DlqObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CloudWatchLogs Service encapsulates the log processing step.
 * It accomplishes this by:
 * <ol>
 *   <li>Reading in log events.</li>
 *   <li>Buffering data.</li>
 *   <li>Checking for limit conditions.</li>
 *   <li>Making PLE calls to CloudWatchLogs.</li>
 * </ol>
 */
public class CloudWatchLogsService {
    private final CloudWatchLogsDispatcher cloudWatchLogsDispatcher;
    private final Buffer buffer;
    private final CloudWatchLogsLimits cloudWatchLogsLimits;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final SinkStopWatch sinkStopWatch;
    private final ReentrantLock processLock;
    private final DlqPushHandler dlqPushHandler;
    private final boolean dropIfDlqNotConfigured;
    public CloudWatchLogsService(final Buffer buffer,
                                 final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final CloudWatchLogsDispatcher cloudWatchLogsDispatcher,
                                 final DlqPushHandler dlqPushHandler,
                                 final boolean dropIfDlqNotConfigured) {

        this.buffer = buffer;
        this.cloudWatchLogsLimits = cloudWatchLogsLimits;

        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        processLock = new ReentrantLock();
        sinkStopWatch = new SinkStopWatch();

        this.cloudWatchLogsDispatcher = cloudWatchLogsDispatcher;
        this.dlqPushHandler = dlqPushHandler;
        this.dropIfDlqNotConfigured = dropIfDlqNotConfigured;
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * @param logs Collection of Record events.
     */
    public void processLogEvents(final Collection<Record<Event>> logs) {
        sinkStopWatch.startIfNotRunning();
        if (logs.isEmpty() && buffer.getEventCount() > 0) {
            processLock.lock();
            try {
                if (cloudWatchLogsLimits.isTimeLimitReached(sinkStopWatch.getElapsedTimeInSeconds())) {
                    stageLogEvents();
                }
            } finally {
                processLock.unlock();
            }
            return;
        }

        final List<DlqObject> dlqObjects = new ArrayList<>();
        for (Record<Event> log : logs) {
            String logString = log.getData().toJsonString();
            int logLength = logString.length();

            cloudWatchLogsMetrics.recordLogSize(logLength);
            if (cloudWatchLogsLimits.isGreaterThanMaxEventSize(logLength)) {
                final String failureMessage = String.format("Event blocked due to Max Size restriction! Event Size : %s", (logLength + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
                DlqObject dlqObject = CloudWatchLogsSinkUtils.createDlqObject(0, log.getData().getEventHandle(), logString, failureMessage, dlqPushHandler, dropIfDlqNotConfigured);
                if (dlqObject != null) {
                    dlqObjects.add(dlqObject);
                } else if (dropIfDlqNotConfigured) {
                    cloudWatchLogsMetrics.increaseLogLargeEventsDroppedCounter(1);
                }
                continue;
            }

            long time = sinkStopWatch.getElapsedTimeInSeconds();

            processLock.lock();
            try {
                int bufferSize = buffer.getBufferSize();
                int bufferEventCount = buffer.getEventCount();
                if (cloudWatchLogsLimits.maxRequestSizeLimitExceeds(logLength + bufferSize, bufferEventCount+1)) {
                    stageLogEvents();
                }
                addToBuffer(log.getData().getEventHandle(), logString);
                bufferEventCount = buffer.getEventCount();
                if (cloudWatchLogsLimits.isMaxEventCountLimitReached(bufferEventCount)) {
                    stageLogEvents();
                }

            } finally {
                processLock.unlock();
            }
        }
        CloudWatchLogsSinkUtils.handleDlqObjects(dlqObjects, dlqPushHandler);
    }

    private void stageLogEvents() {
        sinkStopWatch.stopAndReset();

        List<InputLogEvent> inputLogEvents = cloudWatchLogsDispatcher.prepareInputLogEvents(buffer.getBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEvents, buffer.getEventHandles());
        cloudWatchLogsMetrics.recordRequestSize(buffer.getBufferSize());

        buffer.resetBuffer();
    }

    private void addToBuffer(final EventHandle logEventHandle, final String logString) {
        buffer.writeEvent(logEventHandle, logString.getBytes(StandardCharsets.UTF_8));
    }
}
