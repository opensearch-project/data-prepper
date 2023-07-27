/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.utils.CloudWatchLogsLimits;
import org.opensearch.dataprepper.plugins.sink.utils.SinkStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CloudWatchLogs Service encapsulates the log processing step.
 * It accomplishes this by:
 * 1. Reading in log events.
 * 2. Buffering data.
 * 3. Checking for limit conditions.
 * 4. Making PLE calls to CloudWatchLogs.
 */
public class CloudWatchLogsService {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsService.class);
    private final CloudWatchLogsDispatcher cloudWatchLogsDispatcher;
    private final Buffer buffer;
    private final CloudWatchLogsLimits cloudWatchLogsLimits;
    private List<EventHandle> bufferedEventHandles;
    private final SinkStopWatch sinkStopWatch;
    private final ReentrantLock processLock;
    public CloudWatchLogsService(final Buffer buffer,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final CloudWatchLogsDispatcher cloudWatchLogsDispatcher) {

        this.buffer = buffer;
        this.cloudWatchLogsLimits = cloudWatchLogsLimits;
        this.bufferedEventHandles = new ArrayList<>();

        processLock = new ReentrantLock();
        sinkStopWatch = new SinkStopWatch();

        this.cloudWatchLogsDispatcher = cloudWatchLogsDispatcher;
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * Implements simple conditional buffer. (Sends once batch size, request size in bytes, or time limit is reached)
     * @param logs - Collection of Record events which hold log data.
     */
    public void processLogEvents(final Collection<Record<Event>> logs) {
        try {
            sinkStopWatch.startIfNotRunning();
            for (Record<Event> log : logs) {
                String logString = log.getData().toJsonString();
                int logLength = logString.length();

                if (cloudWatchLogsLimits.isGreaterThanMaxEventSize(logLength)) {
                    LOG.warn("Event blocked due to Max Size restriction! {Event Size: {} bytes}", (logLength + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
                    continue;
                }

                long time = sinkStopWatch.getStopWatchTimeSeconds();

                processLock.lock();
                int bufferSize = buffer.getBufferSize();
                int bufferEventCount = buffer.getEventCount();
                int bufferEventCountWithEvent = bufferEventCount + 1;
                int bufferSizeWithAddedEvent = bufferSize + logLength;

                if ((cloudWatchLogsLimits.isGreaterThanLimitReached(time, bufferSizeWithAddedEvent, bufferEventCountWithEvent) && (bufferEventCount > 0))) {
                    stageLogEvents();
                    addToBuffer(log, logString);
                } else if (cloudWatchLogsLimits.isEqualToLimitReached(bufferSizeWithAddedEvent, bufferEventCountWithEvent)) {
                    addToBuffer(log, logString);
                    stageLogEvents();
                } else {
                    addToBuffer(log, logString);
                }
            }
        } finally {
            processLock.unlock();
        }
    }

    private void stageLogEvents() {
        sinkStopWatch.stopAndResetStopWatch();

        List<InputLogEvent> inputLogEvents = cloudWatchLogsDispatcher.prepareInputLogEvents(buffer.getBufferedData());
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEvents, bufferedEventHandles);

        buffer.resetBuffer();
        bufferedEventHandles = new ArrayList<>();
    }

    private void addToBuffer(final Record<Event> log, final String logString) {
        if (log.getData().getEventHandle() != null) {
            bufferedEventHandles.add(log.getData().getEventHandle());
        }
        buffer.writeEvent(logString.getBytes(StandardCharsets.UTF_8));
    }
}
