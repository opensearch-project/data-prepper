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
 * <ol>
 *   <li>Reading in log events.</li>
 *   <li>Buffering data.</li>
 *   <li>Checking for limit conditions.</li>
 *   <li>Making PLE calls to CloudWatchLogs.</li>
 * </ol>
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

        bufferedEventHandles = new ArrayList<>();

        processLock = new ReentrantLock();
        sinkStopWatch = new SinkStopWatch();

        this.cloudWatchLogsDispatcher = cloudWatchLogsDispatcher;
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * @param logs Collection of Record events.
     */
    public void processLogEvents(final Collection<Record<Event>> logs) {
            sinkStopWatch.startIfNotRunning();
            for (Record<Event> log : logs) {
                String logString = log.getData().toJsonString();
                int logLength = logString.length();

                if (cloudWatchLogsLimits.isGreaterThanMaxEventSize(logLength)) {
                    LOG.warn("Event blocked due to Max Size restriction! {Event Size: {} bytes}", (logLength + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
                    continue;
                }

                long time = sinkStopWatch.getElapsedTimeInSeconds();

                processLock.lock();
                try {
                    int bufferSize = buffer.getBufferSize();
                    int bufferEventCount = buffer.getEventCount();
                    int newBufferEventCount = bufferEventCount + 1;
                    int newBufferSizeCount = bufferSize + logLength;

                    if ((cloudWatchLogsLimits.isGreaterThanLimitReached(time, newBufferSizeCount, newBufferEventCount) && (bufferEventCount > 0))) {
                        stageLogEvents();
                        addToBuffer(log, logString);
                    } else if (cloudWatchLogsLimits.isEqualToLimitReached(newBufferSizeCount, newBufferEventCount)) {
                        addToBuffer(log, logString);
                        stageLogEvents();
                    } else {
                        addToBuffer(log, logString);
                    }
                } finally {
                    processLock.unlock();
                }
            }
    }

    private void stageLogEvents() {
        sinkStopWatch.stopAndReset();

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
