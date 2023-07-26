/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.packaging.ThreadTaskEvents;
import org.opensearch.dataprepper.plugins.sink.utils.CloudWatchLogsLimits;
import org.opensearch.dataprepper.plugins.sink.utils.SinkStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.Executors.newCachedThreadPool; //TODO: Can implement a more strict pooling method if needed.

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
    private final CloudWatchLogsClient cloudWatchLogsClient;
    private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final Buffer buffer;
    private final CloudWatchLogsLimits cloudWatchLogsLimits;
    private List<EventHandle> bufferedEventHandles;
    private final SinkStopWatch sinkStopWatch;
    private final ReentrantLock bufferLock;
    private final String logGroup;
    private final String logStream;
    private final int retryCount;
    private final long backOffTimeBase;

    public CloudWatchLogsService(final Buffer buffer,
                                 final CloudWatchLogsClient cloudWatchLogsClient,
                                 final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final String logGroup, final String logStream,
                                 final int retryCount, final long backOffTimeBase) {

        this.buffer = buffer;
        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.cloudWatchLogsLimits = cloudWatchLogsLimits;
        this.logGroup = logGroup;
        this.logStream = logStream;
        this.retryCount = retryCount;
        this.backOffTimeBase = backOffTimeBase;

        this.bufferedEventHandles = new ArrayList<>();

        bufferLock = new ReentrantLock();
        sinkStopWatch = new SinkStopWatch();

        cloudWatchLogsDispatcher = new CloudWatchLogsDispatcher(cloudWatchLogsClient,
                cloudWatchLogsMetrics, logGroup, logStream, retryCount, backOffTimeBase);
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * Implements simple conditional buffer. (Sends once batch size, request size in bytes, or time limit is reached)
     * @param logs - Collection of Record events which hold log data.
     */
    public void processLogEvents(final Collection<Record<Event>> logs) {
        sinkStopWatch.startIfNotRunning();
        for (Record<Event> log: logs) {
            String logString = log.getData().toJsonString();
            int logLength = logString.length();

            if (cloudWatchLogsLimits.isGreaterThanMaxEventSize(logLength)) {
                LOG.warn("Event blocked due to Max Size restriction! {Event Size: {} bytes}", (logLength + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
                continue;
            }

            long time = sinkStopWatch.getStopWatchTimeSeconds();

            bufferLock.lock();

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

            bufferLock.unlock();
        }
    }

    private void stageLogEvents() {
        sinkStopWatch.stopAndResetStopWatch();

        List<byte[]> eventMessageClone = buffer.getBufferedData();
        ThreadTaskEvents dataToPush = new ThreadTaskEvents(eventMessageClone, bufferedEventHandles);

        buffer.resetBuffer();
        bufferedEventHandles = new ArrayList<>();

        List<InputLogEvent> inputLogEvents = cloudWatchLogsDispatcher.prepareInputLogEvents(dataToPush);
        cloudWatchLogsDispatcher.dispatchLogs(inputLogEvents, dataToPush.getEventHandles());
    }

    private void addToBuffer(final Record<Event> log, final String logString) {
        if (log.getData().getEventHandle() != null) {
            bufferedEventHandles.add(log.getData().getEventHandle());
        }
        buffer.writeEvent(logString.getBytes());
    }

    private void cloneLists(List<byte[]> listToCopy, List<byte[]> listToCopyInto) {
        for (byte[] holder: listToCopy) {
            listToCopyInto.add(holder.clone());
        }
    }
}
