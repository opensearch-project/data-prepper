/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.SinkStopWatch;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsSinkUtils;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.failures.DlqObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    static final String STATIC_GROUP_KEY = "";

    private final CloudWatchLogsDispatcher cloudWatchLogsDispatcher;
    private final BufferFactory bufferFactory;
    private final CloudWatchLogsLimits cloudWatchLogsLimits;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final ReentrantLock processLock;
    private final DlqPushHandler dlqPushHandler;
    private final boolean dropIfDlqNotConfigured;

    // Dynamic-entity configuration (null/false in static mode).
    private final boolean dynamic;
    private final EntityResolver entityResolver;

    private final Map<String, LogGroupBuffer> groups = new LinkedHashMap<>();

    // Static-entity constructor. Preserves the original behavior: a single buffer and one entity for each request
    public CloudWatchLogsService(final Buffer buffer,
                                 final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final CloudWatchLogsDispatcher cloudWatchLogsDispatcher,
                                 final DlqPushHandler dlqPushHandler,
                                 final boolean dropIfDlqNotConfigured) {
        this(new SingleBufferFactory(buffer), cloudWatchLogsMetrics, cloudWatchLogsLimits,
                cloudWatchLogsDispatcher, dlqPushHandler, dropIfDlqNotConfigured, null);
    }

    public CloudWatchLogsService(final BufferFactory bufferFactory,
                                 final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final CloudWatchLogsDispatcher cloudWatchLogsDispatcher,
                                 final DlqPushHandler dlqPushHandler,
                                 final boolean dropIfDlqNotConfigured,
                                 final EntityResolver entityResolver) {
        this.bufferFactory = bufferFactory;
        this.cloudWatchLogsLimits = cloudWatchLogsLimits;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.processLock = new ReentrantLock();
        this.cloudWatchLogsDispatcher = cloudWatchLogsDispatcher;
        this.dlqPushHandler = dlqPushHandler;
        this.dropIfDlqNotConfigured = dropIfDlqNotConfigured;
        this.entityResolver = entityResolver;
        this.dynamic = entityResolver != null;
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * @param logs Collection of Record events.
     */
    public void processLogEvents(final Collection<Record<Event>> logs) {
        if (logs.isEmpty()) {
            processLock.lock();
            try {
                flushIdleGroups();
            } finally {
                processLock.unlock();
            }
            return;
        }

        final List<DlqObject> dlqObjects = new ArrayList<>();
        for (Record<Event> log : logs) {
            final Event event = log.getData();
            String logString = event.toJsonString();
            int logLength = logString.length();

            cloudWatchLogsMetrics.recordLogSize(logLength);
            if (cloudWatchLogsLimits.isGreaterThanMaxEventSize(logLength)) {
                final String failureMessage = String.format("Event blocked due to Max Size restriction! Event Size : %s", (logLength + CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
                DlqObject dlqObject = CloudWatchLogsSinkUtils.createDlqObject(0, event.getEventHandle(), logString, failureMessage, dlqPushHandler, dropIfDlqNotConfigured);
                if (dlqObject != null) {
                    dlqObjects.add(dlqObject);
                } else if (dropIfDlqNotConfigured) {
                    cloudWatchLogsMetrics.increaseLogLargeEventsDroppedCounter(1);
                }
                continue;
            }

            processLock.lock();
            try {
                final LogGroupBuffer group = groupFor(event);
                group.stopWatch.startIfNotRunning();

                final Buffer buffer = group.buffer;
                int bufferSize = buffer.getBufferSize();
                int bufferEventCount = buffer.getEventCount();
                if (cloudWatchLogsLimits.maxRequestSizeLimitExceeds(logLength + bufferSize, bufferEventCount + 1)) {
                    stageLogEvents(group);
                }
                addToBuffer(buffer, event.getEventHandle(), logString);
                bufferEventCount = buffer.getEventCount();
                if (cloudWatchLogsLimits.isMaxEventCountLimitReached(bufferEventCount)) {
                    stageLogEvents(group);
                }
            } finally {
                processLock.unlock();
            }
        }
        CloudWatchLogsSinkUtils.handleDlqObjects(dlqObjects, dlqPushHandler);
    }

    /**
     * Resolves (creating if necessary) the buffer group for an event. Must be called while
     * holding {@code processLock}.
     */
    private LogGroupBuffer groupFor(final Event event) {
        if (!dynamic) {
            LogGroupBuffer group = groups.get(STATIC_GROUP_KEY);
            if (group == null) {
                group = new LogGroupBuffer(bufferFactory.getBuffer(), null, new SinkStopWatch());
                groups.put(STATIC_GROUP_KEY, group);
            }
            return group;
        }

        // Resolving interpolates the entity templates against the event; the key is the resolved key
        // attributes, so events whose templates produce the same entity share a group (and one PLE
        // request). Normalization, if enabled, is applied inside the resolver before the key is formed.
        final EntityResolver.ResolvedEntity resolved = entityResolver.resolve(event);
        String key = resolved.getKey();
        LogGroupBuffer group = groups.get(key);
        if (group != null) {
            return group;
        }

        // Cap the number of buffer groups with the same bound the resolver uses for its entity cache,
        // so a high-cardinality templated key cannot grow the groups map without limit. On overflow,
        // route the event to a shared fallback group that carries no per-resource entity rather than
        // allocating a new group per distinct key.
        Entity entity = resolved.getEntity();
        if (groups.size() >= entityResolver.maxCacheSize()) {
            group = groups.get(STATIC_GROUP_KEY);
            if (group != null) {
                return group;
            }
            key = STATIC_GROUP_KEY;
            entity = null;
        }

        group = new LogGroupBuffer(bufferFactory.getBuffer(), entity, new SinkStopWatch());
        groups.put(key, group);
        cloudWatchLogsMetrics.increaseEntityGroupsCreatedCounter(1);
        return group;
    }

    /**
     * Flushes any group whose time limit has been reached. Called on an empty poll so buffered
     * events do not sit indefinitely below the size/count thresholds.
     */
    private void flushIdleGroups() {
        for (final LogGroupBuffer group : groups.values()) {
            if (group.buffer.getEventCount() > 0
                    && cloudWatchLogsLimits.isTimeLimitReached(group.stopWatch.getElapsedTimeInSeconds())) {
                stageLogEvents(group);
            }
        }
    }

    private void stageLogEvents(final LogGroupBuffer group) {
        final Buffer buffer = group.buffer;
        group.stopWatch.stopAndReset();

        List<InputLogEvent> inputLogEvents = cloudWatchLogsDispatcher.prepareInputLogEvents(buffer.getBufferedData());
        if (dynamic) {
            // Dynamic mode supplies the group's resolved entity explicitly, one entity per request.
            cloudWatchLogsDispatcher.dispatchLogs(inputLogEvents, buffer.getEventHandles(), group.entity);
        } else {
            // Static mode defers to the dispatcher's frozen entity so it is not overridden by null.
            cloudWatchLogsDispatcher.dispatchLogs(inputLogEvents, buffer.getEventHandles());
        }
        cloudWatchLogsMetrics.recordRequestSize(buffer.getBufferSize());

        buffer.resetBuffer();
    }

    private void addToBuffer(final Buffer buffer, final EventHandle logEventHandle, final String logString) {
        buffer.writeEvent(logEventHandle, logString.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Holds the per-entity-group buffer, its resolved entity, and its own flush timer.
     */
    private static final class LogGroupBuffer {
        private final Buffer buffer;
        private final Entity entity;
        private final SinkStopWatch stopWatch;

        private LogGroupBuffer(final Buffer buffer, final Entity entity, final SinkStopWatch stopWatch) {
            this.buffer = buffer;
            this.entity = entity;
            this.stopWatch = stopWatch;
        }
    }

    /**
     * Adapts the legacy single-buffer static constructor to the group model by handing back the
     * one pre-built buffer the first time and fresh buffers thereafter (only relevant if the
     * single static group is ever flushed and reused — the same buffer instance is reset in place).
     */
    private static final class SingleBufferFactory implements BufferFactory {
        private final Buffer buffer;

        private SingleBufferFactory(final Buffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public Buffer getBuffer() {
            return buffer;
        }
    }
}
