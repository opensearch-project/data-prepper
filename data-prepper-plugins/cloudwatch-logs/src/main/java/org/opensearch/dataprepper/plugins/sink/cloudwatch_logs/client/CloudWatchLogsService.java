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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final BufferFactory bufferFactory;
    private final CloudWatchLogsLimits cloudWatchLogsLimits;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final ReentrantLock processLock;
    private final DlqPushHandler dlqPushHandler;
    private final boolean dropIfDlqNotConfigured;

    /**
     * Whether events are partitioned per resolved entity. False in static mode, where
     * {@link #entityResolver} is null and the dispatcher's own entity is used for every request.
     */
    private final boolean dynamic;

    /**
     * Resolves each event's entity key, and builds the entity for a newly opened group. Null in static
     * mode.
     */
    private final EntityResolver entityResolver;

    /**
     * Bound on entity groups held concurrently in dynamic mode; see
     * {@link org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.EntityConfig#getMaxCardinality()}.
     * Unused in static mode, where there is only ever the shared group.
     */
    private final int maxEntityCardinality;

    /**
     * The per-entity buffer groups, keyed by resolved entity. Empty in static mode.
     *
     * <p>Mutated only while holding {@link #processLock}. Concurrent so that
     * {@link #activeEntityGroupCount()}, which the cardinality gauge samples from the metrics thread,
     * never reads the map mid-rehash.
     */
    private final Map<EntityResolver.ResolvedKey, LogGroupBuffer> groups = new ConcurrentHashMap<>();

    /**
     * The one group that carries no per-entity attribution: the only group in static mode, and the
     * overflow group in dynamic mode once {@link #maxEntityCardinality} is reached.
     *
     * <p>Held here rather than in {@link #groups} on purpose. It is never evicted — it has no entity to
     * go stale — so keeping it in the map would permanently consume one of the cardinality slots it
     * exists to absorb overflow from. With {@code max_cardinality} of 1 that was fatal: the first
     * overflow would take the single slot and every subsequent event, of every key, would overflow into
     * it forever. Created lazily, and read and written only while holding {@link #processLock}.
     */
    private LogGroupBuffer sharedGroup;

    /**
     * Whether the cardinality-bound warning has already been emitted. Overflow is counted on every event
     * but logged only once, so a sustained high-cardinality stream cannot flood the logs. Guarded by
     * {@link #processLock}.
     */
    private boolean overflowLogged = false;

    /**
     * Static-entity constructor. Preserves the original behavior: a single buffer, and one entity for
     * every request. The cardinality bound is irrelevant without a resolver, since static mode only ever
     * holds the shared group.
     *
     * @param buffer the single buffer all events are written to
     * @param cloudWatchLogsMetrics the sink's metrics
     * @param cloudWatchLogsLimits the batch size, request size, and flush interval thresholds
     * @param cloudWatchLogsDispatcher dispatches staged events, supplying its own configured entity
     * @param dlqPushHandler handler for events that cannot be sent, or null
     * @param dropIfDlqNotConfigured whether to drop oversized events when no DLQ is configured
     */
    public CloudWatchLogsService(final Buffer buffer,
                                 final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final CloudWatchLogsDispatcher cloudWatchLogsDispatcher,
                                 final DlqPushHandler dlqPushHandler,
                                 final boolean dropIfDlqNotConfigured) {
        this(new SingleBufferFactory(buffer), cloudWatchLogsMetrics, cloudWatchLogsLimits,
                cloudWatchLogsDispatcher, dlqPushHandler, dropIfDlqNotConfigured, null, 0);
    }

    /**
     * Dynamic-entity constructor. Events are partitioned by their resolved entity, one buffer and so one
     * {@code PutLogEvents} request per entity.
     *
     * @param bufferFactory supplies a fresh buffer for each entity group
     * @param cloudWatchLogsMetrics the sink's metrics
     * @param cloudWatchLogsLimits the batch size, request size, and flush interval thresholds
     * @param cloudWatchLogsDispatcher dispatches staged events with the group's resolved entity
     * @param dlqPushHandler handler for events that cannot be sent, or null
     * @param dropIfDlqNotConfigured whether to drop oversized events when no DLQ is configured
     * @param entityResolver resolves entity keys and entities, or null for static mode
     * @param maxEntityCardinality maximum number of entity groups held at one time
     */
    public CloudWatchLogsService(final BufferFactory bufferFactory,
                                 final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final CloudWatchLogsDispatcher cloudWatchLogsDispatcher,
                                 final DlqPushHandler dlqPushHandler,
                                 final boolean dropIfDlqNotConfigured,
                                 final EntityResolver entityResolver,
                                 final int maxEntityCardinality) {
        this.bufferFactory = bufferFactory;
        this.cloudWatchLogsLimits = cloudWatchLogsLimits;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.processLock = new ReentrantLock();
        this.cloudWatchLogsDispatcher = cloudWatchLogsDispatcher;
        this.dlqPushHandler = dlqPushHandler;
        this.dropIfDlqNotConfigured = dropIfDlqNotConfigured;
        this.entityResolver = entityResolver;
        this.dynamic = entityResolver != null;
        this.maxEntityCardinality = maxEntityCardinality;
    }

    /**
     * Function handles the packaging of events into log events before sending a bulk request to CloudWatchLogs.
     * @param logs Collection of Record events.
     */
    public void processLogEvents(final Collection<Record<Event>> logs) {
        if (logs.isEmpty()) {
            sweepGroups();
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
                // The time limit is checked here, not only on an empty poll, because a group that keeps
                // receiving events but never reaches the count or size threshold would otherwise sit
                // unsent for as long as the pipeline stays busy.
                if (cloudWatchLogsLimits.isMaxEventCountLimitReached(bufferEventCount)
                        || cloudWatchLogsLimits.isTimeLimitReached(group.stopWatch.getElapsedTimeInSeconds())) {
                    stageLogEvents(group);
                }
            } finally {
                processLock.unlock();
            }
        }

        // Groups that received nothing in this batch still have to honour the flush interval, so every
        // batch ends with a sweep rather than only empty polls triggering one.
        sweepGroups();
        CloudWatchLogsSinkUtils.handleDlqObjects(dlqObjects, dlqPushHandler);
    }

    /**
     * Resolves (creating if necessary) the buffer group for an event. Must be called while
     * holding {@code processLock}.
     */
    private LogGroupBuffer groupFor(final Event event) {
        if (!dynamic) {
            return sharedGroup();
        }

        // Only the key attributes are interpolated per event; the entity itself is built below, once,
        // when a new group is opened. Events whose key attributes interpolate to the same value share a
        // group, and so one PLE request.
        final EntityResolver.ResolvedKey resolvedKey = entityResolver.resolveKey(event);
        LogGroupBuffer group = groups.get(resolvedKey);
        if (group != null) {
            return group;
        }

        // Cap the groups held concurrently so a high-cardinality templated key cannot grow the groups
        // map without limit. On overflow, route the event to the shared group, which carries no
        // per-resource entity, rather than allocating a new group per distinct key. Because sweepGroups()
        // evicts idle groups, this bound applies to entities active at the same time rather than every
        // key seen since startup.
        if (groups.size() >= maxEntityCardinality) {
            // Counted per event, and separately from the groups-created counter, so that the loss of
            // per-resource attribution is visible rather than mixed in with real entities.
            cloudWatchLogsMetrics.increaseEntityOverflowEventsCounter(1);
            if (!overflowLogged) {
                overflowLogged = true;
                LOG.warn("Entity cardinality bound of {} reached; events are being routed to a shared "
                        + "fallback group with no entity until an existing group goes idle. Reduce the "
                        + "cardinality of the templated entity key attributes, or raise the entity "
                        + "max_cardinality.", maxEntityCardinality);
            }
            return sharedGroup();
        }

        // The only place an entity is built: once per group, not once per event.
        group = new LogGroupBuffer(bufferFactory.getBuffer(), entityResolver.buildEntity(resolvedKey, event),
                new SinkStopWatch());
        groups.put(resolvedKey, group);
        cloudWatchLogsMetrics.increaseEntityGroupsCreatedCounter(1);
        return group;
    }

    /**
     * The shared, entity-less group, created on first use. Must be called while holding
     * {@code processLock}.
     */
    private LogGroupBuffer sharedGroup() {
        if (sharedGroup == null) {
            sharedGroup = new LogGroupBuffer(bufferFactory.getBuffer(), null, new SinkStopWatch());
        }
        return sharedGroup;
    }

    /**
     * Flushes every group whose time limit has been reached and drops the entity groups left empty and
     * idle. Evicting is what keeps the cardinality bound a measure of concurrently active entities: a key
     * seen once would otherwise hold its slot, and its buffer, for the lifetime of the process.
     */
    private void sweepGroups() {
        processLock.lock();
        try {
            final Iterator<LogGroupBuffer> iterator = groups.values().iterator();
            while (iterator.hasNext()) {
                final LogGroupBuffer group = iterator.next();
                if (!cloudWatchLogsLimits.isTimeLimitReached(group.stopWatch.getElapsedTimeInSeconds())) {
                    continue;
                }
                if (group.buffer.getEventCount() > 0) {
                    stageLogEvents(group);
                } else {
                    // Safe to drop: staging hands the events and handles to the dispatcher before the
                    // buffer is reset, so nothing in flight refers to this group.
                    iterator.remove();
                }
            }
            // The shared group is flushed but never dropped. It holds no entity that could go stale, and
            // it lives outside `groups` so retaining it costs no cardinality slot.
            if (sharedGroup != null && sharedGroup.buffer.getEventCount() > 0
                    && cloudWatchLogsLimits.isTimeLimitReached(sharedGroup.stopWatch.getElapsedTimeInSeconds())) {
                stageLogEvents(sharedGroup);
            }
        } finally {
            processLock.unlock();
        }
    }

    /**
     * Number of entity buffer groups currently held, i.e. the entities being buffered right now. The
     * shared group is excluded, so this stays a count of real entities. This is what the
     * {@code cloudWatchLogsEntityCardinality} gauge reports.
     *
     * @return the number of entities currently being buffered
     */
    public int activeEntityGroupCount() {
        return groups.size();
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
     * Holds one group's buffer, its resolved entity, and its own flush timer. The entity is null for the
     * shared group, which carries no per-resource attribution.
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
     * Adapts the legacy single-buffer static constructor to the group model by always handing back the
     * one pre-built buffer. Static mode only ever opens the shared group, so it is only ever asked once.
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
