/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.Buffer;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.CloudWatchLogsLimits;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils.SinkStopWatch;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The sink's group buffers: groups buffered events by resolved entity and headers, and takes ready groups as
 * {@link Batch}es for the caller to send.
 *
 * <p>Not thread-safe. Every method except {@link #activeEntityGroupCount()} MUST be called while holding the
 * sink's {@code processLock}. Taken batches are owned by the caller.
 */
public class CloudWatchLogsService {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsService.class);

    private final BufferFactory bufferFactory;
    private final CloudWatchLogsLimits cloudWatchLogsLimits;
    private CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final Supplier<SinkStopWatch> stopWatchFactory;

    /**
     * Whether events are partitioned per resolved entity. False in static mode, where
     * {@link #entityResolver} is null and every batch carries {@link #staticEntity}.
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
     * A fixed entity applied to every group when the resolver does not build one per group (or in static mode),
     * i.e. when the entity itself is static (or absent, in which case null).
     */
    private final Entity staticEntity;

    /**
     * The per-entity buffer groups, keyed by resolved entity. Empty in static mode.
     *
     * <p>Mutated only while the caller holds {@code processLock}. Concurrent so that
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
     * it forever. Created lazily, and read and written only while the caller holds {@code processLock}.
     */
    private LogGroupBuffer sharedGroup;

    /**
     * Whether the cardinality-bound warning has already been emitted. Overflow is counted on every event
     * but logged only once, so a sustained high-cardinality stream cannot flood the logs. Guarded by
     * the caller's {@code processLock}.
     */
    private boolean overflowLogged = false;

    /**
     * Static-entity constructor: a single buffer, and one entity for every batch. The cardinality bound is
     * irrelevant without a resolver, since static mode only ever holds the shared group.
     *
     * @param buffer the single buffer all events are written to
     * @param cloudWatchLogsMetrics the sink's metrics
     * @param cloudWatchLogsLimits the batch size, request size, and flush interval thresholds
     * @param staticEntity the entity carried by every batch, or null
     */
    public CloudWatchLogsService(final Buffer buffer,
                                 final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final Entity staticEntity) {
        this(new SingleBufferFactory(buffer), cloudWatchLogsMetrics, cloudWatchLogsLimits, null, 0, staticEntity,
                SinkStopWatch::new);
    }

    /**
     * Grouped constructor. Events are partitioned by their resolved entity and headers, one buffer and so one
     * {@code PutLogEvents} request per group.
     *
     * @param bufferFactory supplies a fresh buffer for each entity group
     * @param cloudWatchLogsMetrics the sink's metrics
     * @param cloudWatchLogsLimits the batch size, request size, and flush interval thresholds
     * @param entityResolver resolves entity keys and entities, or null for static mode
     * @param maxEntityCardinality maximum number of entity groups held at one time
     * @param staticEntity the entity applied to each group when {@code entityResolver} does not build one, or null
     */
    public CloudWatchLogsService(final BufferFactory bufferFactory,
                                 final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                                 final CloudWatchLogsLimits cloudWatchLogsLimits,
                                 final EntityResolver entityResolver,
                                 final int maxEntityCardinality,
                                 final Entity staticEntity) {
        this(bufferFactory, cloudWatchLogsMetrics, cloudWatchLogsLimits, entityResolver, maxEntityCardinality, staticEntity,
                SinkStopWatch::new);
    }

    CloudWatchLogsService(final BufferFactory bufferFactory,
                          final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                          final CloudWatchLogsLimits cloudWatchLogsLimits,
                          final EntityResolver entityResolver,
                          final int maxEntityCardinality,
                          final Entity staticEntity,
                          final Supplier<SinkStopWatch> stopWatchFactory) {
        this.bufferFactory = bufferFactory;
        this.cloudWatchLogsLimits = cloudWatchLogsLimits;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.entityResolver = entityResolver;
        this.dynamic = entityResolver != null;
        this.maxEntityCardinality = maxEntityCardinality;
        this.staticEntity = staticEntity;
        this.stopWatchFactory = stopWatchFactory;
    }

    /**
     * Buffers one event, appending to {@code out} each batch taken along the way. The group is resolved before
     * any group state is mutated. Throws only if the event was not written to a buffer; once written, the event
     * is owned by the buffer and any later failure is logged instead.
     *
     * @param event the event to buffer
     * @param message the event's serialized UTF-8 bytes
     * @param out receives the batches taken, 0, 1 or 2 per call
     */
    public void add(final Event event, final byte[] message, final List<Batch> out) {
        final LogGroupBuffer group = groupFor(event);
        final Buffer buffer = group.buffer;
        final int bufferEventCount = buffer.getEventCount();
        if (bufferEventCount > 0
                && cloudWatchLogsLimits.maxRequestSizeLimitExceeds(message.length + buffer.getBufferSize(), bufferEventCount + 1)) {
            take(group, out);
        }
        group.stopWatch.startIfNotRunning();
        buffer.writeEvent(event.getEventHandle(), message);
        try {
            // The time limit is checked here, not only on an empty poll, because a group that keeps
            // receiving events but never reaches the count or size threshold would otherwise sit
            // unsent for as long as the pipeline stays busy.
            if (cloudWatchLogsLimits.isMaxEventCountLimitReached(buffer.getEventCount())
                    || cloudWatchLogsLimits.isTimeLimitReached(group.stopWatch.getElapsedTimeInSeconds())) {
                take(group, out);
            }
        } catch (final Exception | StackOverflowError e) {
            LOG.error("Unexpected error after buffering an event; it stays buffered and is sent with its group.", e);
        }
    }

    /**
     * Takes every non-empty group whose flush interval has elapsed, and evicts the dynamic groups left empty
     * and idle for the flush interval. Evicting is what keeps the cardinality bound a measure of concurrently
     * active entities. The shared group is flushed but never evicted.
     *
     * @param out receives the batches taken
     */
    public void takeReady(final List<Batch> out) {
        final Iterator<LogGroupBuffer> iterator = groups.values().iterator();
        while (iterator.hasNext()) {
            final LogGroupBuffer group = iterator.next();
            if (!cloudWatchLogsLimits.isTimeLimitReached(group.stopWatch.getElapsedTimeInSeconds())) {
                continue;
            }
            if (group.buffer.getEventCount() > 0) {
                take(group, out);
            } else {
                // Safe to drop: a take copies the events and handles into the batch before the buffer is
                // reset, so nothing taken refers to this group.
                iterator.remove();
            }
        }
        // The shared group is flushed but never dropped. It holds no entity that could go stale, and
        // it lives outside `groups` so retaining it costs no cardinality slot.
        if (sharedGroup != null && sharedGroup.buffer.getEventCount() > 0
                && cloudWatchLogsLimits.isTimeLimitReached(sharedGroup.stopWatch.getElapsedTimeInSeconds())) {
            take(sharedGroup, out);
        }
    }

    /**
     * Takes every non-empty group, dynamic and shared, regardless of readiness. Used by shutdown.
     *
     * @param out receives the batches taken
     */
    public void takeAll(final List<Batch> out) {
        for (final LogGroupBuffer group : groups.values()) {
            if (group.buffer.getEventCount() > 0) {
                take(group, out);
            }
        }
        if (sharedGroup != null && sharedGroup.buffer.getEventCount() > 0) {
            take(sharedGroup, out);
        }
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

        final Optional<String> invalidValue = entityResolver.findInvalidValue(resolvedKey, event);
        if (invalidValue.isPresent()) {
            cloudWatchLogsMetrics.increaseEntityDroppedCounter(invalidValue.get());
            return sharedGroup();
        }

        // Cap the groups held concurrently so a high-cardinality templated key cannot grow the groups
        // map without limit. On overflow, route the event to the shared group, which carries no
        // per-resource entity, rather than allocating a new group per distinct key. Because takeReady()
        // evicts idle groups, this bound applies to entities active at the same time rather than every
        // key seen since startup.
        if (groups.size() >= maxEntityCardinality) {
            // Counted per event, and separately from the groups-created counter, so that the loss of
            // per-resource attribution is visible rather than mixed in with real entities.
            cloudWatchLogsMetrics.increaseEntityOverflowEventsCounter(1);
            if (!overflowLogged) {
                overflowLogged = true;
                LOG.warn("Cardinality bound of {} reached; events are being routed to a shared fallback "
                        + "group with no per-resource entity or headers until an existing group goes idle. "
                        + "Reduce the cardinality of the templated entity key attributes or headers, or raise "
                        + "the entity max_cardinality.", maxEntityCardinality);
            }
            return sharedGroup();
        }

        // Built once per group, not once per event. When the resolver partitions purely by dynamic headers,
        // there is no per-group entity to build and the fixed staticEntity (possibly null) is used instead.
        final Entity entity = entityResolver.buildsEntity()
                ? entityResolver.buildEntity(resolvedKey, event)
                : staticEntity;
        group = new LogGroupBuffer(bufferFactory.getBuffer(), entity, resolvedKey.getHeaders(),
                stopWatchFactory.get());
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
            // A constant static entity is not high-cardinality, so it still applies to overflow events; only
            // a per-group resolved entity (and the per-request headers) are dropped when attribution is lost.
            final Entity sharedEntity = (entityResolver == null || !entityResolver.buildsEntity()) ? staticEntity : null;
            sharedGroup = new LogGroupBuffer(bufferFactory.getBuffer(), sharedEntity, null, stopWatchFactory.get());
        }
        return sharedGroup;
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

    /**
     * Takes the group's contents as a batch. The batch copies the handles and messages, and the buffer is reset
     * right after, with nothing that can throw in between. The batch is added to {@code out} before the stopwatch
     * restart and the metric call, so a taken batch always has an owner.
     */
    private void take(final LogGroupBuffer group, final List<Batch> out) {
        final Buffer buffer = group.buffer;
        final int bufferSize = buffer.getBufferSize();
        final Batch batch = new Batch(buffer.getEventHandles(), buffer.getBufferedData(), group.entity, group.headers);
        buffer.resetBuffer();
        out.add(batch);
        group.stopWatch.stopAndReset();
        cloudWatchLogsMetrics.recordRequestSize(bufferSize);
    }

    /**
     * Holds one group's buffer, its resolved entity, and its own flush timer. The entity is null for the
     * shared group, which carries no per-resource attribution.
     */
    private static final class LogGroupBuffer {
        private final Buffer buffer;
        private final Entity entity;
        private final Map<String, String> headers;
        private final SinkStopWatch stopWatch;

        private LogGroupBuffer(final Buffer buffer, final Entity entity, final Map<String, String> headers,
                               final SinkStopWatch stopWatch) {
            this.buffer = buffer;
            this.entity = entity;
            this.headers = headers;
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
