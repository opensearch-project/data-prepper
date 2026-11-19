/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.dataprepper.plugins.processor.oteltrace.model.SpanSet;
import org.opensearch.dataprepper.plugins.processor.oteltrace.model.TraceGroup;
import org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


@DataPrepperPlugin(name = "otel_traces", deprecatedName = "otel_trace_raw", pluginType = Processor.class, pluginConfigurationType = OtelTraceRawProcessorConfig.class)
public class OTelTraceRawProcessor extends AbstractProcessor<Record<Span>, Record<Span>> implements RequiresPeerForwarding {
    private static final long SEC_TO_MILLIS = 1_000L;
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceRawProcessor.class);
    public static final String TRACE_GROUP_CACHE_COUNT_METRIC_NAME = "traceGroupCacheCount";
    public static final String SPAN_SET_COUNT_METRIC_NAME = "spanSetCount";
    public static final String GENAI_COVERAGE_COUNT_METRIC_NAME = "genAiCoverageCount";
    public static final String GENAI_COVERAGE_SPAN_ID_COUNT_METRIC_NAME = "genAiCoverageSpanIdCount";
    public static final String GENAI_HELD_ROOT_COUNT_METRIC_NAME = "genAiHeldRootCount";
    public static final String GENAI_PARKED_SPAN_COUNT_METRIC_NAME = "genAiParkedSpanCount";

    private final long traceFlushInterval;

    private final Map<String, SpanSet> traceIdSpanSetMap = new ConcurrentHashMap<>();

    private final Cache<String, TraceGroup> traceIdTraceGroupCache;

    /** Per-trace GenAI state: covered span ids, token totals and propagated values seen so far. */
    private final GenAiTraceState genAiTraceState;
    private final GenAiRootPropagation genAiRootPropagation;
    private final boolean genAiEnrichmentEnabled;
    private final long genAiRootHoldMillis;

    /** GenAI roots held back for late children that could complete their model or token usage. */
    private final Map<String, HeldRootSpan> heldRootSpansByTraceId = new ConcurrentHashMap<>();

    /**
     * GenAI spans held back, per trace, until their ancestor chain tells whether they are their
     * trace's outermost GenAI span. The lists are replaced, never mutated, so the gauge can read them.
     */
    private final Map<String, List<ParkedSpan>> parkedSpansByTraceId = new ConcurrentHashMap<>();

    private long lastTraceFlushTime = 0L;

    private final ReentrantLock traceFlushLock = new ReentrantLock();
    private final ReentrantLock prepareForShutdownLock = new ReentrantLock();

    private volatile boolean isShuttingDown = false;

    @DataPrepperPluginConstructor
    public OTelTraceRawProcessor(final OtelTraceRawProcessorConfig otelTraceRawProcessorConfig,
                                 final PipelineDescription pipelineDescription,
                                 final PluginMetrics pluginMetrics) {
        super(pluginMetrics);
        traceFlushInterval = SEC_TO_MILLIS * otelTraceRawProcessorConfig.getTraceFlushIntervalSeconds();
        final Duration traceGroupCacheTimeToLive = otelTraceRawProcessorConfig.getTraceGroupCacheTimeToLive();
        traceIdTraceGroupCache = Caffeine.newBuilder()
          .maximumSize(otelTraceRawProcessorConfig.getTraceGroupCacheMaxSize())
          .expireAfterWrite(traceGroupCacheTimeToLive.toMillis(), TimeUnit.MILLISECONDS)
          .build();

        if (traceGroupCacheTimeToLive.toMillis() < traceFlushInterval) {
            LOG.warn("The configured trace_group_cache_ttl ({} ms) is shorter than trace_flush_interval ({} ms). " +
                    "Orphaned child spans held up to trace_flush_interval may find their trace group expired and " +
                    "be written without trace group information.", traceGroupCacheTimeToLive.toMillis(), traceFlushInterval);
        }

        genAiEnrichmentEnabled = !otelTraceRawProcessorConfig.isGenAiEnrichmentDisabled();
        genAiRootPropagation = Objects.requireNonNullElse(
                otelTraceRawProcessorConfig.getGenAiRootPropagation(), GenAiRootPropagation.AI_ROOT);
        genAiRootHoldMillis = genAiEnrichmentEnabled
                ? Objects.requireNonNullElse(otelTraceRawProcessorConfig.getGenAiRootHold(), Duration.ZERO).toMillis()
                : 0L;
        // Outlives the orphan flush: spans that arrive late are held up to two flush intervals.
        genAiTraceState = genAiEnrichmentEnabled
                ? GenAiTraceState.create(otelTraceRawProcessorConfig.getGenAiCoverageCacheMaxSize(),
                        Duration.ofMillis(2 * traceFlushInterval).plus(traceGroupCacheTimeToLive))
                : GenAiTraceState.NONE;

        pluginMetrics.gauge(TRACE_GROUP_CACHE_COUNT_METRIC_NAME, traceIdTraceGroupCache, cache -> (double) cache.estimatedSize());
        pluginMetrics.gauge(SPAN_SET_COUNT_METRIC_NAME, traceIdSpanSetMap, cache -> (double) cache.size());
        pluginMetrics.gauge(GENAI_COVERAGE_COUNT_METRIC_NAME, genAiTraceState, state -> (double) state.estimatedSize());
        pluginMetrics.gauge(GENAI_COVERAGE_SPAN_ID_COUNT_METRIC_NAME, genAiTraceState, state -> (double) state.spanIdCount());
        pluginMetrics.gauge(GENAI_HELD_ROOT_COUNT_METRIC_NAME, heldRootSpansByTraceId, map -> (double) map.size());
        pluginMetrics.gauge(GENAI_PARKED_SPAN_COUNT_METRIC_NAME, parkedSpansByTraceId,
                map -> map.values().stream().mapToInt(List::size).sum());

        LOG.info("Configured Trace Raw Processor with a trace flush interval of {} ms, GenAI enrichment {}.",
                traceFlushInterval, genAiEnrichmentEnabled ? "enabled with root propagation " + genAiRootPropagation : "disabled");
    }

    /**
     * execute the processor logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    @Override
    public Collection<Record<Span>> doExecute(Collection<Record<Span>> records) {
        final List<Span> processedSpans = new LinkedList<>();

        for (Record<Span> record : records) {
            final Span span = record.getData();
            processSpan(span, processedSpans);
            fillInServiceName(span);
        }

        final List<Span> flushedSpans = getTracesToFlushByGarbageCollection();
        processedSpans.addAll(flushedSpans);

        // Enrich GenAI agent traces (normalize attributes, strip conflicting sub-keys, mark agent roots, enrich roots)
        if (genAiEnrichmentEnabled) {
            enrichBatchHoldingBackUnresolvedSpans(processedSpans, flushedSpans);
        }

        // Held GenAI roots, after this batch updated the trace state: release the ones the state
        // can now complete, the ones whose hold expired, and all of them on shutdown.
        final List<Span> releasedRootSpans = releaseHeldRootSpans();
        if (!releasedRootSpans.isEmpty()) {
            if (genAiEnrichmentEnabled) {
                GenAiEnrichmentHelper.enrichBatch(releasedRootSpans, genAiTraceState, genAiRootPropagation,
                        this::rootSpanIdOf, span -> false);
            }
            processedSpans.addAll(releasedRootSpans);
        }

        // Derive server span attributes (fault, error, operation, environment)
        OTelSpanDerivationUtil.deriveServerSpanAttributes(processedSpans);

        return processedSpans.stream().map(Record::new).collect(Collectors.toList());
    }

    /**
     * Branching logic to handle root and child spans.
     * A root span is the first span of a trace, it has no parentSpanId.
     *
     * @param span Span to be evaluated
     * @param spanSet Collection to insert spans to
     */
    private void processSpan(final Span span, final Collection<Span> spanSet) {
        if (StringUtils.isBlank(span.getParentSpanId())) {
            final List<Span> rootSpanAndChildren = processRootSpan(span);
            spanSet.addAll(rootSpanAndChildren);
        } else {
            spanSet.addAll(processChildSpan(span));
        }
    }

    /**
     * Retrieves all child spans from memory and returns them as a set with the root span.
     * Also adds an entry to the traceID cache so that later child spans can be tagged,
     * in the case where a child span is processed AFTER the root span.
     * The waiting children are claimed with a single atomic remove, after the trace group is
     * cached. A child inserts itself and then re-checks the cache ({@link #processChildSpan}), so
     * a child inserted too late for this claim finds the trace group and claims itself; the atomic
     * remove on both sides guarantees exactly one of them emits each waiting child.
     * A root that is itself a GenAI span or marked at the source is recorded as covered in the
     * GenAI trace state first: cover before put, and put before claim, so any child that can
     * observe the trace group, by its first lookup or by its re-check, also observes the root as
     * covered and is not mistaken for the trace's outermost GenAI span. The trace state also
     * records that the root is a GenAI root, so no span of the trace waits for its ancestors. A
     * non-GenAI root is never covered, so the agent span below it is still marked.
     * A GenAI root that is still missing data its late children could provide is parked in the
     * held-roots map instead of being returned; its children keep flowing through the trace group
     * cache, and the root is released by {@link #releaseHeldRootSpans()}.
     *
     * @param parentSpan
     * @return List containing root span, along with any child spans that have already been processed.
     */
    private List<Span> processRootSpan(final Span parentSpan) {
        final TraceGroup traceGroup = TraceGroup.fromSpan(parentSpan);
        final String parentSpanTraceId = parentSpan.getTraceId();
        if (genAiEnrichmentEnabled && GenAiEnrichmentHelper.isGenAiOrMarkedSpan(parentSpan)) {
            genAiTraceState.update(parentSpanTraceId, state -> {
                state.cover(parentSpan.getSpanId());
                state.recordGenAiRoot();
            });
        }
        traceIdTraceGroupCache.put(parentSpanTraceId, traceGroup);

        final List<Span> recordsToFlush = new LinkedList<>();

        final SpanSet claimedSpanSet = traceIdSpanSetMap.remove(parentSpanTraceId);
        if (shouldHoldRootSpan(parentSpan, claimedSpanSet)) {
            final HeldRootSpan replacedRootSpan = heldRootSpansByTraceId.put(parentSpanTraceId,
                    new HeldRootSpan(parentSpan, System.currentTimeMillis() + genAiRootHoldMillis));
            if (replacedRootSpan != null) {
                recordsToFlush.add(replacedRootSpan.span);
            }
        } else {
            recordsToFlush.add(parentSpan);
        }

        if (claimedSpanSet != null) {
            for (final Span span : claimedSpanSet.getSpans()) {
                fillInTraceGroupInfo(span, traceGroup);
                recordsToFlush.add(span);
            }
        }

        return recordsToFlush;
    }

    /**
     * Whether to hold a trace's root span back for late children. Only a GenAI root still missing
     * a model or token usage that the accumulated trace state cannot provide is held; children
     * waiting in memory are about to update the state in this very batch, so their root is not.
     */
    private boolean shouldHoldRootSpan(final Span rootSpan, final SpanSet waitingChildren) {
        if (genAiRootHoldMillis <= 0 || isShuttingDown) {
            return false;
        }
        if (waitingChildren != null && !waitingChildren.getSpans().isEmpty()) {
            return false;
        }
        return GenAiEnrichmentHelper.rootAwaitsGenAiEnrichment(rootSpan, genAiTraceState);
    }

    /**
     * Releases held GenAI roots: as soon as the accumulated trace state can complete them, when
     * their hold expires (enriched with whatever the state has), and all of them on shutdown.
     */
    private List<Span> releaseHeldRootSpans() {
        if (heldRootSpansByTraceId.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Span> releasedRootSpans = new LinkedList<>();
        final long now = System.currentTimeMillis();
        for (final Map.Entry<String, HeldRootSpan> entry : heldRootSpansByTraceId.entrySet()) {
            final HeldRootSpan heldRootSpan = entry.getValue();
            if (isShuttingDown || now >= heldRootSpan.deadlineMillis
                    || !GenAiEnrichmentHelper.rootAwaitsGenAiEnrichment(heldRootSpan.span, genAiTraceState)) {
                if (heldRootSpansByTraceId.remove(entry.getKey(), heldRootSpan)) {
                    releasedRootSpans.add(heldRootSpan.span);
                }
            }
        }
        return releasedRootSpans;
    }

    /**
     * Enriches the batch, first adding the held-back GenAI spans that can now be decided so they
     * are enriched together with the ancestors that arrived for them, and holding back the batch's
     * GenAI spans whose ancestor chain reaches a span not seen yet: marking such a span now could
     * make it a second AI root of its trace, and an indexed document cannot be un-marked later.
     * A held-back span is released when its chain resolves, at its deadline (then treated as
     * outermost, as when nothing beyond its first parent was known), and on shutdown. Spans flushed
     * by the orphan sweep, released at their deadline or on shutdown are never held back again.
     */
    private void enrichBatchHoldingBackUnresolvedSpans(final List<Span> processedSpans, final List<Span> flushedSpans) {
        final long now = System.currentTimeMillis();
        final Set<Span> neverHeldBack = Collections.newSetFromMap(new IdentityHashMap<>());
        neverHeldBack.addAll(flushedSpans);
        final Map<Span, Long> deadlineBySpan = new IdentityHashMap<>();
        for (final ParkedSpan parkedSpan : releaseParkedSpans(processedSpans, now)) {
            processedSpans.add(parkedSpan.span);
            if (isShuttingDown || now >= parkedSpan.deadlineMillis) {
                neverHeldBack.add(parkedSpan.span);
            } else {
                deadlineBySpan.put(parkedSpan.span, parkedSpan.deadlineMillis);
            }
        }

        final List<Span> heldBackSpans = GenAiEnrichmentHelper.enrichBatch(processedSpans, genAiTraceState,
                genAiRootPropagation, this::rootSpanIdOf,
                span -> genAiRootHoldMillis > 0 && !isShuttingDown && !neverHeldBack.contains(span));
        if (heldBackSpans.isEmpty()) {
            return;
        }
        final Set<Span> heldBack = Collections.newSetFromMap(new IdentityHashMap<>());
        heldBack.addAll(heldBackSpans);
        processedSpans.removeIf(heldBack::contains);
        for (final Span span : heldBackSpans) {
            final ParkedSpan parkedSpan = new ParkedSpan(span, deadlineBySpan.getOrDefault(span, now + genAiRootHoldMillis));
            parkedSpansByTraceId.merge(span.getTraceId(), List.of(parkedSpan), (existing, added) -> {
                final List<ParkedSpan> merged = new ArrayList<>(existing);
                merged.addAll(added);
                return List.copyOf(merged);
            });
        }
    }

    /**
     * Claims the held-back spans to re-enter this batch: all of a trace's when the batch holds
     * spans of that trace (its missing ancestors may be among them), or when one of them can now
     * be decided from the trace state or has reached its deadline; all of them on shutdown. A
     * claimed span whose chain still cannot be decided is held back again with its deadline.
     */
    private List<ParkedSpan> releaseParkedSpans(final Collection<Span> batchSpans, final long now) {
        if (parkedSpansByTraceId.isEmpty()) {
            return Collections.emptyList();
        }
        final Set<String> batchTraceIds = new HashSet<>();
        for (final Span span : batchSpans) {
            if (span.getTraceId() != null) {
                batchTraceIds.add(span.getTraceId());
            }
        }
        final List<ParkedSpan> released = new ArrayList<>();
        for (final Map.Entry<String, List<ParkedSpan>> entry : parkedSpansByTraceId.entrySet()) {
            final List<ParkedSpan> parkedSpans = entry.getValue();
            final boolean release = isShuttingDown || batchTraceIds.contains(entry.getKey())
                    || parkedSpans.stream().anyMatch(parkedSpan -> now >= parkedSpan.deadlineMillis
                            || GenAiEnrichmentHelper.isOutermostResolvable(parkedSpan.span, genAiTraceState, this::rootSpanIdOf));
            if (release && parkedSpansByTraceId.remove(entry.getKey(), parkedSpans)) {
                released.addAll(parkedSpans);
            }
        }
        return released;
    }

    /** The root span id of a trace whose root was processed, while its trace group is cached. */
    private Optional<String> rootSpanIdOf(final String traceId) {
        return Optional.ofNullable(traceIdTraceGroupCache.getIfPresent(traceId)).map(TraceGroup::getRootSpanId);
    }

    /** A GenAI span held back until its ancestor chain resolves, and the time its hold expires. */
    private static final class ParkedSpan {
        private final Span span;
        private final long deadlineMillis;

        private ParkedSpan(final Span span, final long deadlineMillis) {
            this.span = span;
            this.deadlineMillis = deadlineMillis;
        }
    }

    /** A root span held back for late children, and the time its hold expires. */
    private static final class HeldRootSpan {
        private final Span span;
        private final long deadlineMillis;

        private HeldRootSpan(final Span span, final long deadlineMillis) {
            this.span = span;
            this.deadlineMillis = deadlineMillis;
        }
    }

    /**
     * Attempts to populate the traceGroup of the child span by fetching from a cache. If the traceGroup is not in the cache,
     * the child span is kept in memory to be populated when its corresponding root span arrives.
     * After inserting itself, the child re-checks the cache: the root may have cached the trace
     * group and claimed the waiting spans between this child's miss and its insert, in which case
     * nobody would ever release it. The root always caches the trace group before its claim, and
     * the re-check always happens after the insert, so every inserted child is observed by at
     * least one claimant; the atomic remove guarantees at most one of them emits it. A claim can
     * include spans other workers inserted, so all claimed spans are returned.
     *
     * @param childSpan
     * @return the child span, and possibly other spans of its trace this call claimed, with their
     *         trace group filled in; empty when the span stays in memory awaiting its root
     */
    private List<Span> processChildSpan(final Span childSpan) {
        final String childSpanTraceId = childSpan.getTraceId();
        final TraceGroup traceGroup = traceIdTraceGroupCache.getIfPresent(childSpanTraceId);

        if (traceGroup != null) {
            fillInTraceGroupInfo(childSpan, traceGroup);
            return Collections.singletonList(childSpan);
        }

        traceIdSpanSetMap.compute(childSpanTraceId, (traceId, spanSet) -> {
            if (spanSet == null) {
                spanSet = new SpanSet();
            }
            spanSet.addSpan(childSpan);
            return spanSet;
        });

        final TraceGroup lateTraceGroup = traceIdTraceGroupCache.getIfPresent(childSpanTraceId);
        if (lateTraceGroup == null) {
            return Collections.emptyList();
        }
        final SpanSet claimedSpanSet = traceIdSpanSetMap.remove(childSpanTraceId);
        if (claimedSpanSet == null) {
            return Collections.emptyList();
        }
        final List<Span> claimedSpans = new LinkedList<>();
        for (final Span claimedSpan : claimedSpanSet.getSpans()) {
            fillInTraceGroupInfo(claimedSpan, lateTraceGroup);
            claimedSpans.add(claimedSpan);
        }
        return claimedSpans;
    }

    /**
     * Periodically flush spans from memory. Typically all spans of a trace are written
     * once the trace's root span arrives, however some child spans my arrive after the root span.
     * This method ensures "orphaned" child spans are eventually flushed from memory.
     * @return List of RawSpans to be sent down the pipeline
     */
    private List<Span> getTracesToFlushByGarbageCollection() {
        final List<Span> recordsToFlush = new LinkedList<>();

        if (shouldGarbageCollect()) {
            final boolean isLockAcquired = traceFlushLock.tryLock();

            if (isLockAcquired) {
                try {
                    final long now = System.currentTimeMillis();
                    lastTraceFlushTime = now;

                    final Iterator<Map.Entry<String, SpanSet>> entryIterator = traceIdSpanSetMap.entrySet().iterator();
                    while (entryIterator.hasNext()) {
                        final Map.Entry<String, SpanSet> entry = entryIterator.next();
                        final String traceId = entry.getKey();
                        final TraceGroup traceGroup = traceIdTraceGroupCache.getIfPresent(traceId);
                        final SpanSet spanSet = entry.getValue();
                        final long traceTime = spanSet.getTimeSeen();
                        if (now - traceTime >= traceFlushInterval || isShuttingDown) {
                            final Set<Span> spans = spanSet.getSpans();
                            if (traceGroup != null) {
                                spans.forEach(span -> {
                                    fillInTraceGroupInfo(span, traceGroup);
                                    fillInServiceName(span);
                                    recordsToFlush.add(span);
                                });
                            } else {
                                LOG.warn("There are {} spans with missing trace groups. Unable to populate with trace group information.", spans.size());
                                spans.forEach(span -> {
                                    recordsToFlush.add(span);
                                    LOG.debug("Missing trace group for SpanId: {}", span.getSpanId());
                                });
                            }
                            entryIterator.remove();
                        }
                    }
                    if (!recordsToFlush.isEmpty()) {
                        LOG.info("Flushing {} records", recordsToFlush.size());
                    }
                } finally {
                    traceFlushLock.unlock();
                }
            }
        }

        return recordsToFlush;
    }

    private void fillInTraceGroupInfo(final Span span, final TraceGroup traceGroup) {
        span.setTraceGroup(traceGroup.getTraceGroup());
        span.setTraceGroupFields(traceGroup.getTraceGroupFields());
    }

    private void fillInServiceName(final Span span) {
        // For standard OTEL getServiceName() returns service name from metadata
        span.setServiceName(span.getServiceName());
    }

    private boolean shouldGarbageCollect() {
        return System.currentTimeMillis() - lastTraceFlushTime >= traceFlushInterval || isShuttingDown;
    }

    /**
     * Forces a flush of all spans in memory
     */
    @Override
    public void prepareForShutdown() {
        boolean isLockAcquired = prepareForShutdownLock.tryLock();

        if (isLockAcquired) {
            try {
                LOG.info("Preparing for shutdown, will attempt to flush {} spans", traceIdSpanSetMap.size());
                isShuttingDown = true;
            } finally {
                prepareForShutdownLock.unlock();
            }
        }
    }

    @Override
    public boolean isReadyForShutdown() {
        return traceIdSpanSetMap.isEmpty() && heldRootSpansByTraceId.isEmpty() && parkedSpansByTraceId.isEmpty();
    }

    @Override
    public void shutdown() {
        traceIdTraceGroupCache.cleanUp();
    }

    @Override
    public Collection<String> getIdentificationKeys() {
        return Collections.singleton("traceId");
    }
}
