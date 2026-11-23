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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.dataprepper.plugins.processor.oteltrace.model.SpanSet;
import org.opensearch.dataprepper.plugins.processor.oteltrace.model.TraceGroup;
import org.opensearch.dataprepper.plugins.otel.common.OTelSpanDerivationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


@DataPrepperPlugin(name = "otel_traces", deprecatedName = "otel_trace_raw", pluginType = Processor.class, pluginConfigurationType = OtelTraceRawProcessorConfig.class)
public class OTelTraceRawProcessor extends AbstractProcessor<Record<Span>, Record<Span>> implements RequiresPeerForwarding {
    private static final long SEC_TO_MILLIS = 1_000L;
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceRawProcessor.class);
    public static final String GENAI_ROOT_ENRICHMENT_COUNT_METRIC_NAME = "genAiRootEnrichmentCount";
    public static final String GENAI_ROOT_ENRICHMENT_SPAN_ID_COUNT_METRIC_NAME = "genAiRootEnrichmentSpanIdCount";
    public static final String GENAI_HELD_ROOT_COUNT_METRIC_NAME = "genAiHeldRootCount";
    public static final String GENAI_HELD_ROOT_EXPIRED_COUNT_METRIC_NAME = "genAiHeldRootExpiredCount";
    public static final String GENAI_HELD_ROOT_SHUTDOWN_COUNT_METRIC_NAME = "genAiHeldRootShutdownCount";
    public static final String GENAI_HELD_ROOT_EVICTED_COUNT_METRIC_NAME = "genAiHeldRootEvictedCount";
    public static final String GENAI_HELD_ROOT_REPLACED_COUNT_METRIC_NAME = "genAiHeldRootReplacedCount";
    public static final String TRACE_GROUP_CACHE_COUNT_METRIC_NAME = "traceGroupCacheCount";
    public static final String SPAN_SET_COUNT_METRIC_NAME = "spanSetCount";
    public static final String GENAI_COVERAGE_COUNT_METRIC_NAME = "genAiCoverageCount";
    public static final String GENAI_COVERAGE_SPAN_ID_COUNT_METRIC_NAME = "genAiCoverageSpanIdCount";

    private final long traceFlushInterval;

    private final Map<String, SpanSet> traceIdSpanSetMap = new ConcurrentHashMap<>();

    private final Cache<String, TraceGroup> traceIdTraceGroupCache;

    /** Span ids covered by GenAI spans, per trace, for spans that arrive after their ancestors. */
    private final GenAiCoverage genAiCoverage;
    private final GenAiRootPropagation genAiRootPropagation;
    private final GenAiRootMarking genAiRootMarking;
    private final boolean genAiEnrichmentEnabled;

    /** Per-trace values and token usage of children that trace roots are enriched from. */
    private final GenAiRootEnrichmentState genAiRootEnrichmentState;
    private final long genAiRootHoldMillis;
    private final long genAiRootHoldMaxSize;

    /** GenAI roots held back for late children that could complete their model or token usage. */
    private final Map<String, HeldRootSpan> heldRootSpansByTraceId = new ConcurrentHashMap<>();
    /**
     * The same held roots in the order they were held, which is the order of their deadlines.
     * Holding and releasing a root changes both collections under this deque's lock.
     */
    private final Deque<HeldRootSpan> heldRootSpansByDeadline = new ArrayDeque<>();
    private final Counter heldRootExpiredCounter;
    private final Counter heldRootShutdownCounter;
    private final Counter heldRootEvictedCounter;
    private final Counter heldRootReplacedCounter;

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
        genAiRootMarking = Objects.requireNonNullElse(
                otelTraceRawProcessorConfig.getGenAiRootMarking(), GenAiRootMarking.OUTERMOST);
        // Outlives the orphan flush: spans that arrive late are held up to two flush intervals.
        // Only outermost marking reads earlier batches, so the other modes keep no cache.
        genAiCoverage = genAiEnrichmentEnabled && genAiRootMarking == GenAiRootMarking.OUTERMOST
                ? GenAiCoverage.create(otelTraceRawProcessorConfig.getGenAiCoverageCacheMaxSize(),
                        Duration.ofMillis(2 * traceFlushInterval).plus(traceGroupCacheTimeToLive))
                : GenAiCoverage.NONE;
        // Root enrichment is about propagation, not marking, so it applies in every marking mode.
        genAiRootEnrichmentState = genAiEnrichmentEnabled
                ? GenAiRootEnrichmentState.create(otelTraceRawProcessorConfig.getGenAiRootEnrichmentCacheMaxSize(),
                        Duration.ofMillis(2 * traceFlushInterval).plus(traceGroupCacheTimeToLive))
                : GenAiRootEnrichmentState.NONE;
        genAiRootHoldMillis = genAiEnrichmentEnabled
                ? Objects.requireNonNullElse(otelTraceRawProcessorConfig.getGenAiRootHold(), Duration.ZERO).toMillis()
                : 0L;
        genAiRootHoldMaxSize = otelTraceRawProcessorConfig.getGenAiRootHoldMaxSize();
        heldRootExpiredCounter = pluginMetrics.counter(GENAI_HELD_ROOT_EXPIRED_COUNT_METRIC_NAME);
        heldRootShutdownCounter = pluginMetrics.counter(GENAI_HELD_ROOT_SHUTDOWN_COUNT_METRIC_NAME);
        heldRootEvictedCounter = pluginMetrics.counter(GENAI_HELD_ROOT_EVICTED_COUNT_METRIC_NAME);
        heldRootReplacedCounter = pluginMetrics.counter(GENAI_HELD_ROOT_REPLACED_COUNT_METRIC_NAME);

        pluginMetrics.gauge(TRACE_GROUP_CACHE_COUNT_METRIC_NAME, traceIdTraceGroupCache, cache -> (double) cache.estimatedSize());
        pluginMetrics.gauge(SPAN_SET_COUNT_METRIC_NAME, traceIdSpanSetMap, cache -> (double) cache.size());
        pluginMetrics.gauge(GENAI_COVERAGE_COUNT_METRIC_NAME, genAiCoverage, coverage -> (double) coverage.estimatedSize());
        pluginMetrics.gauge(GENAI_COVERAGE_SPAN_ID_COUNT_METRIC_NAME, genAiCoverage, coverage -> (double) coverage.spanIdCount());
        pluginMetrics.gauge(GENAI_ROOT_ENRICHMENT_COUNT_METRIC_NAME, genAiRootEnrichmentState,
                state -> (double) state.estimatedSize());
        pluginMetrics.gauge(GENAI_ROOT_ENRICHMENT_SPAN_ID_COUNT_METRIC_NAME, genAiRootEnrichmentState,
                state -> (double) state.spanIdCount());
        pluginMetrics.gauge(GENAI_HELD_ROOT_COUNT_METRIC_NAME, heldRootSpansByTraceId, map -> (double) map.size());

        LOG.info("Configured Trace Raw Processor with a trace flush interval of {} ms, GenAI enrichment {}.",
                traceFlushInterval, genAiEnrichmentEnabled
                        ? "enabled with root propagation " + genAiRootPropagation + " and root marking " + genAiRootMarking
                        : "disabled");
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

        processedSpans.addAll(getTracesToFlushByGarbageCollection());

        if (genAiEnrichmentEnabled) {
            // Held roots whose hold expired, and all of them on shutdown, join the batch, so they are
            // enriched and marked like any other root.
            processedSpans.addAll(releaseHeldRootSpans());
            // Normalize attributes and strip conflicting sub-keys once, for both passes.
            GenAiEnrichmentHelper.normalizeBatch(processedSpans);
            // Record the batch's children and enrich its roots from every child recorded so far.
            GenAiRootEnrichment.enrichNormalizedRoots(processedSpans, genAiRootEnrichmentState, genAiRootPropagation);
            // Mark agent roots and enrich roots from the batch.
            GenAiEnrichmentHelper.enrichNormalizedBatch(processedSpans, genAiCoverage, genAiRootPropagation, genAiRootMarking);
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
     * With {@code outermost} marking, a root that is a GenAI span or marked at the source is recorded
     * in the GenAI coverage cache before its trace group is cached: cover before put, and put before
     * claim, so any child that can observe the trace group, by its first lookup or by its re-check,
     * also observes its root and is not marked as a second agent root. A non-GenAI root is never
     * covered, so the agent span below it is still marked.
     * A GenAI root that is still missing data its late children could provide is held instead of
     * being returned; its children keep flowing through the trace group cache. It is released when
     * its hold expires or on shutdown by {@link #releaseHeldRootSpans()}, or here when a newer root
     * of the same trace replaces it or when holding this root would exceed
     * {@code genai_root_hold_max_size}.
     *
     * @param parentSpan
     * @return List containing root span, along with any child spans that have already been processed.
     */
    private List<Span> processRootSpan(final Span parentSpan) {
        final TraceGroup traceGroup = TraceGroup.fromSpan(parentSpan);
        final String parentSpanTraceId = parentSpan.getTraceId();
        if (genAiEnrichmentEnabled && genAiRootMarking == GenAiRootMarking.OUTERMOST
                && GenAiEnrichmentHelper.isGenAiOrMarkedSpan(parentSpan)) {
            genAiCoverage.coverGenAiRoot(parentSpanTraceId, parentSpan.getSpanId());
        }
        traceIdTraceGroupCache.put(parentSpanTraceId, traceGroup);

        final List<Span> recordsToFlush = new LinkedList<>();

        final SpanSet claimedSpanSet = traceIdSpanSetMap.remove(parentSpanTraceId);
        if (shouldHoldRootSpan(parentSpan, claimedSpanSet)) {
            recordsToFlush.addAll(holdRootSpan(parentSpan));
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
        return GenAiRootEnrichment.rootAwaitsEnrichment(rootSpan, genAiRootEnrichmentState, genAiRootPropagation);
    }

    /**
     * Holds a root until its deadline, and returns the roots this releases: an earlier held root
     * of the same trace, which it replaces, and the roots held longest when holding it would exceed
     * {@code genai_root_hold_max_size}. Released roots join the batch being processed.
     */
    private List<Span> holdRootSpan(final Span rootSpan) {
        final List<Span> releasedRootSpans = new ArrayList<>();
        synchronized (heldRootSpansByDeadline) {
            final HeldRootSpan heldRootSpan = new HeldRootSpan(rootSpan, System.currentTimeMillis() + genAiRootHoldMillis);
            final HeldRootSpan replacedRootSpan = heldRootSpansByTraceId.put(heldRootSpan.traceId, heldRootSpan);
            if (replacedRootSpan != null) {
                heldRootSpansByDeadline.remove(replacedRootSpan);
                releasedRootSpans.add(replacedRootSpan.span);
                heldRootReplacedCounter.increment();
            }
            heldRootSpansByDeadline.addLast(heldRootSpan);
            while (heldRootSpansByTraceId.size() > genAiRootHoldMaxSize) {
                final HeldRootSpan oldestRootSpan = heldRootSpansByDeadline.pollFirst();
                if (oldestRootSpan == null) {
                    break;
                }
                if (heldRootSpansByTraceId.remove(oldestRootSpan.traceId, oldestRootSpan)) {
                    releasedRootSpans.add(oldestRootSpan.span);
                    heldRootEvictedCounter.increment();
                }
            }
        }
        return releasedRootSpans;
    }

    /**
     * Releases the held GenAI roots whose hold expired, enriched with whatever the trace state has
     * by then, and all of them on shutdown. Each root is claimed with an atomic remove, so it is
     * released exactly once.
     */
    private List<Span> releaseHeldRootSpans() {
        if (heldRootSpansByTraceId.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Span> releasedRootSpans = new ArrayList<>();
        final long now = System.currentTimeMillis();
        synchronized (heldRootSpansByDeadline) {
            while (!heldRootSpansByDeadline.isEmpty()
                    && (isShuttingDown || heldRootSpansByDeadline.peekFirst().deadlineMillis <= now)) {
                final HeldRootSpan heldRootSpan = heldRootSpansByDeadline.pollFirst();
                if (heldRootSpansByTraceId.remove(heldRootSpan.traceId, heldRootSpan)) {
                    releasedRootSpans.add(heldRootSpan.span);
                    if (isShuttingDown) {
                        heldRootShutdownCounter.increment();
                    } else {
                        heldRootExpiredCounter.increment();
                    }
                }
            }
        }
        return releasedRootSpans;
    }

    /** A root span held back for late children, and the time its hold expires. */
    private static final class HeldRootSpan {
        private final Span span;
        private final String traceId;
        private final long deadlineMillis;

        private HeldRootSpan(final Span span, final long deadlineMillis) {
            this.span = span;
            this.traceId = span.getTraceId();
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
        return traceIdSpanSetMap.isEmpty() && heldRootSpansByTraceId.isEmpty();
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
