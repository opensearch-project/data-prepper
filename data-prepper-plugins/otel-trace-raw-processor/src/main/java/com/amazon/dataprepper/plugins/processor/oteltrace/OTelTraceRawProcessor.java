/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.oteltrace;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.plugins.processor.oteltrace.model.SpanSet;
import com.amazon.dataprepper.plugins.processor.oteltrace.model.TraceGroup;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.micrometer.core.instrument.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


@DataPrepperPlugin(name = "otel_trace_raw", pluginType = Prepper.class)
public class OTelTraceRawProcessor extends AbstractPrepper<Record<Span>, Record<Span>> {
    private static final long SEC_TO_MILLIS = 1_000L;
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceRawProcessor.class);

    private final long traceFlushInterval;

    private final Map<String, SpanSet> traceIdSpanSetMap = new ConcurrentHashMap<>();

    private final Cache<String, TraceGroup> traceIdTraceGroupCache;

    private long lastTraceFlushTime = 0L;

    private final ReentrantLock traceFlushLock = new ReentrantLock();
    private final ReentrantLock prepareForShutdownLock = new ReentrantLock();

    private volatile boolean isShuttingDown = false;

    public OTelTraceRawProcessor(final PluginSetting pluginSetting) {
        super(pluginSetting);
        traceFlushInterval = SEC_TO_MILLIS * pluginSetting.getLongOrDefault(
                OtelTraceRawProcessorConfig.TRACE_FLUSH_INTERVAL, OtelTraceRawProcessorConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        final int numProcessWorkers = pluginSetting.getNumberOfProcessWorkers();
        traceIdTraceGroupCache = CacheBuilder.newBuilder()
                .concurrencyLevel(numProcessWorkers)
                .maximumSize(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE)
                .expireAfterWrite(OtelTraceRawProcessorConfig.DEFAULT_TRACE_ID_TTL_SEC, TimeUnit.SECONDS)
                .build();
    }

    /**
     * execute the prepper logic which could potentially modify the incoming record. The level to which the record has
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
        }

        processedSpans.addAll(getTracesToFlushByGarbageCollection());

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
            final Optional<Span> populatedChildSpanOptional = processChildSpan(span);
            if (populatedChildSpanOptional.isPresent()) {
                spanSet.add(populatedChildSpanOptional.get());
            }
        }
    }

    /**
     * Retrieves all child spans from memory and returns them as a set with the root span.
     * Also adds an entry to the traceID cache so that later child spans can be tagged,
     * in the case where a child span is processed AFTER the root span.
     *
     * @param parentSpan
     * @return List containing root span, along with any child spans that have already been processed.
     */
    private List<Span> processRootSpan(final Span parentSpan) {
        final TraceGroup traceGroup = new TraceGroup.TraceGroupBuilder().setFromSpan(parentSpan).build();
        final String parentSpanTraceId = parentSpan.getTraceId();
        traceIdTraceGroupCache.put(parentSpanTraceId, traceGroup);

        final List<Span> recordsToFlush = new LinkedList<>();
        recordsToFlush.add(parentSpan);

        final SpanSet spanSet = traceIdSpanSetMap.get(parentSpanTraceId);
        if (spanSet != null) {
            for (final Span span : spanSet.getSpans()) {
                fillInTraceGroupInfo(span, traceGroup);
                recordsToFlush.add(span);
            }

            traceIdSpanSetMap.remove(parentSpanTraceId);
        }

        return recordsToFlush;
    }

    /**
     * Attempts to populate the traceGroup of the child span by fetching from a cache. If the traceGroup is not in the cache,
     * the child span is kept in memory to be populated when its corresponding root span arrives.
     *
     * @param childSpan
     * @return Optional containing childSpan if its traceGroup is in memory, otherwise an empty Optional
     */
    private Optional<Span> processChildSpan(final Span childSpan) {
        final String childSpanTraceId = childSpan.getTraceId();
        final TraceGroup traceGroup = traceIdTraceGroupCache.getIfPresent(childSpanTraceId);

        if (traceGroup != null) {
            fillInTraceGroupInfo(childSpan, traceGroup);
            return Optional.of(childSpan);
        } else {
            traceIdSpanSetMap.compute(childSpanTraceId, (traceId, spanSet) -> {
                if (spanSet == null) {
                    spanSet = new SpanSet();
                }
                spanSet.addSpan(childSpan);
                return spanSet;
            });

            return Optional.empty();
        }
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
                                    recordsToFlush.add(span);
                                });
                            } else {
                                spans.forEach(span -> {
                                    recordsToFlush.add(span);
                                    LOG.warn("Missing trace group for SpanId: {}", span.getSpanId());
                                });
                            }

                            entryIterator.remove();
                        }
                    }
                    if (recordsToFlush.size() > 0) {
                        LOG.info("Flushing {} records due to GC", recordsToFlush.size());
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
        return traceIdSpanSetMap.isEmpty();
    }

    @Override
    public void shutdown() {
        traceIdTraceGroupCache.cleanUp();
    }
}
