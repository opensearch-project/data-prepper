/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
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


@DataPrepperPlugin(name = "otel_traces", deprecatedName = "otel_trace_raw", pluginType = Processor.class, pluginConfigurationType = OtelTraceRawProcessorConfig.class)
public class OTelTraceRawProcessor extends AbstractProcessor<Record<Span>, Record<Span>> implements RequiresPeerForwarding {
    private static final long SEC_TO_MILLIS = 1_000L;
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceRawProcessor.class);
    public static final String TRACE_GROUP_CACHE_COUNT_METRIC_NAME = "traceGroupCacheCount";
    public static final String SPAN_SET_COUNT_METRIC_NAME = "spanSetCount";

    private final long traceFlushInterval;

    private final Map<String, SpanSet> traceIdSpanSetMap = new ConcurrentHashMap<>();

    private final Cache<String, TraceGroup> traceIdTraceGroupCache;

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
        traceIdTraceGroupCache = Caffeine.newBuilder()
          .maximumSize(otelTraceRawProcessorConfig.getTraceGroupCacheMaxSize())
          .expireAfterWrite(otelTraceRawProcessorConfig.getTraceGroupCacheTimeToLive().toMillis(), TimeUnit.MILLISECONDS)
          .build();


        pluginMetrics.gauge(TRACE_GROUP_CACHE_COUNT_METRIC_NAME, traceIdTraceGroupCache, cache -> (double) cache.estimatedSize());
        pluginMetrics.gauge(SPAN_SET_COUNT_METRIC_NAME, traceIdSpanSetMap, cache -> (double) cache.size());

        LOG.info("Configured Trace Raw Processor with a trace flush interval of {} ms.", traceFlushInterval);
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
          populatedChildSpanOptional.ifPresent(spanSet::add);
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
        final TraceGroup traceGroup = TraceGroup.fromSpan(parentSpan);
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
        return traceIdSpanSetMap.isEmpty();
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
