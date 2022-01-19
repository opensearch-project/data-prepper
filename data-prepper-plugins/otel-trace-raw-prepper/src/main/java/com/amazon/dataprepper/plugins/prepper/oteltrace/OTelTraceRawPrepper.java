/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.oteltrace;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.OTelProtoHelper;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.RawSpan;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.RawSpanBuilder;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.RawSpanSet;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.TraceGroup;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.util.StringUtils;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
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


@DataPrepperPlugin(name = "otel_trace_raw_prepper", pluginType = Prepper.class)
public class OTelTraceRawPrepper extends AbstractPrepper<Record<ExportTraceServiceRequest>, Record<String>> {
    private static final long SEC_TO_MILLIS = 1_000L;
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceRawPrepper.class);

    public static final String SPAN_PROCESSING_ERRORS = "spanProcessingErrors";
    public static final String RESOURCE_SPANS_PROCESSING_ERRORS = "resourceSpansProcessingErrors";
    public static final String TOTAL_PROCESSING_ERRORS = "totalProcessingErrors";

    private final long traceFlushInterval;

    private final Counter spanErrorsCounter;
    private final Counter resourceSpanErrorsCounter;
    private final Counter totalProcessingErrorsCounter;

    private final Map<String, RawSpanSet> traceIdRawSpanSetMap = new ConcurrentHashMap<>();

    private final Cache<String, TraceGroup> traceIdTraceGroupCache;

    private long lastTraceFlushTime = 0L;

    private final ReentrantLock traceFlushLock = new ReentrantLock();
    private final ReentrantLock prepareForShutdownLock = new ReentrantLock();

    private volatile boolean isShuttingDown = false;

    public OTelTraceRawPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
        traceFlushInterval = SEC_TO_MILLIS * pluginSetting.getLongOrDefault(
                OtelTraceRawPrepperConfig.TRACE_FLUSH_INTERVAL, OtelTraceRawPrepperConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        final int numProcessWorkers = pluginSetting.getNumberOfProcessWorkers();
        traceIdTraceGroupCache = CacheBuilder.newBuilder()
                .concurrencyLevel(numProcessWorkers)
                .maximumSize(OtelTraceRawPrepperConfig.MAX_TRACE_ID_CACHE_SIZE)
                .expireAfterWrite(OtelTraceRawPrepperConfig.DEFAULT_TRACE_ID_TTL_SEC, TimeUnit.SECONDS)
                .build();
        spanErrorsCounter = pluginMetrics.counter(SPAN_PROCESSING_ERRORS);
        resourceSpanErrorsCounter = pluginMetrics.counter(RESOURCE_SPANS_PROCESSING_ERRORS);
        totalProcessingErrorsCounter = pluginMetrics.counter(TOTAL_PROCESSING_ERRORS);
    }

    /**
     * execute the prepper logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    @Override
    public Collection<Record<String>> doExecute(Collection<Record<ExportTraceServiceRequest>> records) {
        final List<RawSpan> rawSpans = new LinkedList<>();

        for (Record<ExportTraceServiceRequest> ets : records) {
            for (ResourceSpans rs : ets.getData().getResourceSpansList()) {
                try {
                    final String serviceName = OTelProtoHelper.getServiceName(rs.getResource()).orElse(null);
                    final Map<String, Object> resourceAttributes = OTelProtoHelper.getResourceAttributes(rs.getResource());
                    for (InstrumentationLibrarySpans is : rs.getInstrumentationLibrarySpansList()) {
                        for (Span sp : is.getSpansList()) {
                            final RawSpan rawSpan = new RawSpanBuilder()
                                    .setFromSpan(sp, is.getInstrumentationLibrary(), serviceName, resourceAttributes)
                                    .build();

                            processRawSpan(rawSpan, rawSpans);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error("Unable to process invalid ResourceSpan {} :", rs, ex);
                    resourceSpanErrorsCounter.increment();
                    totalProcessingErrorsCounter.increment();
                }
            }
        }

        rawSpans.addAll(getTracesToFlushByGarbageCollection());

        return convertRawSpansToJsonRecords(rawSpans);
    }

    /**
     * Branching logic to handle root and child spans.
     * A root span is the first span of a trace, it has no parentSpanId.
     *
     * @param rawSpan Span to be evaluated
     * @param spanSet Collection to insert spans to
     */
    private void processRawSpan(final RawSpan rawSpan, final Collection<RawSpan> spanSet) {
        if (StringUtils.isBlank(rawSpan.getParentSpanId())) {
            final List<RawSpan> rootSpanAndChildren = processRootSpan(rawSpan);
            spanSet.addAll(rootSpanAndChildren);
        } else {
            final Optional<RawSpan> populatedChildSpanOptional = processChildSpan(rawSpan);
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
    private List<RawSpan> processRootSpan(final RawSpan parentSpan) {
        traceIdTraceGroupCache.put(parentSpan.getTraceId(), parentSpan.getTraceGroup());

        final List<RawSpan> recordsToFlush = new LinkedList<>();
        recordsToFlush.add(parentSpan);

        final TraceGroup traceGroup = parentSpan.getTraceGroup();
        final String parentSpanTraceId = parentSpan.getTraceId();

        final RawSpanSet rawSpanSet = traceIdRawSpanSetMap.get(parentSpanTraceId);
        if (rawSpanSet != null) {
            for (final RawSpan rawSpan : rawSpanSet.getRawSpans()) {
                rawSpan.setTraceGroup(traceGroup);
                recordsToFlush.add(rawSpan);
            }

            traceIdRawSpanSetMap.remove(parentSpanTraceId);
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
    private Optional<RawSpan> processChildSpan(final RawSpan childSpan) {
        final TraceGroup traceGroup = traceIdTraceGroupCache.getIfPresent(childSpan.getTraceId());

        if (traceGroup != null) {
            childSpan.setTraceGroup(traceGroup);
            return Optional.of(childSpan);
        } else {
            traceIdRawSpanSetMap.compute(childSpan.getTraceId(), (traceId, rawSpanSet) -> {
                if (rawSpanSet == null) {
                    rawSpanSet = new RawSpanSet();
                }
                rawSpanSet.addRawSpan(childSpan);
                return rawSpanSet;
            });

            return Optional.empty();
        }
    }

    private List<Record<String>> convertRawSpansToJsonRecords(final List<RawSpan> rawSpans) {
        final List<Record<String>> records = new LinkedList<>();

        for (RawSpan rawSpan : rawSpans) {
            String rawSpanJson;
            try {
                rawSpanJson = rawSpan.toJson();
            } catch (JsonProcessingException e) {
                LOG.error("Unable to process invalid Span {}:", rawSpan, e);
                spanErrorsCounter.increment();
                totalProcessingErrorsCounter.increment();
                continue;
            }

            records.add(new Record<>(rawSpanJson));
        }

        return records;
    }

    /**
     * Periodically flush spans from memory. Typically all spans of a trace are written
     * once the trace's root span arrives, however some child spans my arrive after the root span.
     * This method ensures "orphaned" child spans are eventually flushed from memory.
     * @return List of RawSpans to be sent down the pipeline
     */
    private List<RawSpan> getTracesToFlushByGarbageCollection() {
        final List<RawSpan> recordsToFlush = new LinkedList<>();

        if (shouldGarbageCollect()) {
            final boolean isLockAcquired = traceFlushLock.tryLock();

            if (isLockAcquired) {
                try {
                    final long now = System.currentTimeMillis();
                    lastTraceFlushTime = now;

                    final Iterator<Map.Entry<String, RawSpanSet>> entryIterator = traceIdRawSpanSetMap.entrySet().iterator();
                    while (entryIterator.hasNext()) {
                        final Map.Entry<String, RawSpanSet> entry = entryIterator.next();
                        final String traceId = entry.getKey();
                        final TraceGroup traceGroup = traceIdTraceGroupCache.getIfPresent(traceId);
                        final RawSpanSet rawSpanSet = entry.getValue();
                        final long traceTime = rawSpanSet.getTimeSeen();
                        if (now - traceTime >= traceFlushInterval || isShuttingDown) {
                            final Set<RawSpan> rawSpans = rawSpanSet.getRawSpans();
                            if (traceGroup != null) {
                                rawSpans.forEach(rawSpan -> {
                                    rawSpan.setTraceGroup(traceGroup);
                                    recordsToFlush.add(rawSpan);
                                });
                            } else {
                                rawSpans.forEach(rawSpan -> {
                                    recordsToFlush.add(rawSpan);
                                    LOG.warn("Missing trace group for SpanId: {}", rawSpan.getSpanId());
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
                LOG.info("Preparing for shutdown, will attempt to flush {} spans", traceIdRawSpanSetMap.size());
                isShuttingDown = true;
            } finally {
                prepareForShutdownLock.unlock();
            }
        }
    }

    @Override
    public boolean isReadyForShutdown() {
        return traceIdRawSpanSetMap.isEmpty();
    }

    @Override
    public void shutdown() {
        traceIdTraceGroupCache.cleanUp();
    }
}
