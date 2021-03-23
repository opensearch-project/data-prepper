package com.amazon.dataprepper.plugins.prepper.oteltrace;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.OTelProtoHelper;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.RawSpan;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.RawSpanBuilder;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.RawSpanSet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;
import io.micrometer.core.instrument.Counter;
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


@DataPrepperPlugin(name = "otel_trace_raw_prepper", type = PluginType.PREPPER)
public class OTelTraceRawPrepper extends AbstractPrepper<Record<ExportTraceServiceRequest>, Record<String>> {

    private static final long SEC_TO_MILLIS = 1_000L;
    private static final Logger log = LoggerFactory.getLogger(OTelTraceRawPrepper.class);

    public static final String SPAN_PROCESSING_ERRORS = "spanProcessingErrors";
    public static final String RESOURCE_SPANS_PROCESSING_ERRORS = "resourceSpansProcessingErrors";
    public static final String TOTAL_PROCESSING_ERRORS = "totalProcessingErrors";

    private final long traceFlushInterval;
    private final long rootSpanFlushDelay;

    private final Counter spanErrorsCounter;
    private final Counter resourceSpanErrorsCounter;
    private final Counter totalProcessingErrorsCounter;

    // TODO: replace with file store, e.g. MapDB?
    // TODO: introduce a gauge to monitor the size
    private final Queue<DelayedParentSpan> delayedParentSpanQueue = new DelayQueue<>();
    // TODO: replace with file store, e.g. MapDB?
    // TODO: introduce a gauge to monitor the size
    private final Map<String, RawSpanSet> traceIdRawSpanSetMap = new ConcurrentHashMap<>();

    private final Cache<String, String> traceIdTraceGroupCache;

    private long lastTraceFlushTime = 0L;

    private final ReentrantLock traceFlushLock = new ReentrantLock();

    //TODO: https://github.com/opendistro-for-elasticsearch/simple-ingest-transformation-utility-pipeline/issues/66
    public OTelTraceRawPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
        traceFlushInterval = SEC_TO_MILLIS * pluginSetting.getLongOrDefault(
                OtelTraceRawPrepperConfig.TRACE_FLUSH_INTERVAL, OtelTraceRawPrepperConfig.DEFAULT_TG_FLUSH_INTERVAL_SEC);
        rootSpanFlushDelay = SEC_TO_MILLIS * pluginSetting.getLongOrDefault(
                OtelTraceRawPrepperConfig.ROOT_SPAN_FLUSH_DELAY, OtelTraceRawPrepperConfig.DEFAULT_ROOT_SPAN_FLUSH_DELAY_SEC);
        Preconditions.checkArgument(rootSpanFlushDelay <= traceFlushInterval,
                "rootSpanSetFlushDelay should not be greater than traceFlushInterval.");
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
                            processRawSpan(rawSpan);
                        }
                    }
                } catch (Exception ex) {
                    log.error("Unable to process invalid ResourceSpan {} :", rs, ex);
                    resourceSpanErrorsCounter.increment();
                    totalProcessingErrorsCounter.increment();
                }
            }
        }

        final List<RawSpan> rawSpans = new LinkedList<>();
        rawSpans.addAll(getTracesToFlushByRootSpan());
        rawSpans.addAll(getTracesToFlushByGarbageCollection());

        return convertRawSpansToJsonRecords(rawSpans);
    }

    private void processRawSpan(final RawSpan rawSpan) {
        if (rawSpan.getParentSpanId() == null || "".equals(rawSpan.getParentSpanId())) {
            processRootRawSpan(rawSpan);
        } else {
            processDescendantRawSpan(rawSpan);
        }
    }

    private void processRootRawSpan(final RawSpan rawSpan) {
        // TODO: flush descendants here to get rid of DelayQueue
        // TODO: safe-guard against null traceGroup for rootSpan?
        traceIdTraceGroupCache.put(rawSpan.getTraceId(), rawSpan.getTraceGroup());
        final long now = System.currentTimeMillis();
        final long nowPlusOffset = now + rootSpanFlushDelay;
        final DelayedParentSpan delayedParentSpan = new DelayedParentSpan(rawSpan, nowPlusOffset);
        delayedParentSpanQueue.add(delayedParentSpan);
    }

    private void processDescendantRawSpan(final RawSpan rawSpan) {
        traceIdRawSpanSetMap.compute(rawSpan.getTraceId(), (traceId, rawSpanSet) -> {
            if (rawSpanSet == null) {
                rawSpanSet = new RawSpanSet();
            }
            rawSpanSet.addRawSpan(rawSpan);
            return rawSpanSet;
        });
    }

    private List<Record<String>> convertRawSpansToJsonRecords(final List<RawSpan> rawSpans) {
        final List<Record<String>> records = new LinkedList<>();

        for (RawSpan rawSpan : rawSpans) {
            String rawSpanJson;
            try {
                rawSpanJson = rawSpan.toJson();
            } catch (JsonProcessingException e) {
                log.error("Unable to process invalid Span {}:", rawSpan, e);
                spanErrorsCounter.increment();
                totalProcessingErrorsCounter.increment();
                continue;
            }

            records.add(new Record<>(rawSpanJson));
        }

        return records;
    }

    private List<RawSpan> getTracesToFlushByRootSpan() {
        final List<RawSpan> recordsToFlush = new LinkedList<>();

        for (DelayedParentSpan delayedParentSpan; (delayedParentSpan = delayedParentSpanQueue.poll()) != null;) {
            RawSpan parentSpan = delayedParentSpan.getRawSpan();
            recordsToFlush.add(parentSpan);

            String traceGroup = parentSpan.getTraceGroup();
            String parentSpanTraceId = parentSpan.getTraceId();

            RawSpanSet rawSpanSet = traceIdRawSpanSetMap.get(parentSpanTraceId);
            if (rawSpanSet != null) {
                for (RawSpan rawSpan : rawSpanSet.getRawSpans()) {
                    rawSpan.setTraceGroup(traceGroup);
                    recordsToFlush.add(rawSpan);
                }

                traceIdRawSpanSetMap.remove(parentSpanTraceId);
            }
        }

        return recordsToFlush;
    }


    private List<RawSpan> getTracesToFlushByGarbageCollection() {
        final List<RawSpan> recordsToFlush = new LinkedList<>();

        if (shouldGarbageCollect()) {
            boolean isLockAcquired = traceFlushLock.tryLock();

            if (isLockAcquired) {
                try {
                    final long now = System.currentTimeMillis();
                    lastTraceFlushTime = now;

                    Iterator<Map.Entry<String, RawSpanSet>> entryIterator = traceIdRawSpanSetMap.entrySet().iterator();
                    while (entryIterator.hasNext()) {
                        Map.Entry<String, RawSpanSet> entry = entryIterator.next();
                        String traceId = entry.getKey();
                        String traceGroup = traceIdTraceGroupCache.getIfPresent(traceId);
                        RawSpanSet rawSpanSet = entry.getValue();
                        long traceTime = rawSpanSet.getTimeSeen();
                        if (now - traceTime >= traceFlushInterval) {
                            Set<RawSpan> rawSpans = rawSpanSet.getRawSpans();
                            if (traceGroup != null) {
                                rawSpans.forEach(rawSpan -> {
                                    rawSpan.setTraceGroup(traceGroup);
                                    recordsToFlush.add(rawSpan);
                                });
                            } else {
                                rawSpans.forEach(rawSpan -> {
                                    recordsToFlush.add(rawSpan);
                                    log.warn("Missing trace group for SpanId: {}", rawSpan.getSpanId());
                                });
                            }

                            entryIterator.remove();
                        }
                    }
                    if (recordsToFlush.size() > 0) {
                        log.info("Flushing {} records due to GC", recordsToFlush.size());
                    }
                } finally {
                    traceFlushLock.unlock();
                }
            }
        }

        return recordsToFlush;
    }

    private boolean shouldGarbageCollect() {
        return System.currentTimeMillis() - lastTraceFlushTime >= traceFlushInterval;
    }

    @Override
    public void shutdown() {
        traceIdTraceGroupCache.cleanUp();
    }

    class DelayedParentSpan implements Delayed {
        private RawSpan rawSpan;
        private long delayTime;

        public DelayedParentSpan(RawSpan rawSpan, long startTime) {
            this.rawSpan = rawSpan;
            this.delayTime = startTime;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = delayTime - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Ints.saturatedCast(
                    this.delayTime - ((DelayedParentSpan) o).delayTime);
        }

        public RawSpan getRawSpan() {
            return rawSpan;
        }
    }
}
