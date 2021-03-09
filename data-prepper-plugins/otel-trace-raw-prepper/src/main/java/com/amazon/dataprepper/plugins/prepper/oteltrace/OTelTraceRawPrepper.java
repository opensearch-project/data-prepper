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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;


@DataPrepperPlugin(name = "otel_trace_raw_prepper", type = PluginType.PREPPER)
public class OTelTraceRawPrepper extends AbstractPrepper<Record<ExportTraceServiceRequest>, Record<String>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String INSTRUMENTATION_LIBRARY_SPANS = "instrumentationLibrarySpans";
    private static final String INSTRUMENTATION_LIBRARY = "instrumentationLibrary";
    private static final String SPANS = "spans";
    private static final String RESOURCE = "resource";
    private static final String ATTRIBUTES = "attributes";
    private static final String START_TIME_UNIX_NANOS = "startTimeUnixNano";
    private static final String END_TIME_UNIX_NANOS = "endTimeUnixNano";
    private static final String START_TIME = "startTime";
    private static final String END_TIME = "endTime";
    private static final BigDecimal MILLIS_TO_NANOS = new BigDecimal(1_000_000);
    private static final BigDecimal SEC_TO_MILLIS = new BigDecimal(1_000);
    private static final Logger log = LoggerFactory.getLogger(OTelTraceRawPrepper.class);

    public static final String SPAN_PROCESSING_ERRORS = "spanProcessingErrors";
    public static final String RESOURCE_SPANS_PROCESSING_ERRORS = "resourceSpansProcessingErrors";
    public static final String TOTAL_PROCESSING_ERRORS = "totalProcessingErrors";

    private final long gcInterval;
    private final long parentSpanFlushDelay;

    private final Counter spanErrorsCounter;
    private final Counter resourceSpanErrorsCounter;
    private final Counter totalProcessingErrorsCounter;

    private final Queue<DelayedParentSpan> delayedParentSpanQueue = new DelayQueue<>();
    private final Map<String, RawSpanSet> traceIdRawSpanSetMap = new ConcurrentHashMap<>();

    private long lastGarbageCollectionTime = 0L;


    //TODO: https://github.com/opendistro-for-elasticsearch/simple-ingest-transformation-utility-pipeline/issues/66
    public OTelTraceRawPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
        gcInterval = pluginSetting.getLongOrDefault(OtelTraceRawPrepperConfig.GC_INTERVAL, OtelTraceRawPrepperConfig.DEFAULT_GC_INTERVAL_MS);
        parentSpanFlushDelay = pluginSetting.getLongOrDefault(OtelTraceRawPrepperConfig.PARENT_SPAN_FLUSH_DELAY, OtelTraceRawPrepperConfig.DEFAULT_PARENT_SPAN_FLUSH_DELAY_MS);
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
        // TODO: Track messages in and messages out (for those that get TTL'd by mapdb)

        rawSpans.addAll(getTracesToFlushByParentSpan());
        rawSpans.addAll(getTracesToFlushByGarbageCollection());

        for (Record<ExportTraceServiceRequest> ets : records) {
            for (ResourceSpans rs : ets.getData().getResourceSpansList()) {
                try {
                    final String serviceName = OTelProtoHelper.getServiceName(rs.getResource()).orElse(null);
                    final Map<String, Object> resourceAttributes = OTelProtoHelper.getResourceAttributes(rs.getResource());
                    for (InstrumentationLibrarySpans is : rs.getInstrumentationLibrarySpansList()) {
                        for (Span sp : is.getSpansList()) {
                            final long now = System.currentTimeMillis();
                            final long nowPlusOffset = now + parentSpanFlushDelay;

                            final RawSpan rawSpan = new RawSpanBuilder()
                                    .setFromSpan(sp, is.getInstrumentationLibrary(), serviceName, resourceAttributes)
                                    .build();
                            final String traceId = rawSpan.getTraceId();

                            if (rawSpan.getParentSpanId() == null || "".equals(rawSpan.getParentSpanId())) {
                                // Handle parent spans
                                final DelayedParentSpan delayedParentSpan = new DelayedParentSpan(rawSpan, nowPlusOffset);
                                delayedParentSpanQueue.add(delayedParentSpan);
                            } else {
                                // Handle child spans
                                traceIdRawSpanSetMap.putIfAbsent(traceId, new RawSpanSet());
                                traceIdRawSpanSetMap.get(traceId).addRawSpan(rawSpan);
                            }
                        }
                    }
                } catch (Exception ex) {
                    log.error("Unable to process invalid ResourceSpan {} :", rs, ex);
                    resourceSpanErrorsCounter.increment();
                    totalProcessingErrorsCounter.increment();
                }
            }
        }

        return convertRawSpansToJsonRecords(rawSpans);
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

    private List<RawSpan> getTracesToFlushByParentSpan() {
        final List<RawSpan> recordsToFlush = new LinkedList<>();

        // TODO: can change this do "do while" and remove peek
        while (delayedParentSpanQueue.peek() != null) {
            DelayedParentSpan delayedParentSpan = delayedParentSpanQueue.poll();
            if (delayedParentSpan == null) {
                break;
            }

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
            final long now = System.currentTimeMillis();
            lastGarbageCollectionTime = now;

            Set<String> keys = new HashSet<>(traceIdRawSpanSetMap.keySet());
            for (String traceId : keys) {
                long traceTime = traceIdRawSpanSetMap.get(traceId).getTimeSeen();
                if (now - traceTime >= gcInterval) {
                    Set<RawSpan> rawSpans = traceIdRawSpanSetMap.get(traceId).getRawSpans();
                    for (RawSpan rawSpan : rawSpans) {
                        rawSpan.setTraceGroup("ERROR");
                        recordsToFlush.add(rawSpan);
                    }

                    traceIdRawSpanSetMap.remove(traceId);
                }
            }
            // TODO: Replace
            log.error("Flushing {} records due to GC", recordsToFlush.size());
        }

        return recordsToFlush;
    }

    private boolean shouldGarbageCollect() {
        return System.currentTimeMillis() - lastGarbageCollectionTime >= gcInterval;
    }

    @Override
    public void shutdown() {

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
