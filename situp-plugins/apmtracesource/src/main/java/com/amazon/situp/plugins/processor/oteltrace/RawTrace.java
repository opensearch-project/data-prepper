package com.amazon.situp.plugins.processor.oteltrace;

import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import org.apache.commons.codec.binary.Hex;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class RawTrace {

    private static final BigDecimal MILLIS_TO_NANOS = new BigDecimal(1_000_000);
    private static final BigDecimal SEC_TO_MILLIS = new BigDecimal(1_000);

    private final String traceId;
    private final String spanId;
    private final String parentSpanId;
    private final String name;
    private final String startTime;
    private final String endTime;
    private final String instrumentationName;
    private final String instrumentationVersion;
    private final String serviceName;
    private final Map<String, String> resourceAttributes;


    private RawTrace(RawTraceBuilder builder) {
        this.traceId = builder.traceId;
        this.spanId = builder.spanId;
        this.parentSpanId = builder.parentSpanId;
        this.name = builder.name;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.instrumentationName = builder.instrumentationName;
        this.instrumentationVersion = builder.instrumentationVersion;
        this.serviceName = builder.serviceName;
        this.resourceAttributes = builder.resourceAttributes;
    }

    /**
     * Builder class
     **/
    public static class RawTraceBuilder {
        private String traceId;
        private String spanId;
        private String parentSpanId;
        private String name;
        private String startTime;
        private String endTime;
        private String instrumentationName;
        private String instrumentationVersion;
        private String serviceName;
        private Map<String, String> resourceAttributes;

        public RawTraceBuilder() {
        }

        public RawTraceBuilder setTraceId(final String traceId) {
            checkNotNull(traceId, "traceId cannot be null");
            this.traceId = traceId;
            return this;
        }

        public RawTraceBuilder setSpanId(final String spanId) {
            checkNotNull(spanId, "spanId cannot be null");
            this.spanId = spanId;
            return this;
        }

        public RawTraceBuilder setParentSpanId(final String parentSpanId) {
            this.parentSpanId = parentSpanId;
            return this;
        }

        public RawTraceBuilder setName(final String name) {
            this.name = name;
            return this;
        }

        public RawTraceBuilder setStartTime(final String startTime) {
            this.startTime = startTime;
            return this;
        }

        public RawTraceBuilder setEndTime(final String endTime) {
            this.endTime = endTime;
            return this;
        }

        public RawTraceBuilder setInstrumentationName(final String instrumentationName) {
            this.instrumentationName = instrumentationName;
            return this;
        }

        public RawTraceBuilder setInstrumentationVersion(final String instrumentationVersion) {
            this.instrumentationVersion = instrumentationVersion;
            return this;
        }

        public RawTraceBuilder setServiceName(final String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public RawTraceBuilder setResourceAttributes(final Map<String, String> resourceAttributes) {
            this.resourceAttributes = resourceAttributes;
            return this;
        }

        public RawTrace build() {
            return new RawTrace(this);
        }

    }

    private static String convertStringNanosToISO8601(final String stringNanos) {
        final BigDecimal nanos = new BigDecimal(stringNanos);
        final long epochSeconds = nanos.divide(MILLIS_TO_NANOS.multiply(SEC_TO_MILLIS), RoundingMode.DOWN).longValue();
        final int nanoAdj = nanos.remainder(MILLIS_TO_NANOS.multiply(SEC_TO_MILLIS)).intValue();
        return Instant.ofEpochSecond(epochSeconds, nanoAdj).toString();
    }


    public static RawTrace buildFromProto(Resource resource, Span spans, InstrumentationLibrary instrumentationLibrary) {
        final String SERVICE_NAME = "service.name";

        Map<Object, Object> map = resource.getAttributesList().stream().collect(Collectors.toMap(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue()));
        final Map<String, String> resourceAttributesMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof String) {
                resourceAttributesMap.put((String) entry.getKey(), (String) entry.getValue());
            }
        }

        return new RawTraceBuilder()
                .setTraceId(Hex.encodeHexString(spans.getTraceId().toByteArray()))
                .setSpanId(Hex.encodeHexString(spans.getSpanId().toByteArray()))
                .setParentSpanId(Hex.encodeHexString(spans.getParentSpanId().toByteArray()))
                .setName(spans.getName())
                .setStartTime(convertStringNanosToISO8601(String.valueOf(spans.getStartTimeUnixNano())))
                .setEndTime(convertStringNanosToISO8601(String.valueOf(spans.getEndTimeUnixNano())))
                .setInstrumentationName(instrumentationLibrary.getName())
                .setInstrumentationVersion(instrumentationLibrary.getVersion())
                .setServiceName(resource.getAttributesList().stream().filter(keyValue -> keyValue.getKey().equals(SERVICE_NAME)
                ).findFirst().get().getValue().getStringValue())
                .setResourceAttributes(resourceAttributesMap)
                .build();
    }
}
