package com.amazon.situp.plugins.processor.oteltrace;

import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import org.apache.commons.codec.binary.Hex;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class RawTrace {

    private static final BigDecimal MILLIS_TO_NANOS = new BigDecimal(1_000_000);
    private static final BigDecimal SEC_TO_MILLIS = new BigDecimal(1_000);

    private final String traceID;
    private final String spanID;
    private final String parentSpanID;
    private final String name;
    private final String startTime;
    private final String endTime;
    private final String instrumentationName;
    private final String instrumentationVersion;
    private final String serviceName;
    private final Map<String, String> resourceAttributes;


    private RawTrace(RawTraceBuilder builder) {
        this.traceID = builder.traceID;
        this.spanID = builder.spanID;
        this.parentSpanID = builder.parentSpanID;
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
        private String traceID;
        private String spanID;
        private String parentSpanID;
        private String name;
        private String startTime;
        private String endTime;
        private String instrumentationName;
        private String instrumentationVersion;
        private String serviceName;
        private Map<String, String> resourceAttributes;

        public RawTraceBuilder() {
        }

        public RawTraceBuilder setTraceID(String traceID) {
            checkArgument(traceID != null, "traceID cannot be null");
            this.traceID = traceID;
            return this;
        }

        public RawTraceBuilder setSpanID(String spanID) {
            checkArgument(spanID != null, "spanID cannot be null");
            this.spanID = spanID;
            return this;
        }

        public RawTraceBuilder setParentSpanID(String parentSpanID) {
            checkArgument(parentSpanID != null, "parentSpanID cannot be null");
            this.parentSpanID = parentSpanID;
            return this;
        }

        public RawTraceBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public RawTraceBuilder setStartTime(String startTime) {
            this.startTime = startTime;
            return this;
        }

        public RawTraceBuilder setEndTime(String endTime) {
            this.endTime = endTime;
            return this;
        }

        public RawTraceBuilder setInstrumentationName(String instrumentationName) {
            this.instrumentationName = instrumentationName;
            return this;
        }

        public RawTraceBuilder setInstrumentationVersion(String instrumentationVersion) {
            this.instrumentationVersion = instrumentationVersion;
            return this;
        }

        public RawTraceBuilder setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public RawTraceBuilder setResourceAttributes(Map<String, String> resourceAttributes) {
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

        return new RawTraceBuilder()
                .setTraceID(Hex.encodeHexString(spans.getTraceId().toByteArray()))
                .setSpanID(Hex.encodeHexString(spans.getSpanId().toByteArray()))
                .setParentSpanID(Hex.encodeHexString(spans.getParentSpanId().toByteArray()))
                .setName(spans.getName())
                .setStartTime(convertStringNanosToISO8601(String.valueOf(spans.getStartTimeUnixNano())))
                .setEndTime(convertStringNanosToISO8601(String.valueOf(spans.getEndTimeUnixNano())))
                .setInstrumentationName(instrumentationLibrary.getName())
                .setInstrumentationVersion(instrumentationLibrary.getVersion())
                .setServiceName(resource.getAttributesList().stream().filter(keyValue -> keyValue.getKey().equals(SERVICE_NAME)
                ).findFirst().get().getValue().getStringValue())
                // TODO: verify the data type for resourceAttributes to set it here
                .build();
    }
}
