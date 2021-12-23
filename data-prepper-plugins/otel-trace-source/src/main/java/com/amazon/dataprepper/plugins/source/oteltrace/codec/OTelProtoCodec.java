package com.amazon.dataprepper.plugins.source.oteltrace.codec;

import com.amazon.dataprepper.model.trace.DefaultLink;
import com.amazon.dataprepper.model.trace.DefaultSpanEvent;
import com.amazon.dataprepper.model.trace.DefaultTraceGroupFields;
import com.amazon.dataprepper.model.trace.JacksonSpan;
import com.amazon.dataprepper.model.trace.Link;
import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.model.trace.SpanEvent;
import com.amazon.dataprepper.model.trace.TraceGroupFields;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.commons.codec.binary.Hex;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * OTelProtoCodec parses the {@link ExportTraceServiceRequest} into Collection<{@link Span}>.
 * <p>
 */
public class OTelProtoCodec {
    private static final ObjectMapper OBJECT_MAPPER =  new ObjectMapper();
    private static final String SERVICE_NAME = "service.name";
    private static final String SPAN_ATTRIBUTES = "span.attributes";
    static final String RESOURCE_ATTRIBUTES = "resource.attributes";
    static final String INSTRUMENTATION_LIBRARY_NAME = "instrumentationLibrary.name";
    static final String INSTRUMENTATION_LIBRARY_VERSION = "instrumentationLibrary.version";
    static final String STATUS_CODE = "status.code";
    static final String STATUS_MESSAGE = "status.message";
    /**
     * To make it ES friendly we will replace '.' in keys with '@' in all the Keys in {@link io.opentelemetry.proto.common.v1.KeyValue}
     */
    private static final String DOT = ".";
    private static final String AT = "@";
    public static final Function<String, String> REPLACE_DOT_WITH_AT = i -> i.replace(DOT, AT);
    /**
     * Span and Resource attributes are essential for OpenSearch so they should not be nested. SO we will prefix them with "span.attributes"
     * and "resource.attributes".
     *
     */
    public static final Function<String, String> SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> SPAN_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> RESOURCE_ATTRIBUTES + DOT + i.replace(DOT, AT);

    public static List<Span> parseExportTraceServiceRequest(final ExportTraceServiceRequest exportTraceServiceRequest) {
        return exportTraceServiceRequest.getResourceSpansList().stream()
                .flatMap(rs -> parseResourceSpans(rs).stream()).collect(Collectors.toList());
    }

    public static List<Span> parseResourceSpans(final ResourceSpans resourceSpans) {
        final List<Span> spans = new LinkedList<>();

        final String serviceName = getServiceName(resourceSpans.getResource()).orElse(null);
        final Map<String, Object> resourceAttributes = getResourceAttributes(resourceSpans.getResource());
        for (InstrumentationLibrarySpans is : resourceSpans.getInstrumentationLibrarySpansList()) {
            for (io.opentelemetry.proto.trace.v1.Span sp : is.getSpansList()) {
                final Span span = parseSpan(sp, is.getInstrumentationLibrary(), serviceName, resourceAttributes);
                spans.add(span);
            }
        }
        return spans;
    }

    public static Span parseSpan(final io.opentelemetry.proto.trace.v1.Span sp, final InstrumentationLibrary instrumentationLibrary,
                                 final String serviceName, final Map<String, Object> resourceAttributes) {
        return JacksonSpan.builder()
                .withSpanId(Hex.encodeHexString(sp.getSpanId().toByteArray()))
                .withTraceId(Hex.encodeHexString(sp.getTraceId().toByteArray()))
                .withTraceState(sp.getTraceState())
                .withParentSpanId(Hex.encodeHexString(sp.getParentSpanId().toByteArray()))
                .withName(sp.getName())
                .withServiceName(serviceName)
                .withKind(sp.getKind().name())
                .withStartTime(getStartTimeISO8601(sp))
                .withEndTime(getEndTimeISO8601(sp))
                .withAttributes(mergeAllAttributes(
                        Arrays.asList(
                                getSpanAttributes(sp),
                                resourceAttributes,
                                getInstrumentationLibraryAttributes(instrumentationLibrary),
                                getSpanStatusAttributes(sp.getStatus())
                        )
                ))
                .withDroppedAttributesCount(sp.getDroppedAttributesCount())
                .withEvents(sp.getEventsList().stream().map(OTelProtoCodec::getSpanEvent).collect(Collectors.toList()))
                .withDroppedEventsCount(sp.getDroppedEventsCount())
                .withLinks(sp.getLinksList().stream().map(OTelProtoCodec::getLink).collect(Collectors.toList()))
                .withDroppedLinksCount(sp.getDroppedLinksCount())
                .withTraceGroup(getTraceGroup(sp))
                .withDurationInNanos(sp.getEndTimeUnixNano() - sp.getStartTimeUnixNano())
                .withTraceGroupFields(getTraceGroupFields(sp))
                .build();
    }

    public static Object convertAnyValue(final AnyValue value) {
        switch (value.getValueCase()) {
            case VALUE_NOT_SET:
            case STRING_VALUE:
                return value.getStringValue();
            case BOOL_VALUE:
                return value.getBoolValue();
            case INT_VALUE:
                return value.getIntValue();
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            /**
             * Both {@link AnyValue.ARRAY_VALUE_FIELD_NUMBER} and {@link AnyValue.KVLIST_VALUE_FIELD_NUMBER} are
             * nested objects. Storing them in flatten structure is not OpenSearch friendly. So they are stored
             * as Json string.
             */
            case ARRAY_VALUE:
                try {
                    return OBJECT_MAPPER.writeValueAsString(value.getArrayValue().getValuesList().stream()
                            .map(OTelProtoCodec::convertAnyValue).collect(Collectors.toList()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            case KVLIST_VALUE:
                try {
                    return OBJECT_MAPPER.writeValueAsString(value.getKvlistValue().getValuesList().stream()
                            .collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()),
                                    i ->convertAnyValue(i.getValue()))));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new RuntimeException("Unknown case");
        }
    }

    public static Map<String, Object> mergeAllAttributes(final Collection<Map<String, Object>> attributes) {
        return attributes.stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static SpanEvent getSpanEvent(final io.opentelemetry.proto.trace.v1.Span.Event event) {
        return DefaultSpanEvent.builder()
                .withTime(getTimeISO8601(event))
                .withName(event.getName())
                .withAttributes(getEventAttributes(event))
                .withDroppedAttributesCount(event.getDroppedAttributesCount())
                .build();
    }

    public static Link getLink(final io.opentelemetry.proto.trace.v1.Span.Link link) {
        return DefaultLink.builder()
                .withSpanId(Hex.encodeHexString(link.getSpanId().toByteArray()))
                .withTraceId(Hex.encodeHexString(link.getTraceId().toByteArray()))
                .withTraceState(link.getTraceState())
                .withDroppedAttributesCount(link.getDroppedAttributesCount())
                .withAttributes(getLinkAttributes(link))
                .build();
    }

    public static Map<String, Object> getSpanAttributes(final io.opentelemetry.proto.trace.v1.Span span) {
        return span.getAttributesList().stream().collect(Collectors.toMap(i -> SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    public static Map<String, Object> getResourceAttributes(final Resource resource) {
        return resource.getAttributesList().stream().collect(Collectors.toMap(i -> RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    public static Map<String, Object> getLinkAttributes(final io.opentelemetry.proto.trace.v1.Span.Link link) {
        return link.getAttributesList().stream().collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    public static Map<String, Object> getEventAttributes(final io.opentelemetry.proto.trace.v1.Span.Event event) {
        return event.getAttributesList().stream().collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Trace group represent root span i.e the span which triggers the trace event with the microservice architecture. This field is not part of the OpenTelemetry Spec.
     * This field is something specific to Trace Analytics feature that OpenSearch Dashboards will be supporting. This field is derived from the opentelemetry spec and set as below,
     * <p>
     * span.getParentSpanId().isEmpty()? span.getName() : null;
     * <p>
     * Note: The reason this method is part of the codec class is because the trace group definition will be expanded in the future when we support Links in OpenSearch Dashboards Trace Analytics.
     */
    public static String getTraceGroup(final io.opentelemetry.proto.trace.v1.Span span) {
        return span.getParentSpanId().isEmpty()? span.getName() : null;
    }

    /**
     * Trace group fields represent root span i.e the span which triggers the trace event with the microservice architecture. These fields are not part of the OpenTelemetry Spec.
     * These fields are something specific to Trace Analytics feature that OpenSearch Dashboards will be supporting. They are derived from the opentelemetry spec and set as below,
     * <p>
     * if (span.getParentSpanId().isEmpty()) {
     *     traceGroupFieldsBuilder
     *             .withDurationInNanos(span.getEndTimeUnixNano() - span.getStartTimeUnixNano())
     *             .withEndTime(getEndTimeISO8601(span))
     *             .withStatusCode(span.getStatus().getCodeValue());
     * }
     * <p>
     * Note: The reason this method is part of the codec class is because the trace group definition will be expanded in the future when we support Links in OpenSearch Dashboards Trace Analytics.
     */
    public static TraceGroupFields getTraceGroupFields(final io.opentelemetry.proto.trace.v1.Span span) {
        DefaultTraceGroupFields.Builder traceGroupFieldsBuilder = DefaultTraceGroupFields.builder();
        if (span.getParentSpanId().isEmpty()) {
            traceGroupFieldsBuilder = traceGroupFieldsBuilder
                    .withDurationInNanos(span.getEndTimeUnixNano() - span.getStartTimeUnixNano())
                    .withEndTime(getEndTimeISO8601(span))
                    .withStatusCode(span.getStatus().getCodeValue());
        }
        return traceGroupFieldsBuilder.build();
    }

    public static Map<String, Object> getInstrumentationLibraryAttributes(final InstrumentationLibrary instrumentationLibrary) {
        final Map<String, Object> instrumentationAttr = new HashMap<>();
        if (!instrumentationLibrary.getName().isEmpty()) {
            instrumentationAttr.put(INSTRUMENTATION_LIBRARY_NAME, instrumentationLibrary.getName());
        }
        if (!instrumentationLibrary.getVersion().isEmpty()) {
            instrumentationAttr.put(INSTRUMENTATION_LIBRARY_VERSION, instrumentationLibrary.getVersion());
        }
        return instrumentationAttr;
    }

    public static Map<String, Object> getSpanStatusAttributes(final Status status) {
        final Map<String, Object> statusAttr = new HashMap<>();
        statusAttr.put(STATUS_CODE, status.getCodeValue());
        if(!status.getMessage().isEmpty()){
            statusAttr.put(STATUS_MESSAGE, status.getMessage());
        }
        return statusAttr;
    }

    private static String convertUnixNanosToISO8601(final long unixNano) {
        return Instant.ofEpochSecond(0L, unixNano).toString();
    }

    public static String getStartTimeISO8601(final io.opentelemetry.proto.trace.v1.Span span) {
        return convertUnixNanosToISO8601(span.getStartTimeUnixNano());
    }

    public static String getEndTimeISO8601(final io.opentelemetry.proto.trace.v1.Span span) {
        return convertUnixNanosToISO8601(span.getEndTimeUnixNano());
    }

    public static String getTimeISO8601(final io.opentelemetry.proto.trace.v1.Span.Event event) {
        return convertUnixNanosToISO8601(event.getTimeUnixNano());
    }

    public static Optional<String> getServiceName(final Resource resource) {
        return resource.getAttributesList().stream().filter(
                keyValue -> keyValue.getKey().equals(SERVICE_NAME)
                        && !keyValue.getValue().getStringValue().isEmpty()
        ).findFirst().map(i -> i.getValue().getStringValue());
    }
}
