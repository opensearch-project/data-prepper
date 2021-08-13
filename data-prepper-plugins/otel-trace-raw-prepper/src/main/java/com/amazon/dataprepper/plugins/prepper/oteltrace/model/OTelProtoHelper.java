/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class OTelProtoHelper {

    private final static ObjectMapper OBJECT_MAPPER =  new ObjectMapper();
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
    public final static Function<String, String> REPLACE_DOT_WITH_AT = i -> i.replace(DOT, AT);
    /**
     * Span and Resource attributes are essential for kibana so they should not be nested. SO we will prefix them with "span.attributes"
     * and "resource.attributes".
     *
     */
    public final static Function<String, String> SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> SPAN_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public final static Function<String, String> RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> RESOURCE_ATTRIBUTES + DOT + i.replace(DOT, AT);


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
             * nested objects. Storing them in flatten structure is not elasticsearch friendly. So they are stored
             * as Json string.
             */
            case ARRAY_VALUE:
                try {
                    return OBJECT_MAPPER.writeValueAsString(value.getArrayValue().getValuesList().stream()
                            .map(OTelProtoHelper::convertAnyValue).collect(Collectors.toList()));
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

    public static Map<String, Object> getSpanAttributes(final Span span) {
        return span.getAttributesList().stream().collect(Collectors.toMap(i -> SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    public static Map<String, Object> getResourceAttributes(final Resource resource) {
        return resource.getAttributesList().stream().collect(Collectors.toMap(i -> RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    public static Map<String, Object> getLinkAttributes(final Span.Link link) {
        return link.getAttributesList().stream().collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    public static Map<String, Object> getEventAttributes(final Span.Event event) {
        return event.getAttributesList().stream().collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Trace group represent root span i.e the span which triggers the trace event with the microservice architecture. This field is not part of the OpenTelemetry Spec.
     * This field is something specific to Trace Analytics feature that Kibana will be supporting. This field is derived from the opentelemetry spec and set as below,
     * <p>
     * if (this.parentSpanId.isEmpty()) {
     * traceGroup = this.name;
     * }
     * <p>
     * Note: The reason this method is part of the helper class is because the trace group definition will be expanded in the future when we support Links in Kibana Trace Analytics.
     */
    public static TraceGroup getTraceGroup(final Span span) {
        final TraceGroup.TraceGroupBuilder traceGroupBuilder = new TraceGroup.TraceGroupBuilder();
        if (span.getParentSpanId().isEmpty()) {
            traceGroupBuilder.setFromSpan(span);
        }
        return traceGroupBuilder.build();
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

    public static String getStartTimeISO8601(final Span span) {
        return convertUnixNanosToISO8601(span.getStartTimeUnixNano());
    }

    public static String getEndTimeISO8601(final Span span) {
        return convertUnixNanosToISO8601(span.getEndTimeUnixNano());
    }

    public static String getTimeISO8601(final Span.Event event) {
        return convertUnixNanosToISO8601(event.getTimeUnixNano());
    }

    public static Optional<String> getServiceName(final Resource resource) {
        return resource.getAttributesList().stream().filter(
                keyValue -> keyValue.getKey().equals(SERVICE_NAME)
                        && !keyValue.getValue().getStringValue().isEmpty()
        ).findFirst().map(i -> i.getValue().getStringValue());
    }
}
