/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.otel.codec;

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
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.ArrayList;
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
 * OTelProtoCodec is for encoding/decoding between <{@link com.amazon.dataprepper.model.trace}> and <{@link io.opentelemetry.proto}>.
 * <p>
 */
public class OTelProtoCodec {
    private static final ObjectMapper OBJECT_MAPPER =  new ObjectMapper();
    private static final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
    protected static final String SERVICE_NAME = "service.name";
    protected static final String SPAN_ATTRIBUTES = "span.attributes";
    static final String RESOURCE_ATTRIBUTES = "resource.attributes";
    static final String INSTRUMENTATION_LIBRARY_NAME = "instrumentationLibrary.name";
    static final String INSTRUMENTATION_LIBRARY_VERSION = "instrumentationLibrary.version";
    static final String STATUS_CODE = "status.code";
    static final String STATUS_MESSAGE = "status.message";
    /**
     * To make it OpenSearch friendly we will replace '.' in keys with '@' in all the Keys in {@link io.opentelemetry.proto.common.v1.KeyValue}
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

    public static String convertUnixNanosToISO8601(final long unixNano) {
        return Instant.ofEpochSecond(0L, unixNano).toString();
    }

    public static long timeISO8601ToNanos(final String timeISO08601) {
        final Instant instant = Instant.parse(timeISO08601);
        return instant.getEpochSecond() * NANO_MULTIPLIER + instant.getNano();
    }

    public static class OTelProtoDecoder {
        public List<Span> parseExportTraceServiceRequest(final ExportTraceServiceRequest exportTraceServiceRequest) {
            return exportTraceServiceRequest.getResourceSpansList().stream()
                    .flatMap(rs -> parseResourceSpans(rs).stream()).collect(Collectors.toList());
        }

        protected List<Span> parseResourceSpans(final ResourceSpans resourceSpans) {
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

        protected Span parseSpan(final io.opentelemetry.proto.trace.v1.Span sp, final InstrumentationLibrary instrumentationLibrary,
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
                    .withEvents(sp.getEventsList().stream().map(this::getSpanEvent).collect(Collectors.toList()))
                    .withDroppedEventsCount(sp.getDroppedEventsCount())
                    .withLinks(sp.getLinksList().stream().map(this::getLink).collect(Collectors.toList()))
                    .withDroppedLinksCount(sp.getDroppedLinksCount())
                    .withTraceGroup(getTraceGroup(sp))
                    .withDurationInNanos(sp.getEndTimeUnixNano() - sp.getStartTimeUnixNano())
                    .withTraceGroupFields(getTraceGroupFields(sp))
                    .build();
        }

        protected Object convertAnyValue(final AnyValue value) {
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
                                .map(this::convertAnyValue).collect(Collectors.toList()));
                    } catch (JsonProcessingException e) {
                        throw new OTelEncodingException(e);
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

        protected Map<String, Object> mergeAllAttributes(final Collection<Map<String, Object>> attributes) {
            return attributes.stream()
                    .flatMap(map -> map.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        protected SpanEvent getSpanEvent(final io.opentelemetry.proto.trace.v1.Span.Event event) {
            return DefaultSpanEvent.builder()
                    .withTime(getTimeISO8601(event))
                    .withName(event.getName())
                    .withAttributes(getEventAttributes(event))
                    .withDroppedAttributesCount(event.getDroppedAttributesCount())
                    .build();
        }

        protected Link getLink(final io.opentelemetry.proto.trace.v1.Span.Link link) {
            return DefaultLink.builder()
                    .withSpanId(Hex.encodeHexString(link.getSpanId().toByteArray()))
                    .withTraceId(Hex.encodeHexString(link.getTraceId().toByteArray()))
                    .withTraceState(link.getTraceState())
                    .withDroppedAttributesCount(link.getDroppedAttributesCount())
                    .withAttributes(getLinkAttributes(link))
                    .build();
        }

        protected Map<String, Object> getSpanAttributes(final io.opentelemetry.proto.trace.v1.Span span) {
            return span.getAttributesList().stream().collect(Collectors.toMap(i -> SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
        }

        protected Map<String, Object> getResourceAttributes(final Resource resource) {
            return resource.getAttributesList().stream().collect(Collectors.toMap(i -> RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
        }

        protected Map<String, Object> getLinkAttributes(final io.opentelemetry.proto.trace.v1.Span.Link link) {
            return link.getAttributesList().stream().collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
        }

        protected Map<String, Object> getEventAttributes(final io.opentelemetry.proto.trace.v1.Span.Event event) {
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
        protected String getTraceGroup(final io.opentelemetry.proto.trace.v1.Span span) {
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
        protected TraceGroupFields getTraceGroupFields(final io.opentelemetry.proto.trace.v1.Span span) {
            DefaultTraceGroupFields.Builder traceGroupFieldsBuilder = DefaultTraceGroupFields.builder();
            if (span.getParentSpanId().isEmpty()) {
                traceGroupFieldsBuilder = traceGroupFieldsBuilder
                        .withDurationInNanos(span.getEndTimeUnixNano() - span.getStartTimeUnixNano())
                        .withEndTime(getEndTimeISO8601(span))
                        .withStatusCode(span.getStatus().getCodeValue());
            }
            return traceGroupFieldsBuilder.build();
        }

        protected Map<String, Object> getInstrumentationLibraryAttributes(final InstrumentationLibrary instrumentationLibrary) {
            final Map<String, Object> instrumentationAttr = new HashMap<>();
            if (!instrumentationLibrary.getName().isEmpty()) {
                instrumentationAttr.put(INSTRUMENTATION_LIBRARY_NAME, instrumentationLibrary.getName());
            }
            if (!instrumentationLibrary.getVersion().isEmpty()) {
                instrumentationAttr.put(INSTRUMENTATION_LIBRARY_VERSION, instrumentationLibrary.getVersion());
            }
            return instrumentationAttr;
        }

        protected Map<String, Object> getSpanStatusAttributes(final Status status) {
            final Map<String, Object> statusAttr = new HashMap<>();
            statusAttr.put(STATUS_CODE, status.getCodeValue());
            if(!status.getMessage().isEmpty()){
                statusAttr.put(STATUS_MESSAGE, status.getMessage());
            }
            return statusAttr;
        }

        protected String getStartTimeISO8601(final io.opentelemetry.proto.trace.v1.Span span) {
            return convertUnixNanosToISO8601(span.getStartTimeUnixNano());
        }

        protected String getEndTimeISO8601(final io.opentelemetry.proto.trace.v1.Span span) {
            return convertUnixNanosToISO8601(span.getEndTimeUnixNano());
        }

        protected String getTimeISO8601(final io.opentelemetry.proto.trace.v1.Span.Event event) {
            return convertUnixNanosToISO8601(event.getTimeUnixNano());
        }

        protected Optional<String> getServiceName(final Resource resource) {
            return resource.getAttributesList().stream().filter(
                    keyValue -> keyValue.getKey().equals(SERVICE_NAME)
                            && !keyValue.getValue().getStringValue().isEmpty()
            ).findFirst().map(i -> i.getValue().getStringValue());
        }
    }

    public static class OTelProtoEncoder {
        protected static final String SERVICE_NAME_ATTRIBUTE = "service@name";
        protected static final String RESOURCE_ATTRIBUTES_PREFIX = RESOURCE_ATTRIBUTES + DOT;
        protected static final String SPAN_ATTRIBUTES_PREFIX = SPAN_ATTRIBUTES + DOT;

        public ResourceSpans convertToResourceSpans(final Span span) throws UnsupportedEncodingException, DecoderException {
            final ResourceSpans.Builder rsBuilder = ResourceSpans.newBuilder();
            final Resource resource = constructResource(span.getServiceName(), span.getAttributes());
            rsBuilder.setResource(resource);
            final InstrumentationLibrarySpans.Builder instrumentationLibrarySpansBuilder = InstrumentationLibrarySpans.newBuilder();
            final InstrumentationLibrary instrumentationLibrary = constructInstrumentationLibrary(span.getAttributes());
            instrumentationLibrarySpansBuilder.setInstrumentationLibrary(instrumentationLibrary);
            final io.opentelemetry.proto.trace.v1.Span otelProtoSpan = constructSpan(span);
            instrumentationLibrarySpansBuilder.addSpans(otelProtoSpan);
            rsBuilder.addInstrumentationLibrarySpans(instrumentationLibrarySpansBuilder);
            return rsBuilder.build();
        }

        protected List<KeyValue> getSpanAttributes(final Map<String, Object> attributes) throws UnsupportedEncodingException {
            final List<String> spanAttributeKeys = attributes.keySet().stream()
                    .filter(key -> key.startsWith(SPAN_ATTRIBUTES_PREFIX)).collect(Collectors.toList());
            final List<KeyValue> result = new ArrayList<>();
            for (final String key: spanAttributeKeys) {
                final String trimmedKey = key.substring(SPAN_ATTRIBUTES_PREFIX.length());
                final KeyValue keyValue = KeyValue.newBuilder()
                        .setKey(trimmedKey)
                        .setValue(objectToAnyValue(attributes.get(key)))
                        .build();
                result.add(keyValue);
            }

            return result;
        }

        protected Resource constructResource(final String serviceName, final Map<String, Object> attributes) throws UnsupportedEncodingException {
            final Resource.Builder rsBuilder = Resource.newBuilder();
            final List<KeyValue> resourceAttributes = getResourceAttributes(attributes);
            rsBuilder.addAllAttributes(resourceAttributes);
            if (serviceName != null) {
                final KeyValue.Builder serviceNameKeyValueBuilder = KeyValue.newBuilder()
                        .setKey(SERVICE_NAME)
                        .setValue(objectToAnyValue(serviceName));
                rsBuilder.addAttributes(serviceNameKeyValueBuilder);
            }
            return rsBuilder.build();
        }

        protected List<KeyValue> getResourceAttributes(final Map<String, Object> attributes) throws UnsupportedEncodingException {
            final List<String> resourceAttributeKeys = attributes.keySet().stream()
                    .filter(key -> key.startsWith(RESOURCE_ATTRIBUTES_PREFIX)).collect(Collectors.toList());
            final List<KeyValue> result = new ArrayList<>();
            for (final String key: resourceAttributeKeys) {
                final String trimmedKey = key.substring(RESOURCE_ATTRIBUTES_PREFIX.length());
                if (!trimmedKey.equals(SERVICE_NAME_ATTRIBUTE)) {
                    final KeyValue keyValue = KeyValue.newBuilder()
                            .setKey(trimmedKey)
                            .setValue(objectToAnyValue(attributes.get(key)))
                            .build();
                    result.add(keyValue);
                }
            }

            return result;
        }

        protected InstrumentationLibrary constructInstrumentationLibrary(final Map<String, Object> attributes) {
            final InstrumentationLibrary.Builder builder = InstrumentationLibrary.newBuilder();
            final Optional<String> instrumentationLibraryName = Optional.ofNullable((String) attributes.get(INSTRUMENTATION_LIBRARY_NAME));
            final Optional<String> instrumentationLibraryVersion = Optional.ofNullable((String) attributes.get(INSTRUMENTATION_LIBRARY_VERSION));
            instrumentationLibraryName.ifPresent(builder::setName);
            instrumentationLibraryVersion.ifPresent(builder::setVersion);
            return builder.build();
        }

        protected Status constructSpanStatus(final Map<String, Object> attributes) {
            final io.opentelemetry.proto.trace.v1.Status.Builder builder = Status.newBuilder();
            final Optional<Integer> statusCode = Optional.ofNullable((Integer) attributes.get(STATUS_CODE));
            final Optional<String> statusMessage = Optional.ofNullable((String) attributes.get(STATUS_MESSAGE));
            statusCode.ifPresent(builder::setCodeValue);
            statusMessage.ifPresent(builder::setMessage);
            return builder.build();
        }

        protected List<io.opentelemetry.proto.trace.v1.Span.Event> convertSpanEvents(final List<? extends SpanEvent> spanEvents) throws UnsupportedEncodingException {
            final List<io.opentelemetry.proto.trace.v1.Span.Event> result = new ArrayList<>();
            for (final SpanEvent spanEvent: spanEvents) {
                result.add(convertSpanEvent(spanEvent));
            }
            return result;
        }

        protected io.opentelemetry.proto.trace.v1.Span.Event convertSpanEvent(final SpanEvent spanEvent) throws UnsupportedEncodingException {
            final io.opentelemetry.proto.trace.v1.Span.Event.Builder builder = io.opentelemetry.proto.trace.v1.Span.Event.newBuilder();
            builder.setName(spanEvent.getName());
            builder.setTimeUnixNano(timeISO8601ToNanos(spanEvent.getTime()));
            builder.setDroppedAttributesCount(spanEvent.getDroppedAttributesCount());
            final List<KeyValue> attributeKeyValueList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : spanEvent.getAttributes().entrySet()) {
                KeyValue.Builder setValue = KeyValue.newBuilder().setKey(entry.getKey()).setValue(objectToAnyValue(entry.getValue()));
                attributeKeyValueList.add(setValue.build());
            }
            builder.addAllAttributes(attributeKeyValueList);
            return builder.build();
        }

        protected List<io.opentelemetry.proto.trace.v1.Span.Link> convertSpanLinks(final List<? extends Link> links) throws DecoderException, UnsupportedEncodingException {
            final List<io.opentelemetry.proto.trace.v1.Span.Link> result = new ArrayList<>();
            for (final Link link: links) {
                result.add(convertSpanLink(link));
            }
            return result;
        }

        protected io.opentelemetry.proto.trace.v1.Span.Link convertSpanLink(final Link link) throws DecoderException, UnsupportedEncodingException {
            final io.opentelemetry.proto.trace.v1.Span.Link.Builder builder = io.opentelemetry.proto.trace.v1.Span.Link.newBuilder();
            builder.setSpanId(ByteString.copyFrom(Hex.decodeHex(link.getSpanId())));
            builder.setTraceId(ByteString.copyFrom(Hex.decodeHex(link.getTraceId())));
            builder.setTraceState(link.getTraceState());
            builder.setDroppedAttributesCount(link.getDroppedAttributesCount());
            final List<KeyValue> attributeKeyValueList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : link.getAttributes().entrySet()) {
                KeyValue.Builder setValue = KeyValue.newBuilder().setKey(entry.getKey()).setValue(objectToAnyValue(entry.getValue()));
                attributeKeyValueList.add(setValue.build());
            }
            builder.addAllAttributes(attributeKeyValueList);
            return builder.build();
        }

        protected io.opentelemetry.proto.trace.v1.Span constructSpan(final Span span) throws DecoderException, UnsupportedEncodingException {
            io.opentelemetry.proto.trace.v1.Span.Builder builder = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                    .setSpanId(ByteString.copyFrom(Hex.decodeHex(span.getSpanId())))
                    .setParentSpanId(ByteString.copyFrom(Hex.decodeHex(span.getParentSpanId())))
                    .setTraceId(ByteString.copyFrom(Hex.decodeHex(span.getTraceId())))
                    .setTraceState(span.getTraceState())
                    .setName(span.getName())
                    .setKind(io.opentelemetry.proto.trace.v1.Span.SpanKind.valueOf(span.getKind()))
                    .setStartTimeUnixNano(timeISO8601ToNanos(span.getStartTime()))
                    .setEndTimeUnixNano(timeISO8601ToNanos(span.getEndTime()))
                    .setDroppedAttributesCount(span.getDroppedAttributesCount())
                    .setDroppedEventsCount(span.getDroppedEventsCount())
                    .setDroppedLinksCount(span.getDroppedLinksCount());
            final Map<String, Object> allAttributes = span.getAttributes();
            builder
                    .setStatus(constructSpanStatus(allAttributes))
                    .addAllAttributes(getSpanAttributes(allAttributes))
                    .addAllEvents(convertSpanEvents(span.getEvents()))
                    .addAllLinks(convertSpanLinks(span.getLinks()));

            return builder.build();
        }

        protected AnyValue objectToAnyValue(final Object obj) throws UnsupportedEncodingException {
            final AnyValue.Builder anyValueBuilder = AnyValue.newBuilder();
            if (obj == null) {
                return anyValueBuilder.build();
            } else if (obj instanceof Integer || obj instanceof Long) {
                anyValueBuilder.setIntValue(((Number) obj).longValue());
            } else if (obj instanceof String) {
                anyValueBuilder.setStringValue((String) obj);
            } else if (obj instanceof Boolean) {
                anyValueBuilder.setBoolValue((Boolean) obj);
            } else if (obj instanceof Double) {
                anyValueBuilder.setDoubleValue((Double) obj);
            } else {
                throw new UnsupportedEncodingException(
                        String.format("Unsupported object type: %s in io.opentelemetry.proto.common.v1.AnyValue encoding",
                                obj.getClass().toString()));
            }

            return anyValueBuilder.build();
        }
    }
}
