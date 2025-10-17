package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class OtelTraceJsonDecoder {

    private static final String STATUS_CODE = "status.code";
    private static final String STATUS_MESSAGE = "status.message";
    static final String INSTRUMENTATION_SCOPE_NAME = "instrumentationScope.name";
    static final String INSTRUMENTATION_SCOPE_VERSION = "instrumentationScope.version";
    static final String INSTRUMENTATION_SCOPE_ATTRIBUTES = "instrumentationScope.attributes";
    ObjectMapper objectMapper = new ObjectMapper();

    public void parse(InputStream inputStream, Instant receivedTimeStamp, Consumer<Record<Event>> eventConsumer) throws IOException {

        String inputJson = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));

        List<Span> spans = parseSpans(inputJson, receivedTimeStamp);

        for (Span span : spans) {
            eventConsumer.accept(new Record<>(span));
        }
    }

    private List<Span> parseSpans(String inputJson, Instant receivedTimeStamp) throws IOException {
        OtelTraceJsonPojo otelTracePojo = objectMapper.readValue(inputJson, OtelTraceJsonPojo.class);

        String serviceName = readServiceName(otelTracePojo);
        Map<String, Object> resourceAttributes = getResourcesAttributes(otelTracePojo);
        List<Span> spans = new ArrayList<>();
        for (OtelTraceJsonPojo.ResourceSpan resourceSpan : otelTracePojo.resourceSpans) {
            for (OtelTraceJsonPojo.ScopeSpan scopeSpan : resourceSpan.scopeSpans) {
                Map<String, Object> instrumentationScopeAttributes = getInstrumentationScopeAttributes(scopeSpan);
                for (OtelTraceJsonPojo.Span jsonSpan : scopeSpan.spans) {
                    spans.add(parseSpan(jsonSpan, serviceName, resourceAttributes, instrumentationScopeAttributes, receivedTimeStamp));
                }
            }
        }
        return spans;
    }

    public static Map<String, Object> getInstrumentationScopeAttributes(final OtelTraceJsonPojo.ScopeSpan scopeSpan) {
        final Map<String, Object> instrumentationScopeAttr = new HashMap<>();
        if (scopeSpan.scope.name != null && !scopeSpan.scope.name.isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_NAME, scopeSpan.scope.name);
        }
        if (scopeSpan.scope.version != null && !scopeSpan.scope.version.isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_VERSION, scopeSpan.scope.version);
        }
        if (scopeSpan.scope.attributes != null && !scopeSpan.scope.attributes.isEmpty()) {
            for (OtelTraceJsonPojo.Attribute attribute : scopeSpan.scope.attributes) {
                instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_ATTRIBUTES + attribute.key, attribute.value.getValue());
            }
        }
        return instrumentationScopeAttr;
    }

    private Span parseSpan(OtelTraceJsonPojo.Span jsonSpan, String serviceName, Map<String, Object> resourceAttributes,
                           Map<String, Object> instrumentationScopeAttributes, Instant receivedTime) {

        try {
            return JacksonSpan.builder()
                    .withSpanId(jsonSpan.spanId)
                    .withTraceId(jsonSpan.traceId)
                    .withTraceState(null)
                    .withParentSpanId(jsonSpan.parentSpanId)
                    .withName(jsonSpan.name)
                    .withServiceName(serviceName)
                    .withKind(jsonSpan.kind)
                    .withStartTime(convertUnixNanosToISO8601(jsonSpan.startTimeUnixNano))
                    .withEndTime(convertUnixNanosToISO8601(jsonSpan.endTimeUnixNano))
                    .withAttributes(mergeAllAttributes(
                            Arrays.asList(
                                    getSpanAttributes(jsonSpan),
                                    resourceAttributes,
                                    instrumentationScopeAttributes,
                                    getSpanStatusAttributes(jsonSpan.status)
                            )
                    ))
//                .withDroppedAttributesCount(sp.getDroppedAttributesCount())
                    .withEvents(getSpanEvents(jsonSpan))
//                .withDroppedEventsCount(0)
//                .withLinks(new ArrayList<>())
//                .withDroppedLinksCount(0)
                    .withTraceGroup(getTraceGroup(jsonSpan))
                    .withDurationInNanos(jsonSpan.endTimeUnixNano - jsonSpan.startTimeUnixNano)
                    .withTraceGroupFields(getTraceGroupFields(jsonSpan))
                    .withTimeReceived(receivedTime)
                    .build();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String readServiceName(OtelTraceJsonPojo otelTracePojo) {
        for (OtelTraceJsonPojo.ResourceSpan resourceSpan : otelTracePojo.resourceSpans) {
            for (OtelTraceJsonPojo.Attribute attribute : resourceSpan.resource.attributes) {
                if ("service.name".equals(attribute.key)) {
                    return attribute.value.stringValue;
                }
            }
        }
        return "";
    }

    private Map<String, Object> getResourcesAttributes(OtelTraceJsonPojo otelTracePojo) {
        Map<String, Object> resourceAttributes = new HashMap<>();
        for (OtelTraceJsonPojo.ResourceSpan resourceSpan : otelTracePojo.resourceSpans) {
            for (OtelTraceJsonPojo.Attribute attribute : resourceSpan.resource.attributes) {
                resourceAttributes.put(attribute.key, attribute.value.getValue());
            }
        }
        return resourceAttributes;
    }

    private Map<String, Object> getSpanAttributes(OtelTraceJsonPojo.Span jsonSpan) {
        Map<String, Object> spanAttributes = new HashMap<>();
        if (jsonSpan.attributes == null) {
            return spanAttributes;
        }
        for (OtelTraceJsonPojo.Attribute attribute : jsonSpan.attributes) {
            spanAttributes.put(attribute.key, attribute.value.getValue());
        }
        return spanAttributes;
    }

    private Map<String, Object> getSpanStatusAttributes(final OtelTraceJsonPojo.Status status) {
        final Map<String, Object> statusAttr = new HashMap<>();
        statusAttr.put(STATUS_CODE, status.code);
        if (status.message != null && !status.message.isEmpty()) {
            statusAttr.put(STATUS_MESSAGE, status.message);
        }
        return statusAttr;
    }

    protected Map<String, Object> mergeAllAttributes(final Collection<Map<String, Object>> attributes) {
        return attributes.stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<SpanEvent> getSpanEvents(OtelTraceJsonPojo.Span jsonSpan) {

        if (jsonSpan.events == null || jsonSpan.events.isEmpty()) {
            return new ArrayList<>();
        }

        List<SpanEvent> spanEvents = new ArrayList<>();
        for (OtelTraceJsonPojo.Event event : jsonSpan.events) {

            Map<String, Object> attributes = new HashMap<>();
            for (OtelTraceJsonPojo.Attribute attribute : event.attributes) {
                attributes.put(attribute.key, attribute.value.getValue());
            }

            spanEvents.add(DefaultSpanEvent.builder()
                    .withName(event.name)
                    .withTime(convertUnixNanosToISO8601(jsonSpan.startTimeUnixNano))
                    .withAttributes(attributes)
                    .build());
        }

        return spanEvents;
    }

    private String getTraceGroup(OtelTraceJsonPojo.Span jsonSpan) {
        return jsonSpan.parentSpanId.isEmpty() ? jsonSpan.name : null;
    }

    private TraceGroupFields getTraceGroupFields(OtelTraceJsonPojo.Span jsonSpan) {
        DefaultTraceGroupFields.Builder traceGroupFieldsBuilder = DefaultTraceGroupFields.builder();
        if (jsonSpan.parentSpanId.isEmpty()) {
            traceGroupFieldsBuilder = traceGroupFieldsBuilder
                    .withDurationInNanos(jsonSpan.endTimeUnixNano - jsonSpan.startTimeUnixNano)
                    .withEndTime(convertUnixNanosToISO8601(jsonSpan.endTimeUnixNano))
                    .withStatusCode(jsonSpan.status.code);
        }
        return traceGroupFieldsBuilder.build();
    }

    private static String convertUnixNanosToISO8601(final long unixNano) {
        return Instant.ofEpochSecond(0L, unixNano).toString();
    }
}
