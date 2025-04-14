package org.opensearch.dataprepper.plugins.kafka.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.*;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Parsing Open Telemetry trace json format from kafka topic to Spans.
 */
public class KafkaOtelJsonTraceParser {

    private static final String STATUS_CODE = "status.code";
    private static final String STATUS_MESSAGE = "status.message";
    static final String INSTRUMENTATION_SCOPE_NAME = "instrumentationScope.name";
    static final String INSTRUMENTATION_SCOPE_VERSION = "instrumentationScope.version";
    static final String INSTRUMENTATION_SCOPE_ATTRIBUTES = "instrumentationScope.attributes";
    ObjectMapper objectMapper = new ObjectMapper();

    public List<Span> parse(String inputJson, Instant receivedTimeStamp, Consumer<Record<Event>> eventConsumer) throws IOException {

        List<Span> spans = parseSpans(inputJson, receivedTimeStamp);

        for (Span span : spans) {
            eventConsumer.accept(new Record<>(span));
        }
        return spans;
    }

    private List<Span> parseSpans(String inputJson, Instant receivedTimeStamp) throws IOException {
        JsonOtelTracePojo otelTracePojo = objectMapper.readValue(inputJson, JsonOtelTracePojo.class);

        String serviceName = readServiceName(otelTracePojo);
        Map<String, Object> resourceAttributes = getResourcesAttributes(otelTracePojo);
        List<Span> spans = new ArrayList<>();
        for (JsonOtelTracePojo.ResourceSpan resourceSpan : otelTracePojo.resourceSpans) {
            for (JsonOtelTracePojo.ScopeSpan scopeSpan : resourceSpan.scopeSpans) {
                Map<String, Object> instrumentationScopeAttributes = getInstrumentationScopeAttributes(scopeSpan);
                for (JsonOtelTracePojo.Span jsonSpan : scopeSpan.spans) {
                    spans.add(parseSpan(jsonSpan, serviceName, resourceAttributes, instrumentationScopeAttributes, receivedTimeStamp));
                }
            }
        }


        return spans;
    }

    public static Map<String, Object> getInstrumentationScopeAttributes(final JsonOtelTracePojo.ScopeSpan scopeSpan) {
        final Map<String, Object> instrumentationScopeAttr = new HashMap<>();
        if (scopeSpan.scope.name != null && !scopeSpan.scope.name.isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_NAME, scopeSpan.scope.name);
        }
        if (scopeSpan.scope.version != null && !scopeSpan.scope.version.isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_VERSION, scopeSpan.scope.version);
        }
        if (scopeSpan.scope.attributes != null && !scopeSpan.scope.attributes.isEmpty()) {
            for (JsonOtelTracePojo.Attribute attribute : scopeSpan.scope.attributes) {
                instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_ATTRIBUTES + attribute.key, attribute.value.getValue());
            }
        }
        return instrumentationScopeAttr;
    }

    private Span parseSpan(JsonOtelTracePojo.Span jsonSpan, String serviceName, Map<String, Object> resourceAttributes,
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

    private String readServiceName(JsonOtelTracePojo otelTracePojo) {
        for (JsonOtelTracePojo.ResourceSpan resourceSpan : otelTracePojo.resourceSpans) {
            for (JsonOtelTracePojo.Attribute attribute : resourceSpan.resource.attributes) {
                if ("service.name".equals(attribute.key)) {
                    return attribute.value.stringValue;
                }
            }
        }
        return "";
    }

    private Map<String, Object> getResourcesAttributes(JsonOtelTracePojo otelTracePojo) {
        Map<String, Object> resourceAttributes = new HashMap<>();
        for (JsonOtelTracePojo.ResourceSpan resourceSpan : otelTracePojo.resourceSpans) {
            for (JsonOtelTracePojo.Attribute attribute : resourceSpan.resource.attributes) {
                resourceAttributes.put(attribute.key, attribute.value.getValue());
            }
        }
        return resourceAttributes;
    }

    private Map<String, Object> getSpanAttributes(JsonOtelTracePojo.Span jsonSpan) {
        Map<String, Object> spanAttributes = new HashMap<>();
        if (jsonSpan.attributes == null) {
            return spanAttributes;
        }
        for (JsonOtelTracePojo.Attribute attribute : jsonSpan.attributes) {
            spanAttributes.put(attribute.key, attribute.value.getValue());
        }
        return spanAttributes;
    }

    private Map<String, Object> getSpanStatusAttributes(final JsonOtelTracePojo.Status status) {
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

    private List<SpanEvent> getSpanEvents(JsonOtelTracePojo.Span jsonSpan) {

        if (jsonSpan.events == null || jsonSpan.events.isEmpty()) {
            return new ArrayList<>();
        }

        List<SpanEvent> spanEvents = new ArrayList<>();
        for (JsonOtelTracePojo.Event event : jsonSpan.events) {

            Map<String, Object> attributes = new HashMap<>();
            for (JsonOtelTracePojo.Attribute attribute : event.attributes) {
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

    private String getTraceGroup(JsonOtelTracePojo.Span jsonSpan) {
        return jsonSpan.parentSpanId.isEmpty() ? jsonSpan.name : null;
    }

    private TraceGroupFields getTraceGroupFields(JsonOtelTracePojo.Span jsonSpan) {
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
