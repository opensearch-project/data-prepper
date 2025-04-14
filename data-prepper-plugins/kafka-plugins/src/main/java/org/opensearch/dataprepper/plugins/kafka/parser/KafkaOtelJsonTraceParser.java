package org.opensearch.dataprepper.plugins.kafka.parser;

import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;


/**
 * Parsing Open Telemetry trace json format from kafka topic to Spans.
 */
public class KafkaOtelJsonTraceParser extends OTelProtoOpensearchCodec.OTelProtoDecoder {

    static final String INSTRUMENTATION_SCOPE_NAME = "instrumentationScope.name";
    static final String INSTRUMENTATION_SCOPE_VERSION = "instrumentationScope.version";
    static final String INSTRUMENTATION_SCOPE_ATTRIBUTES = "instrumentationScope.attributes";

    public List<Span> parse(String inputJson, Instant receivedTimeStamp, Consumer<Record<Event>> eventConsumer) throws IOException {

        List<Span> spans = parseSpans(inputJson, receivedTimeStamp);

        for (Span span : spans) {
            eventConsumer.accept(new Record<>(span));
        }
        return spans;
    }

    private List<Span> parseSpans(String inputJson, Instant timeReceived) throws IOException {
        TracesData.Builder builder = TracesData.newBuilder();
        JsonFormat.parser().merge(inputJson, builder);
        TracesData build = builder.build();
        List<ResourceSpans> resourceSpansList = build.getResourceSpansList();


        List<Span> spans = new ArrayList<>();
        for (ResourceSpans resourceSpans : resourceSpansList) {
            String serviceName = readServiceName(resourceSpans.getResource());
            Map<String, Object> resourceAttributes = getResourceAttributes(resourceSpans.getResource());

            for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                Map<String, Object> instrumentationScopeAttributes = getInstrumentationScopeAttributes(scopeSpans);
                for (io.opentelemetry.proto.trace.v1.Span span : scopeSpans.getSpansList()) {
                    spans.add(super.parseSpan(span, instrumentationScopeAttributes, serviceName, resourceAttributes, timeReceived));
                }
            }
        }
        return spans;
    }

    public static Map<String, Object> getInstrumentationScopeAttributes(final ScopeSpans scopeSpans) {
        final Map<String, Object> instrumentationScopeAttr = new HashMap<>();
        scopeSpans.getScope().getName();
        if (!scopeSpans.getScope().getName().isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_NAME, scopeSpans.getScope().getName());
        }
        if (!scopeSpans.getScope().getVersion().isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_VERSION, scopeSpans.getScope().getVersion());
        }
        if (!scopeSpans.getScope().getAttributesList().isEmpty()) {
            for (KeyValue attribute : scopeSpans.getScope().getAttributesList()) {
                instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_ATTRIBUTES + attribute.getKey(), attribute.getValue());
            }
        }
        return instrumentationScopeAttr;
    }

    private String readServiceName(Resource resource) {
        for (KeyValue attribute : resource.getAttributesList()) {
            if ("service.name".equals(attribute.getKey())) {
                return attribute.getValue().getStringValue();
            }
        }
        return "";
    }
}
