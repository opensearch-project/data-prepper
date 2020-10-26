package com.amazon.situp.research.zipkin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class ZipkinElasticToOtelProcessor {
    public static String SPAN_ID = "id";
    public static String NAME = "name";
    public static String TRACE_ID = "traceId";
    public static String TIME_STAMP = "timestamp";
    public static String DURATION = "duration";
    public static String SPAN_KIND = "kind";
    public static String PARENT_ID = "parentId";
    public static String LOCAL_ENDPOINT = "localEndpoint";
    public static String SERVICE_NAME = "serviceName";
    public static String TAGS = "tags";
    public static String HTTP_STATUS_CODE = "http.status_code";
    public static String RESPONSE_STATUS = "response.status";
    public static String ERROR = "error";
    public static String RETURN_VALUE = "return value";
    public static String STATUS_CODE_VALUE = "statusCodeValue";

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {};

    public static Span sourceToSpan(final Map<String, Object> source) {
        final String traceID = (String) source.get(TRACE_ID);
        final String spanID = (String) source.get(SPAN_ID);
        final String name = (String) source.get(NAME);
        final Long startTime = (Long) source.get(TIME_STAMP);
        final Long duration = Long.valueOf((Integer) source.get(DURATION));
        final long endTime = startTime + duration;
        final String parentID = (String) source.get(PARENT_ID);
        final String spanKind = (String) source.get(SPAN_KIND);
        // TODO: read span status from tags
        final Map<String, Object> tags = (Map<String, Object>) source.get(TAGS);
        Integer statusCode = null;
        if (tags != null) {
            statusCode = extractStatusCodeFromTags(tags);
        }

        final Span.Builder spanBuilder = Span.newBuilder()
                .setStartTimeUnixNano(startTime * 1000) // Convert to UnixNano
                .setEndTimeUnixNano(endTime * 1000); // Convert to UnixNano

        if (traceID != null) {
            spanBuilder.setTraceId(ByteString.copyFromUtf8(traceID));
        }
        if (spanID != null) {
            spanBuilder.setSpanId(ByteString.copyFromUtf8(spanID));
        }
        if (name != null) {
            spanBuilder.setName(name);
        }
        if (parentID != null) {
            spanBuilder.setParentSpanId(ByteString.copyFromUtf8(parentID));
        }
        if (spanKind != null) {
            spanBuilder.setKind(Span.SpanKind.valueOf(spanKind));
        }
        if (statusCode != null) {
            spanBuilder.setStatus(Status.newBuilder().setCodeValue(statusCode));
        }

        return spanBuilder.build();
    }

    public static Integer extractStatusCodeFromTags(final Map<String, Object> tags) {
        for (final Map.Entry<String, Object> entry:tags.entrySet()) {
            final String key = entry.getKey();
            if (key.equals(HTTP_STATUS_CODE)) {
                final String value = (String) entry.getValue();
                if (StringUtils.isNumeric(value)) {
                    return Integer.parseInt(value);
                }
            } else if (key.equals(RESPONSE_STATUS)) {
                final String value = (String) entry.getValue();
                if (StringUtils.isNumeric(value)) {
                    return Integer.parseInt(value);
                }
            } else if (key.equals(ERROR)) {
                final String value = (String) entry.getValue();
                if (StringUtils.isNumeric(value)) {
                    return Integer.parseInt(value);
                }
            } else if (key.equals(RETURN_VALUE)) {
                // Extract only if value is json string
                try {
                    final Map<String, Object> value = mapper.readValue((String) entry.getValue(), typeRef);
                    if (value != null) {
                        final Integer statusCodeValue = (Integer) value.get(STATUS_CODE_VALUE);
                        if (statusCodeValue != null) {
                            return statusCodeValue;
                        }
                    }
                } catch (final JsonProcessingException ignored) { }
            }
        }

        return null;
    }

    public static ExportTraceServiceRequest sourcesToRequest(final List<Map<String, Object>> sources) {
        final ExportTraceServiceRequest.Builder exportTraceServiceRequestBuilder = ExportTraceServiceRequest.newBuilder();
        final Map<String, List<Map<String, Object>>> sourceByService = groupSourceByService(sources);
        for (final Map.Entry<String, List<Map<String, Object>>> entry : sourceByService.entrySet()) {
            final String serviceName = entry.getKey();
            final List<Map<String, Object>> sourceGroup = entry.getValue();
            final ResourceSpans.Builder rsBuilder = ResourceSpans.newBuilder().setResource(Resource.newBuilder()
                    .addAttributes(KeyValue.newBuilder()
                            .setKey(SERVICE_NAME)
                            .setValue(AnyValue.newBuilder().setStringValue(serviceName))
                            .build()
                    )
                    .build()
            );
            // TODO: add library name and version
            final InstrumentationLibrarySpans.Builder isBuilder =
                    InstrumentationLibrarySpans.newBuilder();
            final List<Span> spanGroup = sourceGroup.stream()
                    .map(ZipkinElasticToOtelProcessor::sourceToSpan).collect(Collectors.toList());
            isBuilder.addAllSpans(spanGroup);
            rsBuilder.addInstrumentationLibrarySpans(isBuilder);
            exportTraceServiceRequestBuilder.addResourceSpans(rsBuilder);
        }

        return exportTraceServiceRequestBuilder.build();
    }

    public static Map<String, List<Map<String, Object>>> groupSourceByService(final List<Map<String, Object>> sources) {
        final Map<String, List<Map<String, Object>>> sourceByService = new HashMap<>();
        for (final Map<String, Object> source: sources) {
            String serviceName = null;
            final Map<String, Object> localEndpoint = (Map<String, Object>) source.get(LOCAL_ENDPOINT);
            if (localEndpoint != null) {
                serviceName = (String) localEndpoint.get(SERVICE_NAME);
            }
            if (sourceByService.containsKey(serviceName)) {
                sourceByService.get(serviceName).add(source);
            } else {
                sourceByService.put(serviceName, new ArrayList<>(Arrays.asList(source)));
            }
        }
        return sourceByService;
    }
}
