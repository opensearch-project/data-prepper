/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;
import org.opensearch.dataprepper.model.log.JacksonOtelLog;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.DefaultExemplar;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultLink;
import org.opensearch.dataprepper.model.trace.DefaultSpanEvent;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Link;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.SpanEvent;
import org.opensearch.dataprepper.model.trace.TraceGroupFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCommonUtils.convertUnixNanosToISO8601;
import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCommonUtils.convertISO8601ToNanos;
import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCommonUtils.convertByteStringToString;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * OTelProtoOpensearchCodec is for encoding/decoding between {@link org.opensearch.dataprepper.model.trace} and {@link io.opentelemetry.proto} in Opensearch friendly way.
 */
public class OTelProtoOpensearchCodec {

    private static final Logger LOG = LoggerFactory.getLogger(OTelProtoCodec.class);
    public static final int DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE = 10;
    private static final double OTEL_NEGATIVE_INFINITY = -Float.MAX_VALUE;
    private static final double OTEL_POSITIVE_INFINITY = Float.MAX_VALUE;

    private static final ObjectMapper OBJECT_MAPPER =  new ObjectMapper();
    private static final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
    protected static final String SERVICE_NAME = "service.name";
    protected static final String SPAN_ATTRIBUTES = "span.attributes";
    static final String RESOURCE_ATTRIBUTES = "resource.attributes";
    static final String STATUS_CODE = "status.code";
    static final String STATUS_MESSAGE = "status.message";


    /**
     * To make it OpenSearch friendly we will replace '.' in keys with '@' in all the Keys in {@link io.opentelemetry.proto.common.v1.KeyValue}
     */
    private static final String DOT = ".";
    private static final String AT = "@";
    private static final String LOG_ATTRIBUTES = "log.attributes";
    private static final String METRIC_ATTRIBUTES = "metric.attributes";
    private static final String EXEMPLAR_ATTRIBUTES = "exemplar.attributes";
    static final String INSTRUMENTATION_SCOPE_NAME = "instrumentationScope.name";
    static final String INSTRUMENTATION_SCOPE_VERSION = "instrumentationScope.version";
    static final String INSTRUMENTATION_SCOPE_ATTRIBUTES = "instrumentationScope.attributes";

    public static final Function<String, String> REPLACE_DOT_WITH_AT = i -> i.replace(DOT, AT);
    /**
     * Span and Resource attributes are essential for OpenSearch so they should not be nested. SO we will prefix them with "span.attributes"
     * and "resource.attributes".
     *
     */
    public static final Function<String, String> SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> SPAN_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> RESOURCE_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> PREFIX_AND_LOG_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> LOG_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> PREFIX_AND_METRIC_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> METRIC_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> PREFIX_AND_RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> RESOURCE_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> PREFIX_AND_EXEMPLAR_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> EXEMPLAR_ATTRIBUTES + DOT + i.replace(DOT, AT);

    private static final Map<BoundsKey, double[]> EXPONENTIAL_BUCKET_BOUNDS = new ConcurrentHashMap<>();

    static class BoundsKey {
        private final Integer scale;
        private final Sign sign;

        public enum Sign {POSITIVE, NEGATIVE};

        public BoundsKey(Integer scale, Sign sign) {
            this.scale = scale;
            this.sign = sign;
        }

        public Integer getScale() {
            return scale;
        }

        public Sign getSign() {
            return sign;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BoundsKey boundsKey = (BoundsKey) o;
            return scale.equals(boundsKey.scale) && sign == boundsKey.sign;
        }

        @Override
        public int hashCode() {
            return Objects.hash(scale, sign);
        }
    }

    public static class OTelProtoDecoder implements OTelProtoCodec.OTelProtoDecoder {

        public List<Span> parseExportTraceServiceRequest(final ExportTraceServiceRequest exportTraceServiceRequest, final Instant timeReceived) {
            return exportTraceServiceRequest.getResourceSpansList().stream()
                    .flatMap(rs -> parseResourceSpans(rs, timeReceived).stream()).collect(Collectors.toList());
        }

        public Map<String, ExportTraceServiceRequest> splitExportTraceServiceRequestByTraceId(final ExportTraceServiceRequest exportTraceServiceRequest) {
            Map<String, ExportTraceServiceRequest> result = new HashMap<>();
            Map<String, ExportTraceServiceRequest.Builder> resultBuilderMap = new HashMap<>();
            for (final ResourceSpans resourceSpans: exportTraceServiceRequest.getResourceSpansList()) {
                for (Map.Entry<String, ResourceSpans> entry: splitResourceSpansByTraceId(resourceSpans).entrySet()) {
                    String traceId = entry.getKey();

                    if (resultBuilderMap.containsKey(traceId)) {
                        resultBuilderMap.get(traceId).addResourceSpans(entry.getValue());
                    } else {
                        resultBuilderMap.put(traceId, ExportTraceServiceRequest.newBuilder().addResourceSpans(entry.getValue()));
                    }
                }
            }
            for (Map.Entry<String, ExportTraceServiceRequest.Builder> entry: resultBuilderMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue().build());
            }
            return result;
        }

        public List<OpenTelemetryLog> parseExportLogsServiceRequest(final ExportLogsServiceRequest exportLogsServiceRequest, final Instant timeReceived) {
            return exportLogsServiceRequest.getResourceLogsList().stream()
                    .flatMap(rs -> parseResourceLogs(rs, timeReceived)).collect(Collectors.toList());
        }

        protected Stream<OpenTelemetryLog> parseResourceLogs(ResourceLogs rs, final Instant timeReceived) {
            final String serviceName = OTelProtoOpensearchCodec.getServiceName(rs.getResource()).orElse(null);
            final Map<String, Object> resourceAttributes = OTelProtoOpensearchCodec.getResourceAttributes(rs.getResource());
            final String schemaUrl = rs.getSchemaUrl();

            Stream<OpenTelemetryLog> mappedScopeListLogs = rs.getScopeLogsList()
                    .stream()
                    .map(sls ->
                            processLogsList(sls.getLogRecordsList(),
                                    serviceName,
                                    OTelProtoOpensearchCodec.getInstrumentationScopeAttributes(sls.getScope()),
                                    resourceAttributes,
                                    schemaUrl,
                                    timeReceived))
                    .flatMap(Collection::stream);

            return mappedScopeListLogs;
        }

        protected Map<String, ResourceSpans> splitResourceSpansByTraceId(final ResourceSpans resourceSpans) {
            final Resource resource = resourceSpans.getResource();
            final boolean hasResource = resourceSpans.hasResource();
            Map<String, ResourceSpans> result = new HashMap<>();
            Map<String, ResourceSpans.Builder> resultBuilderMap = new HashMap<>();

            if (!resourceSpans.getScopeSpansList().isEmpty()) {
                for (Map.Entry<String, List<ScopeSpans>> entry: splitScopeSpansByTraceId(resourceSpans.getScopeSpansList()).entrySet()) {
                    ResourceSpans.Builder resourceSpansBuilder = ResourceSpans.newBuilder().addAllScopeSpans(entry.getValue());
                    if (hasResource) {
                        resourceSpansBuilder.setResource(resource);
                    }
                    resultBuilderMap.put(entry.getKey(), resourceSpansBuilder);
                }
            }

            for (Map.Entry<String, ResourceSpans.Builder> entry: resultBuilderMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue().build());
            }

            return result;
        }

        protected List<Span> parseResourceSpans(final ResourceSpans resourceSpans, final Instant timeReceived) {
            final String serviceName = getServiceName(resourceSpans.getResource()).orElse(null);
            final Map<String, Object> resourceAttributes = getResourceAttributes(resourceSpans.getResource());

            if (!resourceSpans.getScopeSpansList().isEmpty()) {
                return parseScopeSpans(resourceSpans.getScopeSpansList(), serviceName, resourceAttributes, timeReceived);
            }

            LOG.debug("No spans found to parse from ResourceSpans object: {}", resourceSpans);
            return Collections.emptyList();
        }

        private List<Span> parseScopeSpans(final List<ScopeSpans> scopeSpansList, final String serviceName, final Map<String, Object> resourceAttributes, final Instant timeReceived) {
            return scopeSpansList.stream()
                    .map(scopeSpans -> parseSpans(scopeSpans.getSpansList(), scopeSpans.getScope(),
                            OTelProtoOpensearchCodec::getInstrumentationScopeAttributes, serviceName, resourceAttributes, timeReceived))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        private Map<String, List<ScopeSpans>> splitScopeSpansByTraceId(final List<ScopeSpans> scopeSpansList) {
            Map<String, List<ScopeSpans>> result = new HashMap<>();
            for (ScopeSpans ss: scopeSpansList) {
                final boolean hasScope = ss.hasScope();
                final io.opentelemetry.proto.common.v1.InstrumentationScope scope = ss.getScope();
                for (Map.Entry<String, List<io.opentelemetry.proto.trace.v1.Span>> entry: splitSpansByTraceId(ss.getSpansList()).entrySet()) {
                    ScopeSpans.Builder scopeSpansBuilder = ScopeSpans.newBuilder().addAllSpans(entry.getValue());
                    if (hasScope) {
                        scopeSpansBuilder.setScope(scope);
                    }
                    String traceId = entry.getKey();
                    if (!result.containsKey(traceId)) {
                        result.put(traceId, new ArrayList<>());
                    }
                    result.get(traceId).add(scopeSpansBuilder.build());
                }
            }
            return result;
        }


        private Map<String, List<io.opentelemetry.proto.trace.v1.Span>> splitSpansByTraceId(final List<io.opentelemetry.proto.trace.v1.Span> spans) {
            Map<String, List<io.opentelemetry.proto.trace.v1.Span>> result = new HashMap<>();
            for (io.opentelemetry.proto.trace.v1.Span span: spans) {
                String traceId = convertByteStringToString(span.getTraceId());
                List<io.opentelemetry.proto.trace.v1.Span> spanList;
                if (result.containsKey(traceId)) {
                    spanList = result.get(traceId);
                } else {
                    spanList = new ArrayList<>();
                    result.put(traceId, spanList);
                }
                spanList.add(span);
            }
            return result;
        }

        private <T> List<Span> parseSpans(final List<io.opentelemetry.proto.trace.v1.Span> spans, final T scope,
                                          final Function<T, Map<String, Object>> scopeAttributesGetter,
                                          final String serviceName, final Map<String, Object> resourceAttributes,
                                          final Instant timeReceived) {
            return spans.stream()
                    .map(span -> {
                        final Map<String, Object> scopeAttributes = scopeAttributesGetter.apply(scope);
                        return parseSpan(span, scopeAttributes, serviceName, resourceAttributes, timeReceived);
                    })
                    .collect(Collectors.toList());
        }

        protected List<OpenTelemetryLog> processLogsList(final List<LogRecord> logsList,
                                                         final String serviceName,
                                                         final Map<String, Object> ils,
                                                         final Map<String, Object> resourceAttributes,
                                                         final String schemaUrl,
                                                         final Instant timeReceived) {
            return logsList.stream()
                    .map(log -> JacksonOtelLog.builder()
                            .withTime(convertUnixNanosToISO8601(log.getTimeUnixNano()))
                            .withObservedTime(convertUnixNanosToISO8601(log.getObservedTimeUnixNano()))
                            .withServiceName(serviceName)
                            .withAttributes(mergeAllAttributes(
                                    Arrays.asList(
                                            unpackKeyValueListLog(log.getAttributesList()),
                                            resourceAttributes,
                                            ils
                                    )
                            ))
                            .withSchemaUrl(schemaUrl)
                            .withFlags(log.getFlags())
                            .withTraceId(convertByteStringToString(log.getTraceId()))
                            .withSpanId(convertByteStringToString(log.getSpanId()))
                            .withSeverityNumber(log.getSeverityNumberValue())
                            .withSeverityText(log.getSeverityText())
                            .withDroppedAttributesCount(log.getDroppedAttributesCount())
                            .withBody(convertAnyValue(log.getBody()))
                            .withTimeReceived(timeReceived)
                            .build())
                    .collect(Collectors.toList());
        }

        protected Span parseSpan(final io.opentelemetry.proto.trace.v1.Span sp, final Map<String, Object> instrumentationScopeAttributes,
                                    final String serviceName, final Map<String, Object> resourceAttributes, final Instant timeReceived) {
            return JacksonSpan.builder()
                    .withSpanId(convertByteStringToString(sp.getSpanId()))
                    .withTraceId(convertByteStringToString(sp.getTraceId()))
                    .withTraceState(sp.getTraceState())
                    .withParentSpanId(convertByteStringToString(sp.getParentSpanId()))
                    .withName(sp.getName())
                    .withServiceName(serviceName)
                    .withKind(sp.getKind().name())
                    .withStartTime(convertUnixNanosToISO8601(sp.getStartTimeUnixNano()))
                    .withEndTime(convertUnixNanosToISO8601(sp.getEndTimeUnixNano()))
                    .withAttributes(mergeAllAttributes(
                            Arrays.asList(
                                    getSpanAttributes(sp),
                                    resourceAttributes,
                                    instrumentationScopeAttributes,
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
                    .withTimeReceived(timeReceived)
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
                        throw new OTelDecodingException(e);
                    }
                case KVLIST_VALUE:
                    try {
                        return OBJECT_MAPPER.writeValueAsString(value.getKvlistValue().getValuesList().stream()
                                .collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()),
                                        i ->convertAnyValue(i.getValue()))));
                    } catch (JsonProcessingException e) {
                        throw new OTelDecodingException(e);
                    }
                default:
                    throw new OTelDecodingException("Unknown case");
            }
        }

        protected Map<String, Object> mergeAllAttributes(final Collection<Map<String, Object>> attributes) {
            return attributes.stream()
                    .flatMap(map -> map.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        protected SpanEvent getSpanEvent(final io.opentelemetry.proto.trace.v1.Span.Event event) {
            return DefaultSpanEvent.builder()
                    .withTime(convertUnixNanosToISO8601(event.getTimeUnixNano()))
                    .withName(event.getName())
                    .withAttributes(getEventAttributes(event))
                    .withDroppedAttributesCount(event.getDroppedAttributesCount())
                    .build();
        }

        protected Link getLink(final io.opentelemetry.proto.trace.v1.Span.Link link) {
            return DefaultLink.builder()
                    .withSpanId(convertByteStringToString(link.getSpanId()))
                    .withTraceId(convertByteStringToString(link.getTraceId()))
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
         * @param span opentelemetry-protobuf span
         * @return TraceGroup string
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
                        .withEndTime(convertUnixNanosToISO8601(span.getEndTimeUnixNano()))
         *             .withStatusCode(span.getStatus().getCodeValue());
         * }
         * <p>
         * Note: The reason this method is part of the codec class is because the trace group definition will be expanded in the future when we support Links in OpenSearch Dashboards Trace Analytics.
         * @param span opentelemetry-protobuf span
         * @return TraceGroupFields
         */
        protected TraceGroupFields getTraceGroupFields(final io.opentelemetry.proto.trace.v1.Span span) {
            DefaultTraceGroupFields.Builder traceGroupFieldsBuilder = DefaultTraceGroupFields.builder();
            if (span.getParentSpanId().isEmpty()) {
                traceGroupFieldsBuilder = traceGroupFieldsBuilder
                        .withDurationInNanos(span.getEndTimeUnixNano() - span.getStartTimeUnixNano())
                        .withEndTime(convertUnixNanosToISO8601(span.getEndTimeUnixNano()))
                        .withStatusCode(span.getStatus().getCodeValue());
            }
            return traceGroupFieldsBuilder.build();
        }

        protected Map<String, Object> getSpanStatusAttributes(final Status status) {
            final Map<String, Object> statusAttr = new HashMap<>();
            statusAttr.put(STATUS_CODE, status.getCodeValue());
            if(!status.getMessage().isEmpty()){
                statusAttr.put(STATUS_MESSAGE, status.getMessage());
            }
            return statusAttr;
        }

        protected Optional<String> getServiceName(final Resource resource) {
            return resource.getAttributesList().stream().filter(
                    keyValue -> keyValue.getKey().equals(SERVICE_NAME)
                            && !keyValue.getValue().getStringValue().isEmpty()
            ).findFirst().map(i -> i.getValue().getStringValue());
        }

        public Collection<Record<? extends Metric>> parseExportMetricsServiceRequest(
                            final ExportMetricsServiceRequest request,
                            AtomicInteger droppedCounter,
                            final Integer exponentialHistogramMaxAllowedScale,
                            final Instant timeReceived,
                            final boolean calculateHistogramBuckets,
                            final boolean calculateExponentialHistogramBuckets,
                            final boolean flattenAttributes) {
            Collection<Record<? extends Metric>> recordsOut = new ArrayList<>();
            for (ResourceMetrics rs : request.getResourceMetricsList()) {
                final String schemaUrl = rs.getSchemaUrl();
                final Map<String, Object> resourceAttributes = OTelProtoOpensearchCodec.getResourceAttributes(rs.getResource());
                final String serviceName = OTelProtoOpensearchCodec.getServiceName(rs.getResource()).orElse(null);

                for (ScopeMetrics sm : rs.getScopeMetricsList()) {
                    final Map<String, Object> ils = OTelProtoOpensearchCodec.getInstrumentationScopeAttributes(sm.getScope());
                    recordsOut.addAll(processMetricsList(sm.getMetricsList(), serviceName, ils, resourceAttributes, schemaUrl, droppedCounter, exponentialHistogramMaxAllowedScale, timeReceived, calculateHistogramBuckets, calculateExponentialHistogramBuckets, flattenAttributes));
                }
            }
            return recordsOut;
        }

        private List<? extends Record<? extends Metric>> processMetricsList(
                    final List<io.opentelemetry.proto.metrics.v1.Metric> metricsList,
                    final String serviceName,
                    final Map<String, Object> ils,
                    final Map<String, Object> resourceAttributes,
                    final String schemaUrl,
                    AtomicInteger droppedCounter,
                    final Integer exponentialHistogramMaxAllowedScale,
                    final Instant timeReceived,
                    final boolean calculateHistogramBuckets,
                    final boolean calculateExponentialHistogramBuckets,
                    final boolean flattenAttributes) {
            List<Record<? extends Metric>> recordsOut = new ArrayList<>();
            for (io.opentelemetry.proto.metrics.v1.Metric metric : metricsList) {
                try {
                    if (metric.hasGauge()) {
                        recordsOut.addAll(mapGauge(metric, serviceName, ils, resourceAttributes, schemaUrl, timeReceived, flattenAttributes));
                    } else if (metric.hasSum()) {
                        recordsOut.addAll(mapSum(metric, serviceName, ils, resourceAttributes, schemaUrl, timeReceived, flattenAttributes));
                    } else if (metric.hasSummary()) {
                        recordsOut.addAll(mapSummary(metric, serviceName, ils, resourceAttributes, schemaUrl, timeReceived, flattenAttributes));
                    } else if (metric.hasHistogram()) {
                        recordsOut.addAll(mapHistogram(metric, serviceName, ils, resourceAttributes, schemaUrl, timeReceived, calculateHistogramBuckets, flattenAttributes));
                    } else if (metric.hasExponentialHistogram()) {
                        recordsOut.addAll(mapExponentialHistogram(metric, serviceName, ils, resourceAttributes, schemaUrl, exponentialHistogramMaxAllowedScale, timeReceived, calculateExponentialHistogramBuckets, flattenAttributes));
                    }
                } catch (Exception e) {
                    LOG.warn("Error while processing metrics", e);
                    droppedCounter.incrementAndGet();
                }
            }
            return recordsOut;
        }

        private List<? extends Record<? extends Metric>> mapGauge(
                                         io.opentelemetry.proto.metrics.v1.Metric metric,
                                         String serviceName,
                                         final Map<String, Object> ils,
                                         final Map<String, Object> resourceAttributes,
                                         final String schemaUrl,
                                         final Instant timeReceived,
                                         final boolean flattenAttributes) {
            return metric.getGauge().getDataPointsList().stream()
                .map(dp -> JacksonGauge.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                        .withTime(convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                        .withServiceName(serviceName)
                        .withValue(OTelProtoOpensearchCodec.getValueAsDouble(dp))
                        .withAttributes(OTelProtoOpensearchCodec.mergeAllAttributes(
                                Arrays.asList(
                                        OTelProtoOpensearchCodec.convertKeysOfDataPointAttributes(dp),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .withSchemaUrl(schemaUrl)
                        .withExemplars(OTelProtoOpensearchCodec.convertExemplars(dp.getExemplarsList()))
                        .withFlags(dp.getFlags())
                        .withTimeReceived(timeReceived)
                        .build(flattenAttributes))
                .map(Record::new)
                .collect(Collectors.toList());
        }

        private List<? extends Record<? extends Metric>> mapSum(
                                     final io.opentelemetry.proto.metrics.v1.Metric metric,
                                     final String serviceName,
                                     final Map<String, Object> ils,
                                     final Map<String, Object> resourceAttributes,
                                     final String schemaUrl,
                                     final Instant timeReceived,
                                     final boolean flattenAttributes) {
            return metric.getSum().getDataPointsList().stream()
                .map(dp -> JacksonSum.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                        .withTime(convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                        .withServiceName(serviceName)
                        .withIsMonotonic(metric.getSum().getIsMonotonic())
                        .withValue(OTelProtoOpensearchCodec.getValueAsDouble(dp))
                        .withAggregationTemporality(metric.getSum().getAggregationTemporality().toString())
                        .withAttributes(OTelProtoOpensearchCodec.mergeAllAttributes(
                                Arrays.asList(
                                        OTelProtoOpensearchCodec.convertKeysOfDataPointAttributes(dp),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .withSchemaUrl(schemaUrl)
                        .withExemplars(OTelProtoOpensearchCodec.convertExemplars(dp.getExemplarsList()))
                        .withFlags(dp.getFlags())
                        .withTimeReceived(timeReceived)
                        .build(flattenAttributes))
                .map(Record::new)
                .collect(Collectors.toList());
        }

        private List<? extends Record<? extends Metric>> mapSummary(
                                             final io.opentelemetry.proto.metrics.v1.Metric metric,
                                             final String serviceName,
                                             final Map<String, Object> ils,
                                             final Map<String, Object> resourceAttributes,
                                             final String schemaUrl,
                                             final Instant timeReceived,
                                             final boolean flattenAttributes) {
            return metric.getSummary().getDataPointsList().stream()
                .map(dp -> JacksonSummary.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                        .withTime(convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                        .withServiceName(serviceName)
                        .withCount(dp.getCount())
                        .withSum(dp.getSum())
                        .withQuantiles(OTelProtoOpensearchCodec.getQuantileValues(dp.getQuantileValuesList()))
                        .withQuantilesValueCount(dp.getQuantileValuesCount())
                        .withAttributes(OTelProtoOpensearchCodec.mergeAllAttributes(
                                Arrays.asList(
                                        OTelProtoOpensearchCodec.unpackKeyValueListMetric(dp.getAttributesList()),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .withSchemaUrl(schemaUrl)
                        .withFlags(dp.getFlags())
                        .withTimeReceived(timeReceived)
                        .build(flattenAttributes))
                .map(Record::new)
                .collect(Collectors.toList());
        }

        private List<? extends Record<? extends Metric>> mapHistogram(
                                                 final io.opentelemetry.proto.metrics.v1.Metric metric,
                                                 final String serviceName,
                                                 final Map<String, Object> ils,
                                                 final Map<String, Object> resourceAttributes,
                                                 final String schemaUrl,
                                                 final Instant timeReceived,
                                                 final boolean calculateHistogramBuckets,
                                                 final boolean flattenAttributes) {
            return metric.getHistogram().getDataPointsList().stream()
                .map(dp -> {
                    JacksonHistogram.Builder builder = JacksonHistogram.builder()
                            .withUnit(metric.getUnit())
                            .withName(metric.getName())
                            .withDescription(metric.getDescription())
                            .withStartTime(convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                            .withTime(convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                            .withServiceName(serviceName)
                            .withSum(dp.getSum())
                            .withCount(dp.getCount())
                            .withBucketCount(dp.getBucketCountsCount())
                            .withExplicitBoundsCount(dp.getExplicitBoundsCount())
                            .withAggregationTemporality(metric.getHistogram().getAggregationTemporality().toString())
                            .withBucketCountsList(dp.getBucketCountsList())
                            .withExplicitBoundsList(dp.getExplicitBoundsList())
                            .withAttributes(OTelProtoOpensearchCodec.mergeAllAttributes(
                                    Arrays.asList(
                                            OTelProtoOpensearchCodec.unpackKeyValueListMetric(dp.getAttributesList()),
                                            resourceAttributes,
                                            ils
                                    )
                            ))
                            .withSchemaUrl(schemaUrl)
                            .withExemplars(OTelProtoOpensearchCodec.convertExemplars(dp.getExemplarsList()))
                            .withTimeReceived(timeReceived)
                            .withFlags(dp.getFlags());
                    if (calculateHistogramBuckets) {
                        builder.withBuckets(OTelProtoOpensearchCodec.createBuckets(dp.getBucketCountsList(), dp.getExplicitBoundsList()));
                    }
                    JacksonHistogram jh = builder.build(flattenAttributes);
                    return jh;

                })
                .map(Record::new)
                .collect(Collectors.toList());
        }

        private List<? extends Record<? extends Metric>> mapExponentialHistogram(
                                            final io.opentelemetry.proto.metrics.v1.Metric metric,
                                            final String serviceName,
                                            final Map<String, Object> ils,
                                            final Map<String, Object> resourceAttributes,
                                            final String schemaUrl,
                                            final Integer exponentialHistogramMaxAllowedScale,
                                            final Instant timeReceived,
                                            final boolean calculateExponentialHistogramBuckets,
                                            final boolean flattenAttributes) {
            return metric.getExponentialHistogram()
                .getDataPointsList()
                .stream()
                .filter(dp -> {
                    if (calculateExponentialHistogramBuckets &&
                            exponentialHistogramMaxAllowedScale < Math.abs(dp.getScale())){
                        LOG.error("Exponential histogram can not be processed since its scale of {} is bigger than the configured max of {}.", dp.getScale(), exponentialHistogramMaxAllowedScale);
                        return false;
                    } else {
                        return true;
                    }
                })
                .map(dp -> {
                    JacksonExponentialHistogram.Builder builder = JacksonExponentialHistogram.builder()
                            .withUnit(metric.getUnit())
                            .withName(metric.getName())
                            .withDescription(metric.getDescription())
                            .withStartTime(convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                            .withTime(convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                            .withServiceName(serviceName)
                            .withSum(dp.getSum())
                            .withCount(dp.getCount())
                            .withZeroCount(dp.getZeroCount())
                            .withScale(dp.getScale())
                            .withPositive(dp.getPositive().getBucketCountsList())
                            .withPositiveOffset(dp.getPositive().getOffset())
                            .withNegative(dp.getNegative().getBucketCountsList())
                            .withNegativeOffset(dp.getNegative().getOffset())
                            .withAggregationTemporality(metric.getHistogram().getAggregationTemporality().toString())
                            .withAttributes(OTelProtoOpensearchCodec.mergeAllAttributes(
                                    Arrays.asList(
                                            OTelProtoOpensearchCodec.unpackKeyValueListMetric(dp.getAttributesList()),
                                            resourceAttributes,
                                            ils
                                    )
                            ))
                            .withSchemaUrl(schemaUrl)
                            .withExemplars(OTelProtoOpensearchCodec.convertExemplars(dp.getExemplarsList()))
                            .withTimeReceived(timeReceived)
                            .withFlags(dp.getFlags());

                    if (calculateExponentialHistogramBuckets) {
                        builder.withPositiveBuckets(OTelProtoOpensearchCodec.createExponentialBuckets(dp.getPositive(), dp.getScale()));
                        builder.withNegativeBuckets(OTelProtoOpensearchCodec.createExponentialBuckets(dp.getNegative(), dp.getScale()));
                    }

                    return builder.build(flattenAttributes);
                })
                .map(Record::new)
                .collect(Collectors.toList());
        }

    }

    public static class OTelProtoEncoder implements OTelProtoCodec.OTelProtoEncoder {
        protected static final String SERVICE_NAME_ATTRIBUTE = "service@name";
        protected static final String RESOURCE_ATTRIBUTES_PREFIX = RESOURCE_ATTRIBUTES + DOT;
        protected static final String SPAN_ATTRIBUTES_PREFIX = SPAN_ATTRIBUTES + DOT;

        public ResourceSpans convertToResourceSpans(final Span span) throws UnsupportedEncodingException, DecoderException {
            final ResourceSpans.Builder rsBuilder = ResourceSpans.newBuilder();
            final Map<String, Object> allAttributes = span.getAttributes();
            final Resource resource = constructResource(span.getServiceName(), allAttributes);
            rsBuilder.setResource(resource);
            final ScopeSpans.Builder scopeSpansBuilder = ScopeSpans.newBuilder();
            final InstrumentationScope instrumentationScope = constructInstrumentationScope(allAttributes);
            scopeSpansBuilder.setScope(instrumentationScope);
            final io.opentelemetry.proto.trace.v1.Span otelProtoSpan = constructSpan(span, allAttributes);
            scopeSpansBuilder.addSpans(otelProtoSpan);
            rsBuilder.addScopeSpans(scopeSpansBuilder);
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

        protected InstrumentationScope constructInstrumentationScope(final Map<String, Object> attributes) throws UnsupportedEncodingException, DecoderException {
            final InstrumentationScope.Builder builder = InstrumentationScope.newBuilder();
            final List<KeyValue> attributeKeyValueList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                if (entry.getKey().equals(INSTRUMENTATION_SCOPE_NAME)) {
                    builder.setName((String)entry.getValue());
                } else if (entry.getKey().equals(INSTRUMENTATION_SCOPE_VERSION)) {
                    builder.setVersion((String)entry.getValue());
                } else if (entry.getKey().startsWith(INSTRUMENTATION_SCOPE_ATTRIBUTES)) {
                    KeyValue.Builder setValue = KeyValue.newBuilder().setKey(entry.getKey().substring(INSTRUMENTATION_SCOPE_ATTRIBUTES.length()+1)).setValue(objectToAnyValue(entry.getValue()));
                    attributeKeyValueList.add(setValue.build());
                }
            }
            builder.addAllAttributes(attributeKeyValueList);
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
            builder.setTimeUnixNano(convertISO8601ToNanos(spanEvent.getTime()));
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

        protected io.opentelemetry.proto.trace.v1.Span constructSpan(final Span span, final Map<String, Object> allAttributes)
                throws DecoderException, UnsupportedEncodingException {
            io.opentelemetry.proto.trace.v1.Span.Builder builder = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                    .setSpanId(ByteString.copyFrom(Hex.decodeHex(span.getSpanId())))
                    .setParentSpanId(ByteString.copyFrom(Hex.decodeHex(span.getParentSpanId())))
                    .setTraceId(ByteString.copyFrom(Hex.decodeHex(span.getTraceId())))
                    .setTraceState(span.getTraceState())
                    .setName(span.getName())
                    .setKind(io.opentelemetry.proto.trace.v1.Span.SpanKind.valueOf(span.getKind()))
                    .setStartTimeUnixNano(convertISO8601ToNanos(span.getStartTime()))
                    .setEndTimeUnixNano(convertISO8601ToNanos(span.getEndTime()))
                    .setDroppedAttributesCount(span.getDroppedAttributesCount())
                    .setDroppedEventsCount(span.getDroppedEventsCount())
                    .setDroppedLinksCount(span.getDroppedLinksCount());
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

    /**
     * Converts an {@link AnyValue} into its appropriate data type
     *
     * @param value The value to convert
     * @return the converted value as object
     */
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
                            .map(OTelProtoOpensearchCodec::convertAnyValue)
                            .collect(Collectors.toList()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            case KVLIST_VALUE:
                try {
                    return OBJECT_MAPPER.writeValueAsString(value.getKvlistValue().getValuesList().stream()
                            .collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue()))));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new RuntimeException(String.format("Can not convert AnyValue of type %s", value.getValueCase()));
        }
    }

    /**
     * Converts the keys of all attributes in the {@link NumberDataPoint}.
     * Also, casts the underlying data into its actual type
     *
     * @param numberDataPoint The point to process
     * @return A Map containing all attributes of `numberDataPoint` with keys converted into an OS-friendly format
     */
    public static Map<String, Object> convertKeysOfDataPointAttributes(final NumberDataPoint numberDataPoint) {
        return numberDataPoint.getAttributesList().stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_METRIC_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Unpacks the List of {@link KeyValue} object into a Map. Used for metrics
     * <p>
     * Converts the keys into an os friendly format and casts the underlying data into its actual type?
     *
     * @param attributesList The list of {@link KeyValue} objects to process
     * @return A Map containing unpacked {@link KeyValue} data
     */
    public static Map<String, Object> unpackKeyValueListMetric(List<KeyValue> attributesList) {
        return attributesList.stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_METRIC_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    public static Map<String, Object> unpackKeyValueList(List<KeyValue> attributesList) {
        return attributesList.stream()
                .collect(Collectors.toMap(i -> DOT+(i.getKey()).replace(DOT, AT), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Unpacks the List of {@link KeyValue} object into a Map. Used for logs.
     * <p>
     * Converts the keys into an os friendly format and casts the underlying data into its actual type?
     *
     * @param attributesList The list of {@link KeyValue} objects to process
     * @return A Map containing unpacked {@link KeyValue} data
     */
    public static Map<String, Object> unpackKeyValueListLog(List<KeyValue> attributesList) {
        return attributesList.stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_LOG_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }


    /**
     * Unpacks the List of {@link KeyValue} object into a Map.
     * <p>
     * Converts the keys into an os friendly format and casts the underlying data into its actual type?
     *
     * @param attributesList The list of {@link KeyValue} objects to process
     * @return A Map containing unpacked {@link KeyValue} data
     */
    public static Map<String, Object> unpackExemplarValueList(List<KeyValue> attributesList) {
        return attributesList.stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_EXEMPLAR_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }


    /**
     * Extracts a value from the passed {@link NumberDataPoint} into a double representation
     *
     * @param ndp The {@link NumberDataPoint} which's data should be turned into a double value
     * @return A double representing the numerical value of the passed {@link NumberDataPoint}.
     * Null if the numerical data point is not present
     */
    public static Double getValueAsDouble(final NumberDataPoint ndp) {
        NumberDataPoint.ValueCase ndpCase = ndp.getValueCase();
        if (NumberDataPoint.ValueCase.AS_DOUBLE == ndpCase) {
            return ndp.getAsDouble();
        } else if (NumberDataPoint.ValueCase.AS_INT == ndpCase) {
            return (double) ndp.getAsInt();
        } else {
            return null;
        }
    }

    /**
     * Extracts a value from the passed {@link io.opentelemetry.proto.metrics.v1.Exemplar} into a double representation
     *
     * @param exemplar The {@link io.opentelemetry.proto.metrics.v1.Exemplar} which's data should be turned into a double value
     * @return A double representing the numerical value of the passed {@link io.opentelemetry.proto.metrics.v1.Exemplar}.
     * Null if the numerical data point is not present
     */
    public static Double getExemplarValueAsDouble(final io.opentelemetry.proto.metrics.v1.Exemplar exemplar) {
        io.opentelemetry.proto.metrics.v1.Exemplar.ValueCase valueCase = exemplar.getValueCase();
        if (io.opentelemetry.proto.metrics.v1.Exemplar.ValueCase.AS_DOUBLE == valueCase) {
            return exemplar.getAsDouble();
        } else if (io.opentelemetry.proto.metrics.v1.Exemplar.ValueCase.AS_INT == valueCase) {
            return (double) exemplar.getAsInt();
        } else {
            return null;
        }
    }

    public static Map<String, Object> getResourceAttributes(final Resource resource) {
        return resource.getAttributesList().stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Extracts the name and version of the used instrumentation scope used
     *
     * @param  instrumentationScope the instrumentation scope
     * @return A map, containing information about the instrumentation scope
     */
    public static Map<String, Object> getInstrumentationScopeAttributes(final InstrumentationScope instrumentationScope) {
        final Map<String, Object> instrumentationScopeAttr = new HashMap<>();
        if (!instrumentationScope.getName().isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_NAME, instrumentationScope.getName());
        }
        if (!instrumentationScope.getVersion().isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_VERSION, instrumentationScope.getVersion());
        }
        if (!instrumentationScope.getAttributesList().isEmpty()) {
            for (Map.Entry<String, Object> entry: OTelProtoOpensearchCodec.unpackKeyValueList(instrumentationScope.getAttributesList()).entrySet()) {
                instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_ATTRIBUTES+entry.getKey(), entry.getValue());
            }
        }
        return instrumentationScopeAttr;
    }


    public static Optional<String> getServiceName(final Resource resource) {
        return resource.getAttributesList().stream()
                .filter(keyValue -> keyValue.getKey().equals(SERVICE_NAME) && !keyValue.getValue().getStringValue().isEmpty())
                .findFirst()
                .map(i -> i.getValue().getStringValue());
    }


    public static Map<String, Object> mergeAllAttributes(final Collection<Map<String, Object>> attributes) {
        return attributes.stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    public static List<Quantile> getQuantileValues(List<SummaryDataPoint.ValueAtQuantile> quantileValues) {
        return quantileValues.stream()
                .map(q -> new DefaultQuantile(q.getQuantile(), q.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Create the buckets, see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto">
     *     the OTel metrics proto spec</a>
     * <p>
     * The boundaries for bucket at index i are:
     * </p>
     * <pre>{@code
     * (-infinity, explicit_bounds[i]) for i == 0
     * (explicit_bounds[i-1], +infinity) for i == size(explicit_bounds)
     * (explicit_bounds[i-1], explicit_bounds[i]) for 0 < i < size(explicit_bounds)
     * }</pre>
     * <br>
     * <br>
     * <b>NOTE:</b> here we map infinity as +/- FLOAT.MAX_VALUE since JSON rfc4627 only supports finite numbers and
     * OpenSearch maps double values to floats as per default.
     *
     * @param bucketCountsList   a list with the bucket counts
     * @param explicitBoundsList a list with the bounds
     * @return buckets list
     */
    public static List<Bucket> createBuckets(List<Long> bucketCountsList, List<Double> explicitBoundsList) {
        List<Bucket> buckets = new ArrayList<>();
        if (bucketCountsList.isEmpty()) {
            return buckets;
        }
        if (bucketCountsList.size() - 1 != explicitBoundsList.size()) {
            LOG.error("bucket count list not equals to bounds list {} {}", bucketCountsList.size(), explicitBoundsList.size());
            throw new IllegalArgumentException("OpenTelemetry protocol mandates that the number of elements in bucket_counts array must be by one greater than\n" +
                    "  // the number of elements in explicit_bounds array.");
        } else {
            if (bucketCountsList.size() == 1) {
                double min = OTEL_NEGATIVE_INFINITY;
                double max = OTEL_POSITIVE_INFINITY;
                Long bucketCount = bucketCountsList.get(0);
                buckets.add(new DefaultBucket(min, max, bucketCount));
            } else {
                for (int i = 0; i < bucketCountsList.size(); i++) {
                    if (i == 0) {
                        double min = OTEL_NEGATIVE_INFINITY;
                        double max = explicitBoundsList.get(i);
                        Long bucketCount = bucketCountsList.get(i);
                        buckets.add(new DefaultBucket(min, max, bucketCount));
                    } else if (i == bucketCountsList.size() - 1) {
                        double min = explicitBoundsList.get(i - 1);
                        double max = OTEL_POSITIVE_INFINITY;
                        Long bucketCount = bucketCountsList.get(i);
                        buckets.add(new DefaultBucket(min, max, bucketCount));
                    } else {
                        double min = explicitBoundsList.get(i - 1);
                        double max = explicitBoundsList.get(i);
                        Long bucketCount = bucketCountsList.get(i);
                        buckets.add(new DefaultBucket(min, max, bucketCount));
                    }
                }
            }
        }
        return buckets;
    }

    /**
     * Converts a List of {@link io.opentelemetry.proto.metrics.v1.Exemplar} values to {@link DefaultExemplar}, the
     * internal representation for Data Prepper
     *
     * @param exemplarsList the List of Exemplars
     * @return a mapped list of DefaultExemplars
     */
    public static List<Exemplar> convertExemplars(List<io.opentelemetry.proto.metrics.v1.Exemplar> exemplarsList) {
        return exemplarsList.stream().map(exemplar ->
                        new DefaultExemplar(convertUnixNanosToISO8601(exemplar.getTimeUnixNano()),
                                getExemplarValueAsDouble(exemplar),
                                convertByteStringToString(exemplar.getSpanId()),
                                convertByteStringToString(exemplar.getTraceId()),
                                unpackExemplarValueList(exemplar.getFilteredAttributesList())))
                .collect(Collectors.toList());
    }

    /**
     * Pre-Calculates all possible bucket bounds for this scale.
     * Uses the entire range of Double values.
     *
     * @param key a tuple with scale and offset sign for bounds calculation
     * @return an array with all possible bucket bounds with the scale
     */
    static double[] calculateBoundsForScale(BoundsKey key) {

        //base = 2**(2**(-scale))
        double base = Math.pow(2., Math.pow(2., -key.getScale()));

        //calculate all possible buckets in the Double range and consider the offset sign
        int maxIndex = Math.toIntExact(Math.round(Math.log(Double.MAX_VALUE) / Math.log(base)));
        double[] boundaries = new double[maxIndex + 1];
        for (int i = 0; i <= maxIndex ; i++) {
             boundaries[i] = Math.pow(base, key.getSign() == BoundsKey.Sign.POSITIVE ? i : -i);
        }
        return boundaries;
    }

    /**
     * Maps a List of {@link io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.Buckets} to an
     * internal representation for Data Prepper.
     * See <a href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/datamodel.md#exponential-buckets">data model</a>
     *
     * @param buckets the list of buckets
     * @param scale the scale of the exponential histogram
     * @return a mapped list of Buckets
     */
    public static List<Bucket> createExponentialBuckets(ExponentialHistogramDataPoint.Buckets buckets, int scale) {
        int offset = buckets.getOffset();
        BoundsKey key = new BoundsKey(scale, offset < 0 ? BoundsKey.Sign.NEGATIVE : BoundsKey.Sign.POSITIVE);
        double[] bucketBounds = EXPONENTIAL_BUCKET_BOUNDS.computeIfAbsent(key, boundsKey -> calculateBoundsForScale(key));

        List<Bucket> mappedBuckets = new ArrayList<>();
        List<Long> bucketsList = buckets.getBucketCountsList();

        int boundOffset = Math.abs(offset); // Offset can be negative, but we always want positive offsets for array access
        if (bucketsList.size() + boundOffset >= bucketBounds.length) {
            LOG.error("Max offset is out of range for Double data type, ignoring buckets");
        } else {
            for (int i = 0; i < bucketsList.size(); i++) {
                Long value = bucketsList.get(i);
                double lowerBound = bucketBounds[boundOffset + i];
                double upperBound = bucketBounds[boundOffset + i + 1];
                mappedBuckets.add(new DefaultBucket(lowerBound, upperBound, value));
            }
        }
        return mappedBuckets;
    }

}
