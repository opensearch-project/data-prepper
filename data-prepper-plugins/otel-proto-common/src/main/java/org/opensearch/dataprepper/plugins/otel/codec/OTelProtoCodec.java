/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.commons.codec.DecoderException;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OTelProtoCodec is for encoding/decoding between {@link org.opensearch.dataprepper.model.trace} and {@link io.opentelemetry.proto}.
 */
public interface OTelProtoCodec {
    public static final int DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE = 10;

    public interface OTelProtoDecoder {
        public static final String SERVICE_NAME = "service.name";
        public static final String SERVICE_NAME_KEY = "service_name";

        List<Span> parseExportTraceServiceRequest(final ExportTraceServiceRequest exportTraceServiceRequest, final Instant timeReceived);

        Map<String, ExportTraceServiceRequest> splitExportTraceServiceRequestByTraceId(final ExportTraceServiceRequest exportTraceServiceRequest);

        static Optional<String> getServiceName(final Resource resource) {
            return resource.getAttributesList().stream().filter(
                    keyValue -> keyValue.getKey().equals(SERVICE_NAME)
                            && !keyValue.getValue().getStringValue().isEmpty()
            ).findFirst().map(i -> i.getValue().getStringValue());
        }

        List<OpenTelemetryLog> parseExportLogsServiceRequest(final ExportLogsServiceRequest exportLogsServiceRequest, final Instant timeReceived);
        default Map<String, ExportMetricsServiceRequest> splitExportMetricsServiceRequestByKeys(final ExportMetricsServiceRequest request, final Set<String> keys) {
            Map<String, ExportMetricsServiceRequest.Builder> builderMap = new HashMap<>();

            request.getResourceMetricsList().forEach(resourceMetrics ->
                splitResourceMetricsByKeys(resourceMetrics, keys).forEach((key, value) ->
                    builderMap.computeIfAbsent(key, k -> ExportMetricsServiceRequest.newBuilder())
                             .addResourceMetrics(value)
                )
            );

            return builderMap.entrySet().stream()
                    .collect(HashMap::new,
                            (map, entry) -> map.put(entry.getKey(), entry.getValue().build()),
                            HashMap::putAll);
        }

        private Map<String, ResourceMetrics> splitResourceMetricsByKeys(final ResourceMetrics resourceMetrics, final Set<String> keys) {
            final Resource resource = resourceMetrics.getResource();
            String entryKeyPrefix = "";
            if (keys.contains(SERVICE_NAME_KEY)) {
                entryKeyPrefix = getServiceName(resource).orElse("");
                entryKeyPrefix += ":";
            }
            final boolean hasResource = resourceMetrics.hasResource();
            Map<String, ResourceMetrics> result = new HashMap<>();
            Map<String, ResourceMetrics.Builder> resultBuilderMap = new HashMap<>();

            if (!resourceMetrics.getScopeMetricsList().isEmpty()) {
                for (Map.Entry<String, List<ScopeMetrics>> entry: splitScopeMetricsByKeys(resourceMetrics.getScopeMetricsList(), entryKeyPrefix).entrySet()) {
                    ResourceMetrics.Builder resourceMetricsBuilder = ResourceMetrics.newBuilder().addAllScopeMetrics(entry.getValue());
                    if (hasResource) {
                        resourceMetricsBuilder.setResource(resource);
                    }
                    resultBuilderMap.put(entry.getKey(), resourceMetricsBuilder);
                }
            }

            for (Map.Entry<String, ResourceMetrics.Builder> entry: resultBuilderMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue().build());
            }
            return result;
        }

        private Map<String, List<ScopeMetrics>> splitScopeMetricsByKeys(final List<ScopeMetrics> scopeMetricsList, final String prefix) {
            Map<String, List<ScopeMetrics>> result = new HashMap<>();
            for (ScopeMetrics sm: scopeMetricsList) {
                final boolean hasScope = sm.hasScope();
                final io.opentelemetry.proto.common.v1.InstrumentationScope scope = sm.getScope();
                for (Map.Entry<String, List<io.opentelemetry.proto.metrics.v1.Metric>> entry: splitMetricsByKeys(sm.getMetricsList(), prefix).entrySet()) {
                    ScopeMetrics.Builder scopeMetricsBuilder = ScopeMetrics.newBuilder().addAllMetrics(entry.getValue());
                    if (hasScope) {
                        scopeMetricsBuilder.setScope(scope);
                    }
                    String key = entry.getKey();
                    if (!result.containsKey(key)) {
                        result.put(key, new ArrayList<>());
                    }
                    result.get(key).add(scopeMetricsBuilder.build());
                }
            }
            return result;
        }

        private Map<String, List<io.opentelemetry.proto.metrics.v1.Metric>> splitMetricsByKeys(final List<io.opentelemetry.proto.metrics.v1.Metric> metrics, final String prefix) {
            Map<String, List<io.opentelemetry.proto.metrics.v1.Metric>> result = new HashMap<>();
            for (io.opentelemetry.proto.metrics.v1.Metric metric: metrics) {
                String key = prefix+metric.getName();
                List<io.opentelemetry.proto.metrics.v1.Metric> metricList;
                if (result.containsKey(key)) {
                    metricList = result.get(key);
                } else {
                    metricList = new ArrayList<>();
                    result.put(key, metricList);
                }
                metricList.add(metric);
            }
            return result;
        }

        default Collection<Record<? extends Metric>> parseExportMetricsServiceRequest(
                            final ExportMetricsServiceRequest request,
                            final Instant timeReceived) {
            AtomicInteger droppedCounter = new AtomicInteger(0);
            return parseExportMetricsServiceRequest(request, droppedCounter,
                    DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE, timeReceived, true, true, true);
        }
        Collection<Record<? extends Metric>> parseExportMetricsServiceRequest(
                            final ExportMetricsServiceRequest request,
                            AtomicInteger droppedCounter,
                            final Integer exponentialHistogramMaxAllowedScale,
                            final Instant timeReceived,
                            final boolean calculateHistogramBuckets,
                            final boolean calculateExponentialHistogramBuckets,
                            final boolean flattenAttributes);
    }

    public interface OTelProtoEncoder {
        ResourceSpans convertToResourceSpans(final Span span) throws UnsupportedEncodingException, DecoderException;
        
        io.opentelemetry.proto.metrics.v1.ResourceMetrics convertToResourceMetrics(final Metric metric) throws UnsupportedEncodingException, DecoderException;
        
        io.opentelemetry.proto.logs.v1.ResourceLogs convertToResourceLogs(final org.opensearch.dataprepper.model.log.Log log) throws UnsupportedEncodingException, DecoderException;
    }

}
