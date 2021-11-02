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

package com.amazon.dataprepper.integration.trace;


import com.amazon.dataprepper.plugins.prepper.oteltracegroup.model.TraceGroup;
import com.amazon.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.retry.RetryRule;
import com.linecorp.armeria.client.retry.RetryingClient;
import com.linecorp.armeria.internal.shaded.bouncycastle.util.encoders.Hex;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class EndToEndRawSpanTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};
    private static final int DATA_PREPPER_PORT_1 = 21890;
    private static final int DATA_PREPPER_PORT_2 = 21891;

    private static final Map<String, TraceGroup> TEST_TRACEID_TO_TRACE_GROUP = new HashMap<String, TraceGroup>() {{
       put(Hex.toHexString(EndToEndTestSpan.TRACE_1_ROOT_SPAN.traceId.getBytes()),
               new TraceGroup(
                       EndToEndTestSpan.TRACE_1_ROOT_SPAN.name,
                       EndToEndTestSpan.TRACE_1_ROOT_SPAN.endTime,
                       EndToEndTestSpan.TRACE_1_ROOT_SPAN.durationInNanos,
                       EndToEndTestSpan.TRACE_1_ROOT_SPAN.statusCode
               ));
       put(Hex.toHexString(EndToEndTestSpan.TRACE_2_ROOT_SPAN.traceId.getBytes()),
               new TraceGroup(
                       EndToEndTestSpan.TRACE_2_ROOT_SPAN.name,
                       EndToEndTestSpan.TRACE_2_ROOT_SPAN.endTime,
                       EndToEndTestSpan.TRACE_2_ROOT_SPAN.durationInNanos,
                       EndToEndTestSpan.TRACE_2_ROOT_SPAN.statusCode
               )
       );
    }};
    private static final List<EndToEndTestSpan> TEST_SPAN_SET_1_WITH_ROOT_SPAN = Arrays.asList(
            EndToEndTestSpan.TRACE_1_ROOT_SPAN, EndToEndTestSpan.TRACE_1_SPAN_2, EndToEndTestSpan.TRACE_1_SPAN_3,
            EndToEndTestSpan.TRACE_1_SPAN_4, EndToEndTestSpan.TRACE_1_SPAN_5, EndToEndTestSpan.TRACE_1_SPAN_6);
    private static final List<EndToEndTestSpan> TEST_SPAN_SET_1_WITHOUT_ROOT_SPAN = Arrays.asList(
            EndToEndTestSpan.TRACE_1_SPAN_7, EndToEndTestSpan.TRACE_1_SPAN_8, EndToEndTestSpan.TRACE_1_SPAN_9,
            EndToEndTestSpan.TRACE_1_SPAN_10, EndToEndTestSpan.TRACE_1_SPAN_11);
    private static final List<EndToEndTestSpan> TEST_SPAN_SET_2_WITH_ROOT_SPAN = Arrays.asList(
            EndToEndTestSpan.TRACE_2_ROOT_SPAN, EndToEndTestSpan.TRACE_2_SPAN_2, EndToEndTestSpan.TRACE_2_SPAN_3);
    private static final List<EndToEndTestSpan> TEST_SPAN_SET_2_WITHOUT_ROOT_SPAN = Arrays.asList(
            EndToEndTestSpan.TRACE_2_SPAN_4, EndToEndTestSpan.TRACE_2_SPAN_5);
    private static final String INDEX_NAME = "otel-v1-apm-span-000001";

    @Test
    public void testPipelineEndToEnd() throws InterruptedException {
        //Send data to otel trace source
        final ExportTraceServiceRequest exportTraceServiceRequestTrace1BatchWithRoot = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_SPAN_SET_1_WITH_ROOT_SPAN)
        );
        final ExportTraceServiceRequest exportTraceServiceRequestTrace1BatchNoRoot = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_SPAN_SET_1_WITHOUT_ROOT_SPAN)
        );
        final ExportTraceServiceRequest exportTraceServiceRequestTrace2BatchWithRoot = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_SPAN_SET_2_WITH_ROOT_SPAN)
        );
        final ExportTraceServiceRequest exportTraceServiceRequestTrace2BatchNoRoot = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_SPAN_SET_2_WITHOUT_ROOT_SPAN)
        );

        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_1, exportTraceServiceRequestTrace1BatchWithRoot);
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_1, exportTraceServiceRequestTrace2BatchNoRoot);
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_2, exportTraceServiceRequestTrace2BatchWithRoot);
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_2, exportTraceServiceRequestTrace1BatchNoRoot);

        //Verify data in OpenSearch backend
        final List<Map<String, Object>> expectedDocuments = getExpectedDocuments(
                exportTraceServiceRequestTrace1BatchWithRoot, exportTraceServiceRequestTrace1BatchNoRoot,
                exportTraceServiceRequestTrace2BatchWithRoot, exportTraceServiceRequestTrace2BatchNoRoot);
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        final RestHighLevelClient restHighLevelClient = builder.build().createClient();
        // Wait for otel-trace-raw-prepper by at least trace_flush_interval
        Thread.sleep(6000);
        // Wait for data to flow through pipeline and be indexed by ES
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices(restHighLevelClient);
                    final SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
                    searchRequest.source(
                            SearchSourceBuilder.searchSource().size(100)
                    );
                    final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> foundSources = getSourcesFromSearchHits(searchResponse.getHits());
                    Assert.assertEquals(expectedDocuments.size(), foundSources.size());
                    /**
                     * Our raw trace prepper add more fields than the actual sent object. These are defaults from the proto.
                     * So assertion is done if all the expected fields exists.
                     *
                     * TODO: Can we do better?
                     *
                     */
                    expectedDocuments.forEach(expectedDoc -> {
                        Assert.assertTrue(foundSources.stream()
                                .filter(i -> i.get("spanId").equals(expectedDoc.get("spanId")))
                                .findFirst().get()
                                .entrySet().containsAll(expectedDoc.entrySet()));
                    });
                }
        );
    }

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }

    private void sendExportTraceServiceRequestToSource(final int port, final ExportTraceServiceRequest request) {
        TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(String.format("gproto+http://127.0.0.1:%d/", port))
                .decorator(RetryingClient.newDecorator(RetryRule.failsafe()))
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
        client.export(request);
    }

    private List<Map<String, Object>> getSourcesFromSearchHits(final SearchHits searchHits) {
        final List<Map<String, Object>> sources = new ArrayList<>();
        searchHits.forEach(hit -> {
            Map<String, Object> source = hit.getSourceAsMap();
            // OpenSearch API identifies Number type by range, need to convert to Long
            if (source.containsKey(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD)) {
                final Long durationInNanos = ((Number) source.get(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD)).longValue();
                source.put(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD, durationInNanos);
            }
            sources.add(source);
        });
        return sources;
    }

    public static ResourceSpans getResourceSpans(final String serviceName, final String spanName, final byte[]
            spanId, final byte[] parentId, final byte[] traceId, final Span.SpanKind spanKind, final String endTime,
                                                 final Long durationInNanos, final Integer statusCode) {
        final ByteString parentSpanId = parentId != null ? ByteString.copyFrom(parentId) : ByteString.EMPTY;
        final long endTimeInNanos = convertTimeStampToNanos(endTime);
        final long startTimeInNanos = endTimeInNanos - durationInNanos;
        return ResourceSpans.newBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("service.name")
                                        .setValue(AnyValue.newBuilder().setStringValue(serviceName).build()).build())
                                .build()
                )
                .addInstrumentationLibrarySpans(
                        0,
                        InstrumentationLibrarySpans.newBuilder()
                                .addSpans(
                                        Span.newBuilder()
                                                .setName(spanName)
                                                .setKind(spanKind)
                                                .setSpanId(ByteString.copyFrom(spanId))
                                                .setParentSpanId(parentSpanId)
                                                .setTraceId(ByteString.copyFrom(traceId))
                                                .setStartTimeUnixNano(startTimeInNanos)
                                                .setEndTimeUnixNano(endTimeInNanos)
                                                .setStatus(Status.newBuilder().setCodeValue(statusCode))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    public static ExportTraceServiceRequest getExportTraceServiceRequest(List<ResourceSpans> spans){
        return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(spans)
                .build();
    }

    private static long convertTimeStampToNanos(String timestamp) {
        Instant instant = Instant.parse(timestamp);
        return ChronoUnit.NANOS.between(Instant.EPOCH, instant);
    }

    private List<ResourceSpans> getResourceSpansBatch(final List<EndToEndTestSpan> testSpanList) {
        final ArrayList<ResourceSpans> spansList = new ArrayList<>();
        for(final EndToEndTestSpan testSpan : testSpanList) {
            final String traceId = testSpan.traceId;
            final String parentId = testSpan.parentId;
            final String spanId = testSpan.spanId;
            final String serviceName = testSpan.serviceName;
            final String spanName = testSpan.name;
            final Span.SpanKind spanKind = testSpan.spanKind;
            final String endTime = testSpan.endTime;
            final Long durationInNanos = testSpan.durationInNanos;
            final Integer statusCode = testSpan.statusCode;
            final ResourceSpans rs = getResourceSpans(
                    serviceName,
                    spanName,
                    spanId.getBytes(),
                    parentId != null ? parentId.getBytes() : null,
                    traceId.getBytes(),
                    spanKind,
                    endTime,
                    durationInNanos,
                    statusCode
            );
            spansList.add(rs);
        }
        return spansList;
    }

    private List<Map<String, Object>> getExpectedDocuments(ExportTraceServiceRequest...exportTraceServiceRequests) {
        final List<Map<String, Object>> expectedDocuments = new ArrayList<>();
        for(int i=0; i<exportTraceServiceRequests.length; i++) {
            exportTraceServiceRequests[i].getResourceSpansList().forEach( resourceSpans -> {
                final String resourceName = getServiceName(resourceSpans);
                resourceSpans.getInstrumentationLibrarySpansList().forEach( instrumentationLibrarySpans -> {
                    instrumentationLibrarySpans.getSpansList().forEach(span -> {
                        expectedDocuments.add(getExpectedEsDocumentSource(span, resourceName));
                    });
                });
            });
        }
        return expectedDocuments;
    }

    private Map<String, Object> getExpectedEsDocumentSource(final Span span, final String serviceName) {
        final Map<String, Object> esDocSource = new HashMap<>();
        final String traceId = Hex.toHexString(span.getTraceId().toByteArray());
        esDocSource.put("traceId", traceId);
        esDocSource.put("spanId", Hex.toHexString(span.getSpanId().toByteArray()));
        esDocSource.put("parentSpanId", Hex.toHexString(span.getParentSpanId().toByteArray()));
        esDocSource.put("name", span.getName());
        esDocSource.put("kind", span.getKind().name());
        esDocSource.put("status.code", span.getStatus().getCodeValue());
        esDocSource.put("serviceName", serviceName);
        final TraceGroup traceGroup = TEST_TRACEID_TO_TRACE_GROUP.get(traceId);
        esDocSource.putAll(OBJECT_MAPPER.convertValue(traceGroup, MAP_TYPE_REFERENCE));

        return esDocSource;
    }

    private String getServiceName(final ResourceSpans resourceSpans) {
        return resourceSpans.getResource().getAttributesList().stream().filter(kv -> kv.getKey().equals("service.name"))
                .findFirst().get().getValue().getStringValue();
    }
}
