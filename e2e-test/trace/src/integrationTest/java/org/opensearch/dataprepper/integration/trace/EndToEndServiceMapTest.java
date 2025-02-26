/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration.trace;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.retry.RetryRule;
import com.linecorp.armeria.client.retry.RetryingClient;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

public class EndToEndServiceMapTest {
    private static final String TEST_TRACEID_1 = "ABC";
    private static final String TEST_TRACEID_2 = "CBA";
    private static final int DATA_PREPPER_PORT_1 = 21890;
    private static final int DATA_PREPPER_PORT_2 = 21891;
    private static final List<EndToEndTestSpan> TEST_TRACE_1_BATCH_1 = Arrays.asList(
            EndToEndTestSpan.TRACE_1_ROOT_SPAN, EndToEndTestSpan.TRACE_1_SPAN_2, EndToEndTestSpan.TRACE_1_SPAN_5,
            EndToEndTestSpan.TRACE_1_SPAN_6, EndToEndTestSpan.TRACE_1_SPAN_7, EndToEndTestSpan.TRACE_1_SPAN_10);
    private static final List<EndToEndTestSpan> TEST_TRACE_1_BATCH_2 = Arrays.asList(
            EndToEndTestSpan.TRACE_1_SPAN_3, EndToEndTestSpan.TRACE_1_SPAN_4, EndToEndTestSpan.TRACE_1_SPAN_8,
            EndToEndTestSpan.TRACE_1_SPAN_9, EndToEndTestSpan.TRACE_1_SPAN_11);
    private static final List<EndToEndTestSpan> TEST_TRACE_2_BATCH_1 = Arrays.asList(EndToEndTestSpan.TRACE_2_ROOT_SPAN,
            EndToEndTestSpan.TRACE_2_SPAN_2, EndToEndTestSpan.TRACE_2_SPAN_4, EndToEndTestSpan.TRACE_2_SPAN_5);
    private static final List<EndToEndTestSpan> TEST_TRACE_2_BATCH_2 = Collections.singletonList(EndToEndTestSpan.TRACE_2_SPAN_3);
    private static final String SERVICE_MAP_INDEX_NAME = "otel-v1-apm-service-map";

    @Test
    public void testPipelineEndToEnd() {
        // Send test trace group 1
        final ExportTraceServiceRequest exportTraceServiceRequest11 = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_TRACEID_1, TEST_TRACE_1_BATCH_1)
        );
        final ExportTraceServiceRequest exportTraceServiceRequest12 = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_TRACEID_1, TEST_TRACE_1_BATCH_2)
        );

        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_1, exportTraceServiceRequest11);
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_2, exportTraceServiceRequest12);

        //Verify data in OpenSearch backend
        final List<EndToEndTestSpan> testDataSet1 = Stream.of(TEST_TRACE_1_BATCH_1, TEST_TRACE_1_BATCH_2)
                .flatMap(Collection::stream).collect(Collectors.toList());
        final List<Map<String, Object>> possibleEdges = getPossibleEdges(testDataSet1);
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        final AwsCredentialsSupplier awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        final RestHighLevelClient restHighLevelClient = builder.build().createClient(awsCredentialsSupplier);

        // Wait for service map processor by 2 * window_duration
        await().atMost(45, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    final List<Map<String, Object>> foundSources = getSourcesFromIndex(restHighLevelClient, SERVICE_MAP_INDEX_NAME);
                    foundSources.forEach(source -> source.remove("hashId"));
                    Assert.assertEquals(8, foundSources.size());
                    Assert.assertTrue(foundSources.containsAll(possibleEdges) && possibleEdges.containsAll(foundSources));
                }
        );

        // Resend the same batch of spans (No new edges should be created)
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_1, exportTraceServiceRequest11);
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_2, exportTraceServiceRequest12);
        // Send test trace group 2
        final ExportTraceServiceRequest exportTraceServiceRequest21 = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_TRACEID_2, TEST_TRACE_2_BATCH_1)
        );
        final ExportTraceServiceRequest exportTraceServiceRequest22 = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_TRACEID_2, TEST_TRACE_2_BATCH_2)
        );

        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_1, exportTraceServiceRequest21);
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_2, exportTraceServiceRequest22);

        final List<EndToEndTestSpan> testDataSet2 = Stream.of(TEST_TRACE_2_BATCH_1, TEST_TRACE_2_BATCH_2)
                .flatMap(Collection::stream).collect(Collectors.toList());
        possibleEdges.addAll(getPossibleEdges(testDataSet2));

        await().atMost(60, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    final List<Map<String, Object>> foundSources = getSourcesFromIndex(restHighLevelClient, SERVICE_MAP_INDEX_NAME);
                    foundSources.forEach(source -> source.remove("hashId"));
                    Assert.assertEquals(12, foundSources.size());
                    Assert.assertTrue(foundSources.containsAll(possibleEdges) && possibleEdges.containsAll(foundSources));
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

    private List<Map<String, Object>> getSourcesFromIndex(final RestHighLevelClient restHighLevelClient, final String index) throws IOException {
        refreshIndices(restHighLevelClient);
        final SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(
                SearchSourceBuilder.searchSource().size(100)
        );
        final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        return getSourcesFromSearchHits(searchResponse.getHits());
    }

    private List<Map<String, Object>> getSourcesFromSearchHits(final SearchHits searchHits) {
        final List<Map<String, Object>> sources = new ArrayList<>();
        searchHits.forEach(hit -> sources.add(hit.getSourceAsMap()));
        return sources;
    }


    public static ResourceSpans getResourceSpans(final String serviceName, final String spanName, final byte[]
            spanId, final byte[] parentId, final byte[] traceId, final Span.SpanKind spanKind) {
        final ByteString parentSpanId = parentId != null ? ByteString.copyFrom(parentId) : ByteString.EMPTY;
        return ResourceSpans.newBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("service.name")
                                        .setValue(AnyValue.newBuilder().setStringValue(serviceName).build()).build())
                                .build()
                )
                .addScopeSpans(
                        0,
                        ScopeSpans.newBuilder()
                                .addSpans(
                                        Span.newBuilder()
                                                .setName(spanName)
                                                .setKind(spanKind)
                                                .setSpanId(ByteString.copyFrom(spanId))
                                                .setParentSpanId(parentSpanId)
                                                .setTraceId(ByteString.copyFrom(traceId))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    public static ExportTraceServiceRequest getExportTraceServiceRequest(List<ResourceSpans> spans) {
        return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(spans)
                .build();
    }

    private List<ResourceSpans> getResourceSpansBatch(final String traceId, final List<EndToEndTestSpan> data) {
        final ArrayList<ResourceSpans> spansList = new ArrayList<>();
        for(int i=0; i < data.size(); i++) {
            final EndToEndTestSpan currData = data.get(i);
            final String parentId = currData.parentId;
            final String spanId = currData.spanId;
            final String serviceName = currData.serviceName;
            final String spanName = currData.name;
            final Span.SpanKind spanKind = currData.spanKind;
            final ResourceSpans rs = getResourceSpans(
                    serviceName,
                    spanName,
                    spanId.getBytes(),
                    parentId != null ? parentId.getBytes() : null,
                    traceId.getBytes(),
                    spanKind
            );
            spansList.add(rs);
        }
        return spansList;
    }

    private List<Map<String, Object>> getPossibleEdges(final List<EndToEndTestSpan> data) {
        final Map<String, EndToEndTestSpan> spanIdToServiceMapTestData = data.stream()
                .collect(Collectors.toMap(endToEndTestSpan -> endToEndTestSpan.spanId, endToEndTestSpan -> endToEndTestSpan));
        final List<Map<String, Object>> possibleEdges = new ArrayList<>();
        for (final EndToEndTestSpan currData : data) {
            final String parentId = currData.parentId;
            if (parentId != null) {
                final EndToEndTestSpan parentData = spanIdToServiceMapTestData.get(parentId);
                if (parentData != null && !parentData.serviceName.equals(currData.serviceName)) {
                    String rootSpanName = getRootSpanName(parentId, spanIdToServiceMapTestData);
                    possibleEdges.addAll(getEdgeMaps(rootSpanName, currData, parentData));
                }
            }
        }

        return possibleEdges;
    }

    private String getRootSpanName(String spanId, final Map<String, EndToEndTestSpan> spanIdToServiceMapTestData) {
        EndToEndTestSpan rootEndToEndTestSpan = spanIdToServiceMapTestData.get(spanId);
        while (rootEndToEndTestSpan.parentId != null) {
            rootEndToEndTestSpan = spanIdToServiceMapTestData.get(rootEndToEndTestSpan.parentId);
        }
        return rootEndToEndTestSpan.name;
    }

    private List<Map<String, Object>> getEdgeMaps(
            final String rootSpanName, final EndToEndTestSpan currData, final EndToEndTestSpan parentData) {
        final List<Map<String, Object>> edges = new ArrayList<>();

        Map<String, Object> destination = new HashMap<>();
        destination.put("resource", currData.name);
        destination.put("domain", currData.serviceName);
        Map<String, Object> edge = new HashMap<>();
        edge.put("serviceName", parentData.serviceName);
        edge.put("kind", parentData.spanKind.name());
        edge.put("traceGroupName", rootSpanName);
        edge.put("destination", destination);
        edge.put("target", null);
        edges.add(edge);

        Map<String, Object> target = new HashMap<>(destination);
        edge = new HashMap<>();
        edge.put("serviceName", currData.serviceName);
        edge.put("kind", currData.spanKind.name());
        edge.put("traceGroupName", rootSpanName);
        edge.put("destination", null);
        edge.put("target", target);
        edges.add(edge);

        return edges;
    }
}
