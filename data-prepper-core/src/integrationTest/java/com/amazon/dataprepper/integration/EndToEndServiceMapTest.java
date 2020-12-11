package com.amazon.dataprepper.integration;


import com.google.protobuf.ByteString;
import com.amazon.dataprepper.plugins.sink.elasticsearch.ConnectionConfiguration;
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

import com.linecorp.armeria.client.Clients;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Assert;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class EndToEndServiceMapTest {
    private static final String TEST_TRACEID_1 = "ABC";
    private static final String TEST_TRACEID_2 = "CBA";
    private static final int DATA_PREPPER_PORT_1 = 21890;
    private static final int DATA_PREPPER_PORT_2 = 21891;
    private static final List<ServiceMapTestData> testDataSet11 = Arrays.asList(
            ServiceMapTestData.DATA_100, ServiceMapTestData.DATA_200, ServiceMapTestData.DATA_500, ServiceMapTestData.DATA_600,
            ServiceMapTestData.DATA_700, ServiceMapTestData.DATA_1000);
    private static final List<ServiceMapTestData> testDataSet12 = Arrays.asList(
            ServiceMapTestData.DATA_300, ServiceMapTestData.DATA_400, ServiceMapTestData.DATA_800,
            ServiceMapTestData.DATA_900, ServiceMapTestData.DATA_1100);
    private static final List<ServiceMapTestData> testDataSet21 = Arrays.asList(
            ServiceMapTestData.DATA_101, ServiceMapTestData.DATA_201, ServiceMapTestData.DATA_401, ServiceMapTestData.DATA_501);
    private static final List<ServiceMapTestData> testDataSet22 = Collections.singletonList(ServiceMapTestData.DATA_301);
    private static final String SERVICE_MAP_INDEX_NAME = "otel-v1-apm-service-map";

    @Test
    public void testPipelineEndToEnd() throws IOException, InterruptedException {
        // Send test trace group 1
        final ExportTraceServiceRequest exportTraceServiceRequest11 = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_TRACEID_1, testDataSet11)
        );
        final ExportTraceServiceRequest exportTraceServiceRequest12 = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_TRACEID_1, testDataSet12)
        );

        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_1, exportTraceServiceRequest11);
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_2, exportTraceServiceRequest12);

        //Verify data in elasticsearch sink
        final List<ServiceMapTestData> testDataSet1 = Stream.of(testDataSet11, testDataSet12)
                .flatMap(Collection::stream).collect(Collectors.toList());
        final List<Map<String, Object>> possibleEdges = getPossibleEdges(TEST_TRACEID_1, testDataSet1);
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        final RestHighLevelClient restHighLevelClient = builder.build().createClient();

        // Wait for service map processor by 2 * window_duration
        Thread.sleep(6000);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
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
                getResourceSpansBatch(TEST_TRACEID_2, testDataSet21)
        );
        final ExportTraceServiceRequest exportTraceServiceRequest22 = getExportTraceServiceRequest(
                getResourceSpansBatch(TEST_TRACEID_2, testDataSet22)
        );

        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_1, exportTraceServiceRequest21);
        sendExportTraceServiceRequestToSource(DATA_PREPPER_PORT_2, exportTraceServiceRequest22);

        final List<ServiceMapTestData> testDataSet2 = Stream.of(testDataSet21, testDataSet22)
                .flatMap(Collection::stream).collect(Collectors.toList());
        possibleEdges.addAll(getPossibleEdges(TEST_TRACEID_2, testDataSet2));
        // Wait for service map processor by 2 * window_duration
        Thread.sleep(6000);
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(
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
        Clients.newClient(String.format("gproto+http://127.0.0.1:%d/", port),
                TraceServiceGrpc.TraceServiceBlockingStub.class).export(request);
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

    private List<ResourceSpans> getResourceSpansBatch(final String traceId, final List<ServiceMapTestData> data) {
        final ArrayList<ResourceSpans> spansList = new ArrayList<>();
        for(int i=0; i < data.size(); i++) {
            final ServiceMapTestData currData = data.get(i);
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

    private List<Map<String, Object>> getPossibleEdges(final String traceId, final List<ServiceMapTestData> data) {
        final Map<String, ServiceMapTestData> spanIdToServiceMapTestData = data.stream()
                .collect(Collectors.toMap(serviceMapTestData -> serviceMapTestData.spanId, serviceMapTestData -> serviceMapTestData));
        final List<Map<String, Object>> possibleEdges = new ArrayList<>();
        for (final ServiceMapTestData currData : data) {
            final String parentId = currData.parentId;
            if (parentId != null) {
                final ServiceMapTestData parentData = spanIdToServiceMapTestData.get(parentId);
                if (parentData != null && !parentData.serviceName.equals(currData.serviceName)) {
                    String rootSpanName = getRootSpanName(parentId, spanIdToServiceMapTestData);
                    Map<String, Object> destination = new HashMap<>();
                    destination.put("resource", currData.name);
                    destination.put("domain", currData.serviceName);
                    Map<String, Object> edge = new HashMap<>();
                    edge.put("serviceName", parentData.serviceName);
                    edge.put("kind", parentData.spanKind.name());
                    edge.put("traceGroupName", rootSpanName);
                    edge.put("destination", destination);
                    edge.put("target", null);
                    possibleEdges.add(edge);

                    Map<String, Object> target = new HashMap<>(destination);
                    edge = new HashMap<>();
                    edge.put("serviceName", currData.serviceName);
                    edge.put("kind", currData.spanKind.name());
                    edge.put("traceGroupName", rootSpanName);
                    edge.put("destination", null);
                    edge.put("target", target);
                    possibleEdges.add(edge);
                }
            }
        }

        return possibleEdges;
    }

    private String getRootSpanName(String spanId, final Map<String, ServiceMapTestData> spanIdToServiceMapTestData) {
        ServiceMapTestData rootServiceMapTestData = spanIdToServiceMapTestData.get(spanId);
        while (rootServiceMapTestData.parentId != null) {
            rootServiceMapTestData = spanIdToServiceMapTestData.get(rootServiceMapTestData.parentId);
        }
        return rootServiceMapTestData.name;
    }
}
