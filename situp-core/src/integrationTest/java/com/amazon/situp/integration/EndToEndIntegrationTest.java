package com.amazon.situp.integration;


import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.amazon.situp.plugins.sink.elasticsearch.ConnectionConfiguration;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

public class EndToEndIntegrationTest {

    private static final Random RANDOM = new Random();
    private static final List<Span.SpanKind> SPAN_KINDS = Arrays.asList(
            Span.SpanKind.CLIENT, Span.SpanKind.SERVER, Span.SpanKind.CONSUMER, Span.SpanKind.INTERNAL);
    private List<byte[]> spanIds;
    private byte[] traceId;
    private static final List<String> serviceNames = Arrays.asList("FRONTEND", "BACKEND", "PAYMENT", "DATABASE");
    private static final List<String> traceGroups = Arrays.asList("tg1", "tg2", "tg3", "tg4");
    private static final String RAW_SPAN_INDEX_ALIAS = "otel-v1-apm-span";
    private static final String SERVICE_MAP_INDEX_NAME = "otel-v1-apm-service-map";

    @Test
    public void testPipelineEndToEnd() throws IOException, InterruptedException {
        //Send data to otel trace source
        final ExportTraceServiceRequest exportTraceServiceRequest1 = getExportTraceServiceRequest(
                getRandomResourceSpans(10)
        );

        final ExportTraceServiceRequest exportTraceServiceRequest2 = getExportTraceServiceRequest(
                getRandomResourceSpans(10)
        );

        sendExportTraceServiceRequestToSource(exportTraceServiceRequest1);
        sendExportTraceServiceRequestToSource(exportTraceServiceRequest2);

        //Verify data in elasticsearch sink
        final List<Map<String, Object>> expectedDocuments = getExpectedDocuments(exportTraceServiceRequest1, exportTraceServiceRequest2);
        final List<Map<String, Object>> expectedTargets = IntStream.range(0, serviceNames.size()).mapToObj(i -> {
            final Map<String, Object> target = new HashMap<>();
            target.put("resource", serviceNames.get(i));
            target.put("domain", traceGroups.get(i));
            return target;
        }).collect(Collectors.toList());
        final List<Map<String, Object>> expectedEdges = IntStream.range(1, serviceNames.size() + 1).mapToObj(i -> {
            final Map<String, Object> edge = new HashMap<>();
            edge.put("traceGroupName", "tg1");
            edge.put("serviceName", serviceNames.get((i - 1) % serviceNames.size()));
            edge.put("resource", serviceNames.get(i % serviceNames.size()));
            edge.put("domain", traceGroups.get(i % traceGroups.size()));
            return edge;
        }).collect(Collectors.toList());
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        final RestHighLevelClient restHighLevelClient = builder.build().createClient();
        // Wait for raw span data to flow through pipeline and be indexed by ES
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    final List<Map<String, Object>> foundSources = getSourcesFromIndex(restHighLevelClient, RAW_SPAN_INDEX_ALIAS);
                    Assert.assertEquals(expectedDocuments.size(), foundSources.size());
                    expectedDocuments.forEach(expectedDoc -> Assert.assertTrue(foundSources.contains(expectedDoc)));
                }
        );
        // Wait for service map processor window duration
        Thread.sleep(3000);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    final List<Map<String, Object>> foundSources = getSourcesFromIndex(restHighLevelClient, SERVICE_MAP_INDEX_NAME);
                    Assert.assertEquals(8, foundSources.size());
                    final List<Map<String, Object>> foundTargets = getTargets(foundSources);
                    Assert.assertTrue(foundTargets.size() == expectedTargets.size() &&
                            foundTargets.containsAll(expectedTargets));
                    final List<Map<String, Object>> foundEdges = getEdges(foundSources);
                    Assert.assertTrue(foundEdges.size() == expectedEdges.size() &&
                            foundEdges.containsAll(expectedEdges));
                }
        );
    }

    /**
     * Gets a new root trace id.
     */
    private byte[] getTraceId() {
        if(traceId == null) {
            traceId = getRandomBytes(16);
        }
        return traceId;
    }

    /**
     * Gets a span id and adds to the list of existing span ids
     */
    private byte[] getSpanId() {
        final byte[] spanId = getRandomBytes(8);
        spanIds.add(spanId);
        return spanId;
    }

    /**
     * Gets a parent id. For the root span return null, otherwise picks the previous span id
     */
    private byte[] getParentId() {
        if(spanIds.isEmpty()) {
            return null;
        } else {
            return spanIds.get(spanIds.size() - 1);
        }
    }

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }

    private void sendExportTraceServiceRequestToSource(ExportTraceServiceRequest request) throws InvalidProtocolBufferException {
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(request).getBytes())).aggregate().join();
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
            spanId, final byte[] parentId, final byte[] traceId, final Span.SpanKind spanKind) throws UnsupportedEncodingException {
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

    public static ExportTraceServiceRequest getExportTraceServiceRequest(List<ResourceSpans> spans){
        return ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(spans)
                .build();
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
        esDocSource.put("traceId", Base64.encodeBase64String(span.getTraceId().toByteArray()));
        esDocSource.put("spanId", Base64.encodeBase64String(span.getSpanId().toByteArray()));
        if (!span.getParentSpanId().isEmpty()) {
            esDocSource.put("parentSpanId", Base64.encodeBase64String(span.getParentSpanId().toByteArray()));
        }
        esDocSource.put("name", span.getName());
        esDocSource.put("kind", span.getKind().name());
        esDocSource.put("resource.attributes.service.name", serviceName);
        return esDocSource;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getTargets(final List<Map<String, Object>> sources) {
        final List<Map<String, Object>> targets = new ArrayList<>();
        for (final Map<String, Object> source: sources) {
            if (source.getOrDefault("target", null) != null) {
                targets.add((Map<String, Object>) source.get("target"));
            }
        }

        return targets;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getEdges(final List<Map<String, Object>> sources) {
        final List<Map<String, Object>> edges = new ArrayList<>();
        for (final Map<String, Object> source: sources) {
            if (source.getOrDefault("destination", null) != null) {
                final Map<String, Object> edge = new HashMap<>((Map<String, Object>) source.get("destination"));
                edge.put("serviceName", source.getOrDefault("serviceName", null));
                edge.put("traceGroupName", source.getOrDefault("traceGroupName", null));
                edges.add(edge);
            }
        }
        return edges;
    }

    private byte[] getRandomBytes(int len) {
        final byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    private List<ResourceSpans> getRandomResourceSpans(int len) throws UnsupportedEncodingException {
        spanIds = new ArrayList<>();
        traceId = null;
        final ArrayList<ResourceSpans> spansList = new ArrayList<>();
        for(int i=0; i<len; i++) {
            final byte[] parentId = getParentId();
            final byte[] spanId = getSpanId();
            spansList.add(
                    getResourceSpans(
                            serviceNames.get(i % serviceNames.size()),
                            traceGroups.get(i % traceGroups.size()),
                            spanId,
                            parentId,
                            getTraceId(),
                            SPAN_KINDS.get(i % SPAN_KINDS.size())
                    )
            );
        }
        return spansList;
    }

    private String getServiceName(final ResourceSpans resourceSpans) {
        return resourceSpans.getResource().getAttributesList().stream().filter(kv -> kv.getKey() == "service.name")
                .findFirst().get().getValue().getStringValue();
    }
}
