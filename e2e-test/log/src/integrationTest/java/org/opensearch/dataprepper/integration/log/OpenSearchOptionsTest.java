/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration.log;

import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import io.netty.util.AsciiString;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.document.DocumentField;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;


public class OpenSearchOptionsTest {
    private static final int HTTP_SOURCE_PORT = 2021;
    private static final String[] TEST_INDEX_NAMES = {"test-index-one", "test-index-two", "test-index-three"};
    private static final String[] testString = {"this is a test message", "this is another test message"};
    private static final String[] testIds = {"my_id1", "my_id2"};
    private static final String[] testRouting = {"my_rid1", "my_rid2"};

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testPipelineEndToEnd() throws JsonProcessingException {
        // Send data to http source
        sendHttpRequestToSource(HTTP_SOURCE_PORT, generateRandomLogHttpData());
        // Verify data in OpenSearch backend
        final RestHighLevelClient restHighLevelClient = prepareOpenSearchRestHighLevelClient();
        final List<Map<String, Object>> retrievedDocs = new ArrayList<>();
        // Wait for data to flow through pipeline and be indexed by ES
        await().atMost(20, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices(restHighLevelClient);
                    // All indexes receive 6 events with different id and rid 
                    // values. Verify that they are received the way it is
                    // expected.
                    for (int idx = 0; idx < TEST_INDEX_NAMES.length; idx++) {
                        final SearchRequest testIndexRequest = new SearchRequest(TEST_INDEX_NAMES[idx]);
                        testIndexRequest.source(
                            SearchSourceBuilder.searchSource().size(100)
                        );
                        final SearchResponse testIndexResponse = restHighLevelClient.search(testIndexRequest, RequestOptions.DEFAULT);
                        final List<Map<String, Object>> testIndexSources = getSourcesFromSearchHits(testIndexResponse.getHits());
                        Assert.assertEquals(6, testIndexSources.size());
                        for (int i = 0; i < testIndexSources.size(); i++) {
                            Map<String, Object> testIndexSource = testIndexSources.get(i);
                            Assert.assertEquals(3, testIndexSource.size());
                            Assert.assertEquals(testIndexSource.get("message"), testString[i%2]);
                            final List<String> ids = getIdsFromSearchHits(testIndexResponse.getHits());
                            Assert.assertEquals(6, ids.size());
                            int testIds0Count = 0;
                            int testIds1Count = 0;
                            int otherIdsCount = 0;
                            for (int j = 0; j < ids.size(); j++) {
                                if (ids.get(j).equals(testIds[0])) {
                                    testIds0Count++;
                                } else if (ids.get(j).equals(testIds[1])) {
                                    testIds1Count++;
                                } else {
                                    otherIdsCount++;
                                }
                            }
                            Assert.assertEquals(testIds0Count, 1);
                            Assert.assertEquals(testIds1Count, 1);
                            Assert.assertEquals(otherIdsCount, 4);

                            final List<String> routingFields = getRoutingFieldsFromSearchHits(testIndexResponse.getHits());
                            Assert.assertEquals(2, routingFields.size());
                            for (int j = 0; j < routingFields.size(); j++) {
                                assertThat((routingFields.get(j).equals(testRouting[0]) || routingFields.get(j).equals(testRouting[1])), is(true));
                            }
                        }
                    }
                }
        );
    }

    private RestHighLevelClient prepareOpenSearchRestHighLevelClient() {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        return builder.build().createClient();
    }

    private void sendHttpRequestToSource(final int port, final HttpData httpData) {
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority(String.format("127.0.0.1:%d", port))
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                httpData)
                .aggregate()
                .whenComplete((i, ex) -> {
                    assertThat(i.status(), is(HttpStatus.OK));
                    final List<String> headerKeys = i.headers()
                            .stream()
                            .map(Map.Entry::getKey)
                            .map(AsciiString::toString)
                            .collect(Collectors.toList());
                    assertThat("Response Header Keys", headerKeys, not(contains("server")));
                }).join();
    }

    private List<Map<String, Object>> getSourcesFromSearchHits(final SearchHits searchHits) {
        final List<Map<String, Object>> sources = new ArrayList<>();
        searchHits.forEach(hit -> {
            Map<String, Object> source = hit.getSourceAsMap();
            sources.add(source);
        });
        return sources;
    }

    private List<String> getIdsFromSearchHits(final SearchHits searchHits) {
        final List<String> ids = new ArrayList<>();
        searchHits.forEach(hit -> {
            String id = hit.getId();
            ids.add(id);
        });
        return ids;
    }

    private List<String> getRoutingFieldsFromSearchHits(final SearchHits searchHits) {
        final List<String> rids = new ArrayList<>();
        searchHits.forEach(hit -> {
            DocumentField routing = hit.field("_routing");
            if (routing != null) {
                String rid = routing.getValue();
                if (rid != null) {
                    rids.add(rid);
                }
            }
        });
        return rids;
    }

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }

    private HttpData generateRandomLogHttpData() throws JsonProcessingException {
        final List<Map<String, Object>> jsonArray = new ArrayList<>();
        final Map<String, Object> infoObj1 = Map.of(
	    "id", testIds[0],
	    "rid", testRouting[0]
	);
        final Map<String, Object> infoObj2 = Map.of(
            "id", testIds[1],
            "rid", testRouting[1]
        );
        final Map<String, Object> logObj1 = Map.of(
            "message", testString[0],
            "idx", "test",
            "info", infoObj1
	);
        final Map<String, Object> logObj2 = Map.of(
            "message", testString[1],
            "idx", "test",
            "info", infoObj2
        );
        final Map<String, Object> ridsObj1 = Map.of(
            "rid", testRouting[0]
        );
        final Map<String, Object> ridsObj2 = Map.of(
            "rid", testRouting[1]
        );
        final Map<String, Object> idsObj1 = Map.of(
            "id", testIds[0]
        );
        final Map<String, Object> idsObj2 = Map.of(
            "id", testIds[1]
        );
        final Map<String, Object> infoObj3 = Map.of(
            "ids", idsObj1,
            "rids", ridsObj1
        );
        final Map<String, Object> infoObj4 = Map.of(
            "ids", idsObj2,
            "rids", ridsObj2
        );
        final Map<String, Object> logObj3 = Map.of(
            "message", testString[0],
            "idx", "test",
            "info", infoObj3
        );
        final Map<String, Object> logObj4 = Map.of(
            "message", testString[1],
            "idx", "test",
            "info", infoObj4
        );
        final Map<String, Object> logObj5 = Map.of(
            "message", testString[0],
            "id", testIds[0],
            "rid", testRouting[0]
        );
        final Map<String, Object> logObj6 = Map.of(
            "message", testString[1],
            "id", testIds[1],
            "rid", testRouting[1]
        );
        jsonArray.add(logObj1);
        jsonArray.add(logObj2);
        jsonArray.add(logObj3);
        jsonArray.add(logObj4);
        jsonArray.add(logObj5);
        jsonArray.add(logObj6);
        // Send data with ids "id", "info/id" and "info/ids/id" and
        // with routing "rid", "info/rid" and "info/rids/rid" and
        final String jsonData = objectMapper.writeValueAsString(jsonArray);
        return HttpData.ofUtf8(jsonData);
    }
}
