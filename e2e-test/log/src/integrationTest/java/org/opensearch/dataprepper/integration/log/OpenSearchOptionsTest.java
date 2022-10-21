/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration.log;

import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.loggenerator.ApacheLogFaker;
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
import java.util.HashMap;
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
    private static final String[] test_ids = {"my_id1", "my_id2"};
    private static final String[] test_routing_ids = {"my_rid1", "my_rid2"};

    private final ApacheLogFaker apacheLogFaker = new ApacheLogFaker();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testPipelineEndToEnd() throws JsonProcessingException {
        // Send data to http source
        sendHttpRequestToSource(HTTP_SOURCE_PORT, generateRandomApacheLogHttpData());
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
                            int test_ids0_count = 0;
                            int test_ids1_count = 0;
                            int other_ids_count = 0;
                            for (int j = 0; j < ids.size(); j++) {
                                if (ids.get(j).equals(test_ids[0])) {
                                    test_ids0_count++;
                                } else if (ids.get(j).equals(test_ids[1])) {
                                    test_ids1_count++;
                                } else {
                                    other_ids_count++;
                                }
                            }
                            Assert.assertEquals(test_ids0_count, 1);
                            Assert.assertEquals(test_ids1_count, 1);
                            Assert.assertEquals(other_ids_count, 4);

                            final List<String> routing_ids = getRoutingIdsFromSearchHits(testIndexResponse.getHits());
                            Assert.assertEquals(2, routing_ids.size());
                            for (int j = 0; j < routing_ids.size(); j++) {
                                assertThat((routing_ids.get(j).equals(test_routing_ids[0]) || routing_ids.get(j).equals(test_routing_ids[1])), is(true));
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

    private List<String> getRoutingIdsFromSearchHits(final SearchHits searchHits) {
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

    private HttpData generateRandomApacheLogHttpData() throws JsonProcessingException {
        final List<Map<String, Object>> jsonArray = new ArrayList<>();
        final Map<String, Object> infoObj1 = new HashMap<String, Object>() {{
            put("id", test_ids[0]);
            put("rid", test_routing_ids[0]);
        }};
        final Map<String, Object> infoObj2 = new HashMap<String, Object>() {{
            put("id", test_ids[1]);
            put("rid", test_routing_ids[1]);
        }};
        final Map<String, Object> logObj1 = new HashMap<String, Object>() {{
            put("message", testString[0]);
            put("idx", "test");
            put("info", infoObj1);
        }};
        final Map<String, Object> logObj2 = new HashMap<String, Object>() {{
            put("message", testString[1]);
            put("idx", "test");
            put("info", infoObj2);
        }};
        final Map<String, Object> ridsObj1 = new HashMap<String, Object>() {{
            put("rid", test_routing_ids[0]);
        }};
        final Map<String, Object> ridsObj2 = new HashMap<String, Object>() {{
            put("rid", test_routing_ids[1]);
        }};
        final Map<String, Object> idsObj1 = new HashMap<String, Object>() {{
            put("id", test_ids[0]);
        }};
        final Map<String, Object> idsObj2 = new HashMap<String, Object>() {{
            put("id", test_ids[1]);
        }};
        final Map<String, Object> infoObj3 = new HashMap<String, Object>() {{
            put("ids", idsObj1);
            put("rids", ridsObj1);
        }};
        final Map<String, Object> infoObj4 = new HashMap<String, Object>() {{
            put("ids", idsObj2);
            put("rids", ridsObj2);
        }};
        final Map<String, Object> logObj3 = new HashMap<String, Object>() {{
            put("message", testString[0]);
            put("idx", "test");
            put("info", infoObj3);
        }};
        final Map<String, Object> logObj4 = new HashMap<String, Object>() {{
            put("message", testString[1]);
            put("idx", "test");
            put("info", infoObj4);
        }};
        final Map<String, Object> logObj5 = new HashMap<String, Object>() {{
            put("message", testString[0]);
            put("id", test_ids[0]);
            put("rid", test_routing_ids[0]);
        }};
        final Map<String, Object> logObj6 = new HashMap<String, Object>() {{
            put("message", testString[1]);
            put("id", test_ids[1]);
            put("rid", test_routing_ids[1]);
        }};
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
