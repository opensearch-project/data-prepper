/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration.log;

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
import org.junit.Test;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.loggenerator.ApacheLogFaker;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class EndToEndBasicLogTest {
    private static final int HTTP_SOURCE_PORT = 2021;

    private final ApacheLogFaker apacheLogFaker = new ApacheLogFaker();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testPipelineEndToEnd() throws JsonProcessingException {
        final String testIndexName = "test-grok-index";
        // Send data to http source
        sendHttpRequestToSource(HTTP_SOURCE_PORT, generateRandomApacheLogHttpData(2));
        sendHttpRequestToSource(HTTP_SOURCE_PORT, generateRandomApacheLogHttpData(3));
        final RestHighLevelClient restHighLevelClient = prepareOpenSearchRestHighLevelClient();
        final List<Map<String, Object>> retrievedDocs = new ArrayList<>();

        // Wait for data to flow through pipeline and be indexed by ES
        makeRequestAndWaitForResponse(restHighLevelClient, testIndexName, retrievedDocs);

        // Verify original and grokked keys from retrieved docs
        final List<String> expectedDocKeys = createListOfExpectedDocumentKeys();
        assertThatGrokkedKeysOccurInRetreivedDocuments(expectedDocKeys, retrievedDocs);
    }

    @Test
    public void testPipelineWithDatePatternedIndexEndToEnd() throws JsonProcessingException {
        final String testIndexName = "test-grok-index-%s";
        // Send data to http source
        sendHttpRequestToSource(HTTP_SOURCE_PORT, generateRandomApacheLogHttpData(2));
        sendHttpRequestToSource(HTTP_SOURCE_PORT, generateRandomApacheLogHttpData(3));
        final RestHighLevelClient restHighLevelClient = prepareOpenSearchRestHighLevelClient();
        final List<Map<String, Object>> retrievedDocs = new ArrayList<>();

        // Wait for data to flow through pipeline and be indexed by ES
        makeRequestAndWaitForResponse(restHighLevelClient, String.format(testIndexName, DateTimeFormatter.ofPattern("yyyy.MM.dd").format(LocalDateTime.now())), retrievedDocs);

        // Verify original and grokked keys from retrieved docs
        final List<String> expectedDocKeys = createListOfExpectedDocumentKeys();
        assertThatGrokkedKeysOccurInRetreivedDocuments(expectedDocKeys, retrievedDocs);
    }

    private void assertThatGrokkedKeysOccurInRetreivedDocuments(List<String> expectedDocKeys, List<Map<String, Object>> retrievedDocuments) {
        retrievedDocuments.forEach(expectedDoc -> {
            for (String key: expectedDocKeys) {
                assertThat(expectedDoc, hasKey(key));
            }

            verifyVerbFieldNotDuplicated(expectedDoc);
        });
    }

    /**
     * Verifies that the verb field does not contain duplicated values.
     * This is to ensure that grok processor doesn't produce duplicate values.
     */
    private void verifyVerbFieldNotDuplicated(Map<String, Object> document) {
        if (document.containsKey("verb") && document.get("verb") != null) {
            Object verbValue = document.get("verb");
            assertFalse(verbValue instanceof List, "Verb field should not contain duplicates: " + verbValue);
        }
    }

    private List<String> createListOfExpectedDocumentKeys() {
        return Arrays.asList("date", "log", "clientip", "ident", "auth", "timestamp", "verb", "request", "httpversion", "response", "bytes");
    }

    private void makeRequestAndWaitForResponse(RestHighLevelClient restHighLevelClient, String testIndexName, List<Map<String, Object>> retrievedDocs) {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices(restHighLevelClient);
                    final SearchRequest searchRequest = new SearchRequest(testIndexName);
                    searchRequest.source(
                            SearchSourceBuilder.searchSource().size(100)
                    );
                    final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> foundSources = getSourcesFromSearchHits(searchResponse.getHits());
                    assertEquals(5, foundSources.size());
                    retrievedDocs.addAll(foundSources);
                }
        );
    }

    private RestHighLevelClient prepareOpenSearchRestHighLevelClient() {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        return builder.build().createClient(null);
    }

    private void sendHttpRequestToSource(final int port, final HttpData httpData) {
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority(String.format("127.0.0.1:%d", port))
                        .method(HttpMethod.POST)
                        .path("/grok-pipeline/logs")
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

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }

    private HttpData generateRandomApacheLogHttpData(final int numLogs) throws JsonProcessingException {
        final List<Map<String, Object>> jsonArray = new ArrayList<>();
        for (int i = 0; i < numLogs; i++) {
            final Map<String, Object> logObj = Map.of(
                "date", System.currentTimeMillis(),
                "log", apacheLogFaker.generateRandomExtendedApacheLog()
            );
            jsonArray.add(logObj);
        }
        final String jsonData = objectMapper.writeValueAsString(jsonArray);
        return HttpData.ofUtf8(jsonData);
    }
}
