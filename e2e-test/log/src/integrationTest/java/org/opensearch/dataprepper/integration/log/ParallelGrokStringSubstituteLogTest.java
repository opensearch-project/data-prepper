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
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
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
import static org.mockito.Mockito.mock;

public class ParallelGrokStringSubstituteLogTest {
    private static final int HTTP_SOURCE_PORT = 2021;
    private static final String GROK_INDEX_NAME = "test-grok-index";
    private static final String SUBSTITUTE_INDEX_NAME = "test-substitute-index";
    private static final String TEST_STRING = "firstword secondword thirdword";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testPipelineEndToEnd() throws JsonProcessingException {
        // Send data to http source
        sendHttpRequestToSource(HTTP_SOURCE_PORT, generateRandomApacheLogHttpData());
        // Verify data in OpenSearch backend
        final RestHighLevelClient restHighLevelClient = prepareOpenSearchRestHighLevelClient();

        // Wait for data to flow through pipeline and be indexed by ES
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    refreshIndices(restHighLevelClient);
                    final SearchRequest grokRequest = new SearchRequest(GROK_INDEX_NAME);
                    final SearchRequest substRequest = new SearchRequest(SUBSTITUTE_INDEX_NAME);
                    grokRequest.source(
                            SearchSourceBuilder.searchSource().size(100)
                    );
                    substRequest.source(
                            SearchSourceBuilder.searchSource().size(100)
                    );
                    // Verify documents from grok index
                    final SearchResponse grokResponse = restHighLevelClient.search(grokRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> grokSources = getSourcesFromSearchHits(grokResponse.getHits());

                    Assert.assertEquals(1, grokSources.size());
                    Map<String, Object> grokSource = grokSources.get(0);
                    Assert.assertEquals(4, grokSource.size());
                    Assert.assertEquals(TEST_STRING, grokSource.get("message"));
                    String[] words = TEST_STRING.split(" ");
                    Assert.assertEquals(words[0], grokSource.get("word1"));
                    Assert.assertEquals(words[1], grokSource.get("word2"));
                    Assert.assertEquals(words[2], grokSource.get("word3"));

                    // Verify documents from substitute index
                    final SearchResponse substResponse = restHighLevelClient.search(substRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> substSources = getSourcesFromSearchHits(substResponse.getHits());

                    Assert.assertEquals(1, substSources.size());
                    Map<String, Object> substSource = substSources.get(0);
                    Assert.assertEquals(1, substSource.size());
                    String expectedString = TEST_STRING.replace("word", "WORD");
                    Assert.assertEquals(expectedString, substSource.get("message"));
                }
        );
    }

    private RestHighLevelClient prepareOpenSearchRestHighLevelClient() {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        builder.withInsecure(true);
        final AwsCredentialsSupplier awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        return builder.build().createClient(awsCredentialsSupplier);
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

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }

    private HttpData generateRandomApacheLogHttpData() throws JsonProcessingException {
        final List<Map<String, Object>> jsonArray = new ArrayList<>();
        final Map<String, Object> logObj = Map.of("message", TEST_STRING);
        jsonArray.add(logObj);
        final String jsonData = objectMapper.writeValueAsString(jsonArray);
        return HttpData.ofUtf8(jsonData);
    }
}
