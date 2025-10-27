/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration.peerforwarder;

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
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.loggenerator.ApacheLogFaker;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import java.util.concurrent.ThreadLocalRandom;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class EndToEndLogMetricsTest {
    private static final int HTTP_SOURCE_PORT_1 = 2021;
    private static final int HTTP_SOURCE_PORT_2 = 2022;
    private static final String TEST_INDEX_NAME = "test-log-metrics-index";
    private static final String CUSTOM_KEY_1 = "verb";
    private static final String CUSTOM_KEY_2 = "bytes";
    private static final String SOURCE_IP = "127.0.0.1";
    private static final String DESTINATION_IP_1 = "8.8.8.8";
    private static final String DESTINATION_IP_2 = "12.12.13.13";
    private static final int TOTAL_EVENTS = 40;
    public static final String SOURCE_IP_KEY = "sourceIp";
    public static final String DESTINATION_IP_KEY = "destinationIp";
    private double expectedMin = Float.MAX_VALUE;
    private double expectedMax = -Float.MAX_VALUE;
    private double expectedSum = 0;

    private final ApacheLogFaker apacheLogFaker = new ApacheLogFaker();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();

    @Test
    public void testLogMetricsPipelineWithMultipleNodesEndToEnd() throws JsonProcessingException {
        double[] latencies = new double[TOTAL_EVENTS];
        for (int i = 0; i < TOTAL_EVENTS; i++) {
            latencies[i] = ThreadLocalRandom.current().nextDouble(-2, 12);
            expectedSum += latencies[i];
            if (latencies[i] < expectedMin) {
                expectedMin = latencies[i];
            }
            if (latencies[i] > expectedMax) {
                expectedMax = latencies[i];
            }
        }
        int latencyIdx = 0;
        for (int i = 0; i < TOTAL_EVENTS / 4; i++) {
            sendHttpRequestToSource(HTTP_SOURCE_PORT_1, generateRandomApacheLogHttpData(DESTINATION_IP_1, latencies[latencyIdx++], CUSTOM_KEY_1, "POST"));
            sendHttpRequestToSource(HTTP_SOURCE_PORT_1, generateRandomApacheLogHttpData(DESTINATION_IP_2, latencies[latencyIdx++], CUSTOM_KEY_2, String.valueOf(random.nextInt())));
            sendHttpRequestToSource(HTTP_SOURCE_PORT_2, generateRandomApacheLogHttpData(DESTINATION_IP_1, latencies[latencyIdx++], CUSTOM_KEY_2, String.valueOf(random.nextInt())));
            sendHttpRequestToSource(HTTP_SOURCE_PORT_2, generateRandomApacheLogHttpData(DESTINATION_IP_2, latencies[latencyIdx++], CUSTOM_KEY_1, "GET"));
        }

        verifyDataInOpenSearch();
    }

    private void verifyDataInOpenSearch() {
        // Verify data in OpenSearch backend
        final RestHighLevelClient restHighLevelClient = prepareOpenSearchRestHighLevelClient();
        final List<Map<String, Object>> retrievedDocs = new ArrayList<>();
        // Wait for data to flow through pipeline and be indexed by ES
        await().atMost(45, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    refreshIndices(restHighLevelClient);
                    final SearchRequest searchRequest = new SearchRequest(TEST_INDEX_NAME);
                    searchRequest.source(
                            SearchSourceBuilder.searchSource().size(100)
                    );
                    final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                    final List<Map<String, Object>> foundSources = getSourcesFromSearchHits(searchResponse.getHits());
                    assertThat(foundSources.size(), equalTo(2));
                    retrievedDocs.addAll(foundSources);
                }
        );

        verifyDocuments(retrievedDocs);
    }

    private void verifyDocuments(List<Map<String, Object>> retrievedDocs) {
        // Verify original and aggregated keys from retrieved docs
        final List<String> expectedDocKeys = Arrays.asList(
                "kind", "name", "description", "unit", "min", "max", "count", "sum", "time", "startTime", "attributes",
                "buckets", "bucketCountsList", "bucketCounts",  "explicitBoundsCount", "explicitBounds", "aggregationTemporality");
        final List<String> attributeKeys = Arrays.asList(DESTINATION_IP_KEY, SOURCE_IP_KEY, "histogram_key", "aggr._duration");
        double receivedMin = Float.MAX_VALUE;
        double receivedMax = -Float.MAX_VALUE;
        double receivedSum = 0.0;
        int receivedCount = 0;
        for (Map<String, Object> retrievedDoc: retrievedDocs) {
            for (String key: expectedDocKeys) {
                assertThat(retrievedDoc, hasKey(key));
                if (key.equals("kind")) {
                    assertThat((String)retrievedDoc.get("kind"), equalTo("HISTOGRAM"));
                }
                if (key.equals("explicitBoundsCount")) {
                    assertThat(retrievedDoc.get("explicitBoundsCount"), equalTo(5));
                }
                if (key.equals("bucketCounts")) {
                    assertThat(retrievedDoc.get("bucketCounts"), equalTo(6));
                }
                if (key.equals("min")) {
                    double minVal = (double)retrievedDoc.get("min");
                    if (minVal < receivedMin) {
                        receivedMin = minVal;
                    }
                }
                if (key.equals("max")) {
                    double maxVal = (double)retrievedDoc.get("max");
                    if (maxVal > receivedMax) {
                        receivedMax = maxVal;
                    }
                }
                if (key.equals("sum")) {
                    receivedSum += (double)retrievedDoc.get("sum");
                }
                if (key.equals("count")) {
                    receivedCount += (int)retrievedDoc.get("count");
                }
            }

            final Map<String, Object> attributes = (Map<String, Object>) retrievedDoc.get("attributes");
            assertThat(attributes, notNullValue());
            assertThat(attributes.size(), equalTo(4));

            for (String attributeKey: attributeKeys) {
                assertThat(attributes.containsKey(attributeKey), equalTo(true));
            }
            assertThat((String)attributes.get("histogram_key"), equalTo("latency"));
            assertThat((String)attributes.get(SOURCE_IP_KEY), equalTo(SOURCE_IP));
        }

        assertThat(TOTAL_EVENTS, equalTo(receivedCount));
        assertThat(expectedMin, equalTo(receivedMin));
        assertThat(expectedMax, equalTo(receivedMax));
        assertThat(expectedSum, closeTo(receivedSum, 0.1));
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

    private HttpData generateRandomApacheLogHttpData(final String destinationIp, double latency, final String customKey, final String customValue) throws JsonProcessingException {
        final List<Map<String, Object>> jsonArray = new ArrayList<>();
        final Map<String, Object> logObj = new HashMap<>();
        logObj.put("date", System.currentTimeMillis());
        logObj.put("log", apacheLogFaker.generateRandomExtendedApacheLog());
        logObj.put("latency", latency);
        logObj.put(SOURCE_IP_KEY, SOURCE_IP);
        logObj.put(DESTINATION_IP_KEY, destinationIp);
        logObj.put(customKey, customValue);
        jsonArray.add(logObj);

        final String jsonData = objectMapper.writeValueAsString(jsonArray);
        return HttpData.ofUtf8(jsonData);
    }
}
