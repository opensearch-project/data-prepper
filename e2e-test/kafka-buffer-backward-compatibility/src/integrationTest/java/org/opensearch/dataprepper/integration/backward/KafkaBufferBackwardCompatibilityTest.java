 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */

package org.opensearch.dataprepper.integration.backward;

 import com.fasterxml.jackson.core.JsonProcessingException;
 import com.fasterxml.jackson.databind.ObjectMapper;
 import com.linecorp.armeria.client.WebClient;
 import com.linecorp.armeria.common.HttpData;
 import com.linecorp.armeria.common.HttpMethod;
 import com.linecorp.armeria.common.HttpStatus;
 import com.linecorp.armeria.common.MediaType;
 import com.linecorp.armeria.common.RequestHeaders;
 import com.linecorp.armeria.common.SessionProtocol;
 import org.junit.jupiter.api.BeforeEach;
 import org.junit.jupiter.api.Test;
 import org.opensearch.action.admin.indices.refresh.RefreshRequest;
 import org.opensearch.action.search.SearchRequest;
 import org.opensearch.action.search.SearchResponse;
 import org.opensearch.client.RequestOptions;
 import org.opensearch.client.RestHighLevelClient;
 import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
 import org.opensearch.search.SearchHits;
 import org.opensearch.search.builder.SearchSourceBuilder;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.io.IOException;
 import java.util.ArrayList;
 import java.util.Collections;
 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import java.util.UUID;
 import java.util.concurrent.TimeUnit;

 import static org.awaitility.Awaitility.await;
 import static org.hamcrest.CoreMatchers.is;
 import static org.hamcrest.MatcherAssert.assertThat;
 import static org.hamcrest.Matchers.hasKey;
 import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Kafka Buffer Backward Compatibility End-to-End Test
 *
 * This test verifies that the current build of Data Prepper can successfully read
 * and process messages written to Kafka by a previous released version.
 *
 * Test Scenario:
 * 1. Phase 1 - Write with Released Version:
 *    - Released Data Prepper (latest version 2) receives HTTP requests
 *    - Writes 2 test records to Kafka buffer
 *
 * 2. Phase 2 - Read with Current Build:
 *    - Current Data Prepper (built from source) reads from Kafka
 *    - Processes and writes messages to OpenSearch
 *
 * 3. Phase 3 - Verification:
 *    - Verify 2 records exist in OpenSearch with correct content
 *
 * This ensures backward compatibility of Kafka message format and serialization.
 *
 * Note: Gradle manages container lifecycle. Test only handles sending data and verification.
 */
public class KafkaBufferBackwardCompatibilityTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBufferBackwardCompatibilityTest.class);

    private static final int WRITER_HTTP_PORT = 2021;
    private static final String TEST_INDEX_NAME = "kafka-buffer-backward-compatibility-test-index";
    private static final String MESSAGE_KEY = "message";
    private String testValue1;
    private String testValue2;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setup() {
        testValue1 = UUID.randomUUID().toString();
        testValue2 = UUID.randomUUID().toString();
    }

    @Test
    public void testBackwardCompatibility() throws Exception {
        LOG.info("========================================");
        LOG.info("Starting Kafka Backward Compatibility Test");
        LOG.info("========================================");

        // Phase 1: Send data to released Data Prepper (writer)
        LOG.info("Phase 1: Sending test records to released Data Prepper...");
        sendHttpRequest(generateTestData(testValue1), WRITER_HTTP_PORT);
        LOG.info("Sent record 1: {}", testValue1);

        sendHttpRequest(generateTestData(testValue2), WRITER_HTTP_PORT);
        LOG.info("Sent record 2: {}", testValue2);

        // Phase 2: Current Data Prepper automatically reads from Kafka (already running)
        LOG.info("Phase 2: Current Data Prepper is reading from Kafka...");
        LOG.info("Waiting for data to be processed and written to OpenSearch...");

        // Phase 3: Verify data in OpenSearch
        LOG.info("Phase 3: Verifying data in OpenSearch...");
        verifyDataInOpenSearch();

        LOG.info("========================================");
        LOG.info("✅ Backward Compatibility Test PASSED");
        LOG.info("========================================");
    }

    private void verifyDataInOpenSearch() {
        final RestHighLevelClient restHighLevelClient = prepareOpenSearchRestHighLevelClient();
        final List<Map<String, Object>> retrievedDocs = new ArrayList<>();

        LOG.info("Querying OpenSearch for test data...");

        // Wait for data to be indexed
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            refreshIndices(restHighLevelClient);
            final SearchRequest searchRequest = new SearchRequest(TEST_INDEX_NAME);
            searchRequest.source(SearchSourceBuilder.searchSource().size(100));

            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            final List<Map<String, Object>> foundSources = getSourcesFromSearchHits(searchResponse.getHits());

            LOG.info("Found {} documents in OpenSearch", foundSources.size());
            assertEquals(2, foundSources.size(), "Expected exactly 2 documents in OpenSearch");
            retrievedDocs.addAll(foundSources);
        });

        // Verify the content of retrieved documents
        assertEquals(2, retrievedDocs.size(), "Should have exactly 2 documents");

        LOG.info("Verifying document contents...");
        boolean foundRecord1 = false;
        boolean foundRecord2 = false;

        for (Map<String, Object> doc : retrievedDocs) {
            assertThat("Document should have 'message' key", doc, hasKey(MESSAGE_KEY));
            assertThat("Document should have '@timestamp' key", doc, hasKey("@timestamp"));

            String message = (String) doc.get(MESSAGE_KEY);
            LOG.info("Found document with message: {}", message);

            if (testValue1.equals(message)) {
                foundRecord1 = true;
            } else if (testValue2.equals(message)) {
                foundRecord2 = true;
            }
        }

        assertThat("Should find record 1", foundRecord1, is(true));
        assertThat("Should find record 2", foundRecord2, is(true));

        LOG.info("✅ All documents verified successfully");
    }

    private RestHighLevelClient prepareOpenSearchRestHighLevelClient() {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://127.0.0.1:9200"));
        builder.withUsername("admin");
        builder.withPassword("admin");
        builder.withInsecure(true);
        return builder.build().createClient(null);
    }

    private void sendHttpRequest(final HttpData httpData, int port) {
        WebClient.of().execute(
                RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority(String.format("127.0.0.1:%d", port))
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                httpData)
                .aggregate()
                .whenComplete((response, ex) -> {
                    if (ex != null) {
                        LOG.error("HTTP request failed", ex);
                        throw new RuntimeException("Failed to send HTTP request", ex);
                    }
                    assertThat("HTTP response should be 200 OK",
                            response.status(), is(HttpStatus.OK));
                    LOG.debug("HTTP request successful: {}", response.status());
                }).join();
    }

    private HttpData generateTestData(final String testValue) throws JsonProcessingException {
        final List<Map<String, Object>> jsonArray = new ArrayList<>();
        final Map<String, Object> logObj = new HashMap<>();
        logObj.put("timestamp", System.currentTimeMillis());
        logObj.put(MESSAGE_KEY, testValue);
        jsonArray.add(logObj);

        final String jsonData = objectMapper.writeValueAsString(jsonArray);
        return HttpData.ofUtf8(jsonData);
    }

    private List<Map<String, Object>> getSourcesFromSearchHits(final SearchHits searchHits) {
        final List<Map<String, Object>> sources = new ArrayList<>();
        searchHits.forEach(hit -> sources.add(hit.getSourceAsMap()));
        return sources;
    }

    private void refreshIndices(final RestHighLevelClient restHighLevelClient) throws IOException {
        final RefreshRequest requestAll = new RefreshRequest();
        restHighLevelClient.indices().refresh(requestAll, RequestOptions.DEFAULT);
    }
}
