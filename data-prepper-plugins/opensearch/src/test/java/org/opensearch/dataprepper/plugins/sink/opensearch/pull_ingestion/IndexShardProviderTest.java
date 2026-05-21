/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import org.apache.http.HttpEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IndexShardProviderTest {

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private RestClient restClient;

    @Mock
    private Response response;

    @Mock
    private HttpEntity httpEntity;

    private String indexName;
    private String topicName;
    private int shardCount;

    @BeforeEach
    void setUp() {
        indexName = UUID.randomUUID().toString();
        topicName = UUID.randomUUID().toString();
        shardCount = 5;

        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
    }

    private IndexShardProvider createObjectUnderTest() {
        return new IndexShardProvider(restHighLevelClient);
    }

    private String buildSettingsResponse() {
        return "{\n" +
                "  \"" + indexName + "\": {\n" +
                "    \"settings\": {\n" +
                "      \"index\": {\n" +
                "        \"ingestion_source\": {\n" +
                "          \"type\": \"kafka\",\n" +
                "          \"param\": {\n" +
                "            \"bootstrap_servers\": \"kafka:9092\",\n" +
                "            \"topic\": \"" + topicName + "\"\n" +
                "          }\n" +
                "        },\n" +
                "        \"number_of_shards\": \"" + shardCount + "\",\n" +
                "        \"number_of_replicas\": \"1\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
    }

    @Test
    void getNumberOfShards_fetches_from_cluster() throws IOException {
        mockSettingsResponse(buildSettingsResponse());

        assertThat(createObjectUnderTest().getNumberOfShards(indexName), equalTo(shardCount));
    }

    @Test
    void getIngestionTopic_fetches_from_cluster() throws IOException {
        mockSettingsResponse(buildSettingsResponse());

        assertThat(createObjectUnderTest().getIngestionTopic(indexName), equalTo(topicName));
    }

    @Test
    void getNumberOfShards_caches_result() throws IOException {
        mockSettingsResponse(buildSettingsResponse());

        final IndexShardProvider objectUnderTest = createObjectUnderTest();
        objectUnderTest.getNumberOfShards(indexName);
        objectUnderTest.getNumberOfShards(indexName);
        objectUnderTest.getNumberOfShards(indexName);

        verify(restClient, times(1)).performRequest(any());
    }

    @Test
    void getIngestionTopic_uses_same_cache_as_shards() throws IOException {
        mockSettingsResponse(buildSettingsResponse());

        final IndexShardProvider objectUnderTest = createObjectUnderTest();
        objectUnderTest.getNumberOfShards(indexName);
        objectUnderTest.getIngestionTopic(indexName);

        verify(restClient, times(1)).performRequest(any());
    }

    @Test
    void invalidate_causes_refetch() throws IOException {
        mockSettingsResponse(buildSettingsResponse());
        final IndexShardProvider objectUnderTest = createObjectUnderTest();
        objectUnderTest.getNumberOfShards(indexName);

        objectUnderTest.invalidate(indexName);

        final int updatedShardCount = 10;
        final String updatedResponse = buildSettingsResponse().replace(
                "\"" + shardCount + "\"", "\"" + updatedShardCount + "\"");
        mockSettingsResponse(updatedResponse);

        assertThat(objectUnderTest.getNumberOfShards(indexName), equalTo(updatedShardCount));
        verify(restClient, times(2)).performRequest(any());
    }

    @Test
    void getNumberOfShards_throws_when_settings_missing() throws IOException {
        mockSettingsResponse("{\"" + indexName + "\": {\"settings\": {}}}");

        assertThrows(IllegalStateException.class, () -> createObjectUnderTest().getNumberOfShards(indexName));
    }

    @Test
    void getIngestionTopic_throws_when_ingestion_source_missing() throws IOException {
        final String noIngestionSource = "{\n" +
                "  \"" + indexName + "\": {\n" +
                "    \"settings\": {\n" +
                "      \"index\": {\n" +
                "        \"number_of_shards\": \"" + shardCount + "\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        mockSettingsResponse(noIngestionSource);

        assertThrows(IllegalStateException.class, () -> createObjectUnderTest().getIngestionTopic(indexName));
    }

    private void mockSettingsResponse(final String json) throws IOException {
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        when(response.getEntity()).thenReturn(httpEntity);
        when(restClient.performRequest(any())).thenReturn(response);
    }
}
