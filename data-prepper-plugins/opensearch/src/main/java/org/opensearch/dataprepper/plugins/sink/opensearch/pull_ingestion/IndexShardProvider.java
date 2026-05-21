/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IndexShardProvider {
    private static final Logger LOG = LoggerFactory.getLogger(IndexShardProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final RestHighLevelClient restHighLevelClient;
    private final Cache<String, IndexIngestionSettings> settingsCache;

    public IndexShardProvider(final RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
        this.settingsCache = Caffeine.newBuilder().build();
    }

    public int getNumberOfShards(final String indexName) throws IOException {
        return getSettings(indexName).numberOfShards();
    }

    public String getIngestionTopic(final String indexName) throws IOException {
        return getSettings(indexName).ingestionTopic();
    }

    public void invalidate(final String indexName) {
        settingsCache.invalidate(indexName);
    }

    private IndexIngestionSettings getSettings(final String indexName) throws IOException {
        final IndexIngestionSettings cached = settingsCache.getIfPresent(indexName);
        if (cached != null) {
            return cached;
        }

        final IndexIngestionSettings settings = fetchSettings(indexName);
        settingsCache.put(indexName, settings);
        LOG.info("Fetched settings for index '{}': shards={}, topic='{}'",
                indexName, settings.numberOfShards(), settings.ingestionTopic());
        return settings;
    }

    private IndexIngestionSettings fetchSettings(final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_settings");
        final Response response = restHighLevelClient.getLowLevelClient().performRequest(request);
        final JsonNode root = OBJECT_MAPPER.readTree(response.getEntity().getContent());

        final JsonNode indexNode = root.path(indexName).path("settings").path("index");
        if (indexNode.isMissingNode()) {
            throw new IllegalStateException("No settings found for index: " + indexName);
        }

        final JsonNode shardsNode = indexNode.path("number_of_shards");
        if (shardsNode.isMissingNode()) {
            throw new IllegalStateException("number_of_shards not found in settings for index: " + indexName);
        }

        final JsonNode topicNode = indexNode.path("ingestion_source").path("param").path("topic");
        if (topicNode.isMissingNode()) {
            throw new IllegalStateException("ingestion_source.param.topic not found in settings for index: " + indexName);
        }

        return new IndexIngestionSettings(Integer.parseInt(shardsNode.asText()), topicNode.asText());
    }

    static class IndexIngestionSettings {
        private final int numberOfShards;
        private final String ingestionTopic;

        IndexIngestionSettings(final int numberOfShards, final String ingestionTopic) {
            this.numberOfShards = numberOfShards;
            this.ingestionTopic = ingestionTopic;
        }

        int numberOfShards() {
            return numberOfShards;
        }

        String ingestionTopic() {
            return ingestionTopic;
        }
    }
}
