/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.endpoints.BooleanResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.AbstractIndexManager.TIME_PATTERN;

class NoIsmPolicyManagement implements IsmPolicyManagementStrategy {
    private final RestHighLevelClient restHighLevelClient;
    private final OpenSearchClient openSearchClient;

    public NoIsmPolicyManagement(final OpenSearchClient openSearchClient,
                                 final RestHighLevelClient restHighLevelClient) {
        checkNotNull(restHighLevelClient);
        checkNotNull(openSearchClient);
        this.openSearchClient = openSearchClient;
        this.restHighLevelClient = restHighLevelClient;
    }

    @Override
    public Optional<String> checkAndCreatePolicy(final String indexAlias) throws IOException {
        return Optional.empty();
    }

    @Override
    public List<String> getIndexPatterns(final String indexAlias) {
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        return Collections.singletonList(TIME_PATTERN.matcher(indexAlias).replaceAll("*"));
    }

    @Override
    public boolean checkIfIndexExistsOnServer(final String indexAlias) throws IOException {
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        final BooleanResponse booleanResponse = openSearchClient.indices().exists(
                new ExistsRequest.Builder().index(indexAlias).build());
        return booleanResponse.value();
    }

    @Override
    public CreateIndexRequest getCreateIndexRequest(final String indexAlias) {
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        final String initialIndexName = indexAlias;
        return new CreateIndexRequest.Builder()
                .index(initialIndexName)
                .build();
    }
}
