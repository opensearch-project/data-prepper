package com.amazon.dataprepper.plugins.sink.opensearch.index;

import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class NoIsmPolicyManagement implements IsmPolicyManagementStrategy {
    private final RestHighLevelClient restHighLevelClient;

    public NoIsmPolicyManagement(final RestHighLevelClient restHighLevelClient) {
        checkNotNull(restHighLevelClient);
        this.restHighLevelClient = restHighLevelClient;
    }

    @Override
    public Optional<String> checkAndCreatePolicy() throws IOException {
        return Optional.empty();
    }

    @Override
    public List<String> getIndexPatterns(final String indexAlias) {
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        return Collections.singletonList(indexAlias);
    }

    @Override
    public boolean checkIfIndexExistsOnServer(final String indexAlias) throws IOException {
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        return restHighLevelClient.indices().exists(new GetIndexRequest(indexAlias), RequestOptions.DEFAULT);
    }

    @Override
    public CreateIndexRequest getCreateIndexRequest(final String indexAlias) {
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        final String initialIndexName = indexAlias;
        return new CreateIndexRequest(initialIndexName);
    }
}
