package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Optional;

public class DefaultIndexManager extends IndexManager {

    public DefaultIndexManager(final RestHighLevelClient restHighLevelClient,
                               final OpenSearchSinkConfiguration openSearchSinkConfiguration) {
        super(restHighLevelClient, openSearchSinkConfiguration);
    }

    @Override
    public Optional<String> checkAndCreatePolicy() throws IOException {
        return Optional.empty();
    }

}
