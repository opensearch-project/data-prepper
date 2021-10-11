package com.amazon.dataprepper.plugins.sink.opensearch.index.ismpolicy;

import org.opensearch.client.indices.CreateIndexRequest;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface IsmPolicyManagementStrategy {

    Optional<String> checkAndCreatePolicy() throws IOException;

    List<String> getIndexPatterns(final String indexAlias);

    boolean checkIfIndexExistsOnServer(final String indexAlias) throws IOException;

    CreateIndexRequest getCreateIndexRequest(final String indexAlias);
}
