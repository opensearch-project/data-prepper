package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TraceAnalyticsRawIndexManager extends IndexManager {

    public TraceAnalyticsRawIndexManager(final RestHighLevelClient restHighLevelClient,
                                         final OpenSearchSinkConfiguration openSearchSinkConfiguration) {
        super(restHighLevelClient, openSearchSinkConfiguration);
    }

    @Override
    protected List<String> getIndexPatterns(final String indexAlias){
        return  Collections.singletonList(indexAlias + "-*");
    }

    @Override
    public Optional<String> checkAndCreatePolicy() throws IOException {
             // TODO: replace with new _opensearch API
            final String endPoint = "/_opendistro/_ism/policies/" + IndexConstants.RAW_ISM_POLICY;
            Request request = createPolicyRequestFromFile(endPoint, IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE);
            try {
                restHighLevelClient.getLowLevelClient().performRequest(request);
            } catch (ResponseException e1) {
                final String msg = e1.getMessage();
                if (msg.contains("Invalid field: [ism_template]")) {
                    request = createPolicyRequestFromFile(endPoint, IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE);
                    try {
                        restHighLevelClient.getLowLevelClient().performRequest(request);
                    } catch (ResponseException e2) {
                        if (e2.getMessage().contains("version_conflict_engine_exception")
                                || e2.getMessage().contains("resource_already_exists_exception")) {
                            // Do nothing - likely caused by
                            // (1) a race condition where the resource was created by another host before this host's
                            // restClient made its request;
                            // (2) policy already exists in the cluster
                        } else {
                            throw e2;
                        }
                    }
                    return Optional.of(IndexConstants.RAW_ISM_POLICY);
                } else if (e1.getMessage().contains("version_conflict_engine_exception")
                        || e1.getMessage().contains("resource_already_exists_exception")) {
                    // Do nothing - likely caused by
                    // (1) a race condition where the resource was created by another host before this host's
                    // restClient made its request;
                    // (2) policy already exists in the cluster
                } else {
                    throw e1;
                }
            }

        return Optional.empty();
    }

    @Override
    protected boolean checkIfIndexExistsOnServer(final String indexAlias) throws IOException {
        return restHighLevelClient.indices().existsAlias(new GetAliasesRequest().aliases(indexAlias), RequestOptions.DEFAULT);
    }

    @Override
    protected CreateIndexRequest getCreateIndexRequest(final String indexAlias) {
        final String initialIndexName = indexAlias + "-000001";
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest(initialIndexName);
        createIndexRequest.alias(new Alias(indexAlias).writeIndex(true));
        return createIndexRequest;
    }
}
