package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import org.opensearch.client.util.ApiTypeHelper;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class BulkAPIWrapper {
    static final String DUMMY_DEFAULT_INDEX = "dummy";
    private final IndexConfiguration indexConfiguration;
    private final OpenSearchClient openSearchClient;

    public BulkAPIWrapper(final IndexConfiguration indexConfiguration,
                                final OpenSearchClient openSearchClient) {
        this.indexConfiguration = indexConfiguration;
        this.openSearchClient = openSearchClient;
    }

    public BulkResponse bulk(BulkRequest request) throws IOException, OpenSearchException {
        if (indexConfiguration.isEs6()) {
            final JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse> endpoint = es6BulkEndpoint(request);
            return openSearchClient._transport().performRequest(request, endpoint, openSearchClient._transportOptions());
        } else {
            return openSearchClient.bulk(request);
        }
    }

    private JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse> es6BulkEndpoint(BulkRequest bulkRequest) {
        return new SimpleEndpoint<>(
                // Request method
                request -> HttpMethod.POST,

                // Request path
                request -> {
                    final String index = request.index() == null ? DUMMY_DEFAULT_INDEX : request.index();
                    StringBuilder buf = new StringBuilder();
                    buf.append("/");
                    SimpleEndpoint.pathEncode(index, buf);
                    buf.append("/_doc");
                    buf.append("/_bulk");
                    return buf.toString();
                },

                // Request parameters
                request -> {
                    Map<String, String> params = new HashMap<>();
                    if (request.pipeline() != null) {
                        params.put("pipeline", request.pipeline());
                    }
                    if (request.routing() != null) {
                        params.put("routing", request.routing());
                    }
                    if (request.requireAlias() != null) {
                        params.put("require_alias", String.valueOf(request.requireAlias()));
                    }
                    if (request.refresh() != null) {
                        params.put("refresh", request.refresh().jsonValue());
                    }
                    if (request.waitForActiveShards() != null) {
                        params.put("wait_for_active_shards", request.waitForActiveShards()._toJsonString());
                    }
                    if (request.source() != null) {
                        params.put("_source", request.source()._toJsonString());
                    }
                    if (ApiTypeHelper.isDefined(request.sourceExcludes())) {
                        params.put("_source_excludes",
                                request.sourceExcludes().stream().map(v -> v).collect(Collectors.joining(",")));
                    }
                    if (ApiTypeHelper.isDefined(request.sourceIncludes())) {
                        params.put("_source_includes",
                                request.sourceIncludes().stream().map(v -> v).collect(Collectors.joining(",")));
                    }
                    if (request.timeout() != null) {
                        params.put("timeout", request.timeout()._toJsonString());
                    }
                    return params;

                }, SimpleEndpoint.emptyMap(), true, BulkResponse._DESERIALIZER);
    }
}
