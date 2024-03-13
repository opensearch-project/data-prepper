package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import org.opensearch.client.util.ApiTypeHelper;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Es6BulkApiWrapper implements BulkApiWrapper {
    private final Supplier<OpenSearchClient> openSearchClientSupplier;

    public Es6BulkApiWrapper(final Supplier<OpenSearchClient> openSearchClientSupplier) {
        this.openSearchClientSupplier = openSearchClientSupplier;
    }

    @Override
    public BulkResponse bulk(BulkRequest request) throws IOException, OpenSearchException {
        final JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse> endpoint = es6BulkEndpoint(request);
        final OpenSearchClient openSearchClient = openSearchClientSupplier.get();
        return openSearchClient._transport().performRequest(request, endpoint, openSearchClient._transportOptions());
    }

    private JsonEndpoint<BulkRequest, BulkResponse, ErrorResponse> es6BulkEndpoint(final BulkRequest bulkRequest) {
        return new SimpleEndpoint<>(
                // Request method
                request -> HttpMethod.POST,

                // Request path
                request -> {
                    final String index = request.index() == null ? getFirstOperationIndex(request) : request.index();
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

    private String getFirstOperationIndex(final BulkRequest bulkRequest) {
        final BulkOperation firstBulkOperation = bulkRequest.operations().get(0);
        if (firstBulkOperation.isIndex()) {
            return firstBulkOperation.index().index();
        } else if (firstBulkOperation.isCreate()) {
            return firstBulkOperation.create().index();
        } else if (firstBulkOperation.isUpdate()) {
            return firstBulkOperation.update().index();
        } else if (firstBulkOperation.isDelete()) {
            return firstBulkOperation.delete().index();
        }
        throw new IllegalArgumentException(String.format("Unsupported bulk operation kind: %s",
                firstBulkOperation._kind()));
    }
}
