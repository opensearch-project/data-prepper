package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;

import java.io.IOException;

public class OpenSearchDefaultBulkApiWrapper implements BulkApiWrapper {
    private final OpenSearchClient openSearchClient;

    public OpenSearchDefaultBulkApiWrapper(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    @Override
    public BulkResponse bulk(BulkRequest request) throws IOException {
        return openSearchClient.bulk(request);
    }
}
