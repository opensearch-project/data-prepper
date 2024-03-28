package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;

import java.io.IOException;
import java.util.function.Supplier;

public class OpenSearchDefaultBulkApiWrapper implements BulkApiWrapper {
    private final Supplier<OpenSearchClient> openSearchClientSupplier;

    public OpenSearchDefaultBulkApiWrapper(final Supplier<OpenSearchClient> openSearchClientSupplier) {
        this.openSearchClientSupplier = openSearchClientSupplier;
    }

    @Override
    public BulkResponse bulk(BulkRequest request) throws IOException {
        return openSearchClientSupplier.get().bulk(request);
    }
}
