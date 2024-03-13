package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;

import java.util.function.Supplier;

public class BulkApiWrapperFactory {
    public static BulkApiWrapper getWrapper(final IndexConfiguration indexConfiguration,
                                            final Supplier<OpenSearchClient> openSearchClientSupplier) {
        if (DistributionVersion.ES6.equals(indexConfiguration.getDistributionVersion())) {
            return new Es6BulkApiWrapper(openSearchClientSupplier);
        } else {
            return new OpenSearchDefaultBulkApiWrapper(openSearchClientSupplier);
        }
    }
}
