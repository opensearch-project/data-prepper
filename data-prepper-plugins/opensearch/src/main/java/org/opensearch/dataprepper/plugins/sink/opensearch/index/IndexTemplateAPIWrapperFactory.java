package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;

public class IndexTemplateAPIWrapperFactory {
    public static IndexTemplateAPIWrapper getWrapper(final IndexConfiguration indexConfiguration,
                                                     final OpenSearchClient openSearchClient) {
        if (DistributionVersion.ES6.equals(indexConfiguration.getDistributionVersion())) {
            return new Es6IndexTemplateAPIWrapper(openSearchClient);
        } else if (TemplateType.V1.equals(indexConfiguration.getTemplateType())) {
            return new OpenSearchLegacyTemplateAPIWrapper(openSearchClient);
        } else {
            return new ComposableTemplateAPIWrapper(openSearchClient);
        }
    }
}
