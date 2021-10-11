package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import com.amazon.dataprepper.plugins.sink.opensearch.index.ismpolicy.NoIsmPolicyManagement;
import org.opensearch.client.RestHighLevelClient;

public class TraceAnalyticsServiceMapIndexManager extends IndexManager {

    public TraceAnalyticsServiceMapIndexManager(final RestHighLevelClient restHighLevelClient,
                                                final OpenSearchSinkConfiguration openSearchSinkConfiguration) {
        super(restHighLevelClient, openSearchSinkConfiguration);
        this.ismPolicyManagementStrategy = new NoIsmPolicyManagement(restHighLevelClient);
    }

}
