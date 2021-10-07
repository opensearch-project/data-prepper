package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import com.amazon.dataprepper.plugins.sink.opensearch.index.ismpolicy.IsmPolicyManagement;
import org.opensearch.client.RestHighLevelClient;

public class TraceAnalyticsRawIndexManager extends IndexManager {
    public TraceAnalyticsRawIndexManager(final RestHighLevelClient restHighLevelClient,
                                         final OpenSearchSinkConfiguration openSearchSinkConfiguration) {
        super(restHighLevelClient, openSearchSinkConfiguration);
        this.ismPolicyManagementStrategy = new IsmPolicyManagement(
                restHighLevelClient,
                IndexConstants.RAW_ISM_POLICY,
                IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE,
                IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE);
    }

}
