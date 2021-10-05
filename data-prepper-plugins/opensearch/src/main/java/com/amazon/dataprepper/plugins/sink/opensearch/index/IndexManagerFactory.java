package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.client.RestHighLevelClient;

public class IndexManagerFactory {

    public final IndexManager getIndexManager(final IndexType indexType,
                                        final RestHighLevelClient restHighLevelClient,
                                        final OpenSearchSinkConfiguration openSearchSinkConfiguration) {
        switch (indexType) {
            case TRACE_ANALYTICS_RAW:
                return new TraceAnalyticsRawIndexManager(restHighLevelClient, openSearchSinkConfiguration);
            case TRACE_ANALYTICS_SERVICE_MAP:
                return new TraceAnalyticsServiceMapIndexManager(restHighLevelClient, openSearchSinkConfiguration);
            default:
                return new DefaultIndexManager(restHighLevelClient, openSearchSinkConfiguration);
        }
    }

}
