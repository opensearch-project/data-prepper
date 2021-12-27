/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.client.RestHighLevelClient;

import java.util.Optional;

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

    private static class DefaultIndexManager extends IndexManager {

        public static final String POLICY_NAME_SUFFIX = "-policy";

        public DefaultIndexManager(final RestHighLevelClient restHighLevelClient,
                                   final OpenSearchSinkConfiguration openSearchSinkConfiguration) {
            super(restHighLevelClient, openSearchSinkConfiguration);
            final Optional<String> ismPolicyFile = openSearchSinkConfiguration.getIndexConfiguration().getIsmPolicyFile();
            if (ismPolicyFile.isPresent()) {
                final String indexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
                final String indexPolicyName = indexAlias + POLICY_NAME_SUFFIX;
                this.ismPolicyManagementStrategy = new IsmPolicyManagement(restHighLevelClient, indexPolicyName, ismPolicyFile.get());
            } else {
                //Policy file doesn't exist
                this.ismPolicyManagementStrategy = new NoIsmPolicyManagement(restHighLevelClient);
            }
        }

    }

    private static class TraceAnalyticsRawIndexManager extends IndexManager {
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

    private static class TraceAnalyticsServiceMapIndexManager extends IndexManager {

        public TraceAnalyticsServiceMapIndexManager(final RestHighLevelClient restHighLevelClient,
                                                    final OpenSearchSinkConfiguration openSearchSinkConfiguration) {
            super(restHighLevelClient, openSearchSinkConfiguration);
            this.ismPolicyManagementStrategy = new NoIsmPolicyManagement(restHighLevelClient);
        }

    }
}
