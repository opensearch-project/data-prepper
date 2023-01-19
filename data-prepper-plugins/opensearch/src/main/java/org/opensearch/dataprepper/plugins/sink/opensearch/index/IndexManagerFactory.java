/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3ClientProvider;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.Optional;

public class IndexManagerFactory {
    private static final String S3_PREFIX = "s3://";

    public final AbstractIndexManager getIndexManager(final IndexType indexType,
                                        final RestHighLevelClient restHighLevelClient,
                                        final OpenSearchSinkConfiguration openSearchSinkConfiguration) {
        try {
            return (AbstractIndexManager) getIndexManager(indexType, restHighLevelClient, openSearchSinkConfiguration, null);
        } catch (IOException e) {
            return null;
        }
    }

    public final IndexManager getIndexManager(final IndexType indexType,
                                        final RestHighLevelClient restHighLevelClient,
                                        final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                        String indexAlias) throws IOException {
        if (indexAlias != null && isDynamicIndexAlias(indexAlias)) {
            return  new DynamicIndexManager(indexType, restHighLevelClient, openSearchSinkConfiguration, this);
        }

        IndexManager indexManager;
        switch (indexType) {
            case TRACE_ANALYTICS_RAW:
                indexManager = new TraceAnalyticsRawIndexManager(restHighLevelClient, openSearchSinkConfiguration, indexAlias);
                break;
            case TRACE_ANALYTICS_SERVICE_MAP:
                indexManager = new TraceAnalyticsServiceMapIndexManager(restHighLevelClient, openSearchSinkConfiguration, indexAlias);
                break;
            case MANAGEMENT_DISABLED:
                indexManager = new ManagementDisabledIndexManager(restHighLevelClient, openSearchSinkConfiguration, indexAlias);
                break;
            default:
                indexManager = new DefaultIndexManager(restHighLevelClient, openSearchSinkConfiguration, indexAlias);
                break;
        }
        return indexManager;
    }

    private boolean isDynamicIndexAlias(final String indexAlias) {
      return indexAlias.indexOf("${") != -1;
    }

    private static class DefaultIndexManager extends AbstractIndexManager {

        private static final String POLICY_NAME_SUFFIX = "-policy";

        public DefaultIndexManager(final RestHighLevelClient restHighLevelClient,
                                   final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                   final String indexAlias) {
            super(restHighLevelClient, openSearchSinkConfiguration, indexAlias);
            final Optional<String> ismPolicyFile = openSearchSinkConfiguration.getIndexConfiguration().getIsmPolicyFile();
            if (ismPolicyFile.isPresent()) {
                S3Client s3Client = null;
                if (ismPolicyFile.get().startsWith(S3_PREFIX)) {
                    final String s3AwsRegion = openSearchSinkConfiguration.getIndexConfiguration().getS3AwsRegion();
                    final String s3AwsStsRoleArn = openSearchSinkConfiguration.getIndexConfiguration().getS3AwsStsRoleArn();
                    final S3ClientProvider clientProvider = new S3ClientProvider(s3AwsRegion, s3AwsStsRoleArn);
                    s3Client = clientProvider.buildS3Client();
                }
                final String indexPolicyName = getIndexPolicyName();
                this.ismPolicyManagementStrategy = new IsmPolicyManagement(restHighLevelClient, indexPolicyName, ismPolicyFile.get(), s3Client);
            } else {
                //Policy file doesn't exist
                this.ismPolicyManagementStrategy = new NoIsmPolicyManagement(restHighLevelClient);
            }
        }

        private String getIndexPolicyName() {
            //If index prefix has a ending dash, then remove it to avoid two consecutive dashes.
            return indexPrefix.replaceAll("-$", "") + POLICY_NAME_SUFFIX;
        }
    }

    private static class TraceAnalyticsRawIndexManager extends AbstractIndexManager {
        public TraceAnalyticsRawIndexManager(final RestHighLevelClient restHighLevelClient,
                                             final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                                    final String indexAlias) {
            super(restHighLevelClient, openSearchSinkConfiguration, indexAlias);
            this.ismPolicyManagementStrategy = new IsmPolicyManagement(
                    restHighLevelClient,
                    IndexConstants.RAW_ISM_POLICY,
                    IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE,
                    IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE);
        }

    }

    private static class TraceAnalyticsServiceMapIndexManager extends AbstractIndexManager {

        public TraceAnalyticsServiceMapIndexManager(final RestHighLevelClient restHighLevelClient,
                                                    final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                                    final String indexAlias) {
            super(restHighLevelClient, openSearchSinkConfiguration, indexAlias);
            this.ismPolicyManagementStrategy = new NoIsmPolicyManagement(restHighLevelClient);
        }
    }

    private class ManagementDisabledIndexManager extends AbstractIndexManager {
        protected ManagementDisabledIndexManager(final RestHighLevelClient restHighLevelClient, final OpenSearchSinkConfiguration openSearchSinkConfiguration, final String indexAlias) {
            super(restHighLevelClient, openSearchSinkConfiguration, indexAlias);
        }
        @Override
        public void setupIndex() throws IOException {
        }


    }
}
