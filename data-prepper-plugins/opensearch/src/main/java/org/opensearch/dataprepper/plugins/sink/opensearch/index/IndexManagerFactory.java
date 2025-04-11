/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3ClientProvider;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.Optional;

public class IndexManagerFactory {
    private static final String S3_PREFIX = "s3://";

    private final ClusterSettingsParser clusterSettingsParser;

    public IndexManagerFactory(final ClusterSettingsParser clusterSettingsParser) {
        this.clusterSettingsParser = clusterSettingsParser;
    }

    public final AbstractIndexManager getIndexManager(final IndexType indexType,
                                                      final OpenSearchClient openSearchClient,
                                                      final RestHighLevelClient restHighLevelClient,
                                                      final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                                      final TemplateStrategy templateStrategy) {
        try {
            return (AbstractIndexManager) getIndexManager(
                    indexType, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy, null);
        } catch (IOException e) {
            return null;
        }
    }

    public final IndexManager getIndexManager(final IndexType indexType,
                                              final OpenSearchClient openSearchClient,
                                              final RestHighLevelClient restHighLevelClient,
                                              final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                              final TemplateStrategy templateStrategy,
                                              final String indexAlias) throws IOException {
        if (indexAlias != null && isDynamicIndexAlias(indexAlias)) {
            return  new DynamicIndexManager(
                    indexType, openSearchClient, restHighLevelClient, openSearchSinkConfiguration,
                    clusterSettingsParser, templateStrategy, this);
        }

        IndexManager indexManager;
        switch (indexType) {
            case TRACE_ANALYTICS_RAW:
            case TRACE_ANALYTICS_RAW_PLAIN:
                indexManager = new TraceAnalyticsRawIndexManager(
                        restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
                break;
            case TRACE_ANALYTICS_SERVICE_MAP:
                indexManager = new TraceAnalyticsServiceMapIndexManager(
                        restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
                break;
            case LOG_ANALYTICS:
            case LOG_ANALYTICS_PLAIN:
                indexManager = new LogAnalyticsIndexManager(
                        restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
                break;
            case METRIC_ANALYTICS:
            case METRIC_ANALYTICS_PLAIN:
                indexManager = new MetricAnalyticsIndexManager(
                        restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
                break;
            case MANAGEMENT_DISABLED:
                indexManager = new ManagementDisabledIndexManager(
                        restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
                break;
            default:
                indexManager = new DefaultIndexManager(
                        restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
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
                                   final OpenSearchClient openSearchClient,
                                   final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                   final ClusterSettingsParser clusterSettingsParser,
                                   final TemplateStrategy templateStrategy,
                                   final String indexAlias) {
            super(restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
            final Optional<String> ismPolicyFile = openSearchSinkConfiguration.getIndexConfiguration().getIsmPolicyFile();
            if (ismPolicyFile.isPresent()) {
                S3Client s3Client = null;
                if (ismPolicyFile.get().startsWith(S3_PREFIX)) {
                    final String s3AwsRegion = openSearchSinkConfiguration.getIndexConfiguration().getS3AwsRegion();
                    final String s3AwsStsRoleArn = openSearchSinkConfiguration.getIndexConfiguration().getS3AwsStsRoleArn();
                    final String s3AwsStsExternalId = openSearchSinkConfiguration.getIndexConfiguration().getS3AwsStsExternalId();
                    final S3ClientProvider clientProvider = new S3ClientProvider(s3AwsRegion, s3AwsStsRoleArn, s3AwsStsExternalId);
                    s3Client = clientProvider.buildS3Client();
                }
                final String indexPolicyName = getIndexPolicyName();
                this.ismPolicyManagementStrategy = new IsmPolicyManagement(
                        openSearchClient, restHighLevelClient, indexPolicyName, ismPolicyFile.get(), s3Client);
            } else {
                //Policy file doesn't exist
                this.ismPolicyManagementStrategy = new NoIsmPolicyManagement(openSearchClient, restHighLevelClient);
            }
        }

        private String getIndexPolicyName() {
            return indexPrefix + POLICY_NAME_SUFFIX;
        }
    }

    private static class TraceAnalyticsRawIndexManager extends AbstractIndexManager {
        public TraceAnalyticsRawIndexManager(final RestHighLevelClient restHighLevelClient,
                                             final OpenSearchClient openSearchClient,
                                             final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                             final ClusterSettingsParser clusterSettingsParser,
                                             final TemplateStrategy templateStrategy,
                                             final String indexAlias) {
            super(restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
            this.ismPolicyManagementStrategy = new IsmPolicyManagement(
                    openSearchClient,
                    restHighLevelClient,
                    IndexConstants.RAW_ISM_POLICY,
                    IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE,
                    IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE);
        }

    }

    private static class TraceAnalyticsServiceMapIndexManager extends AbstractIndexManager {

        public TraceAnalyticsServiceMapIndexManager(final RestHighLevelClient restHighLevelClient,
                                                    final OpenSearchClient openSearchClient,
                                                    final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                                    final ClusterSettingsParser clusterSettingsParser,
                                                    final TemplateStrategy templateStrategy,
                                                    final String indexAlias) {
            super(restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
            this.ismPolicyManagementStrategy = new NoIsmPolicyManagement(openSearchClient, restHighLevelClient);
        }
    }

    private static class LogAnalyticsIndexManager extends AbstractIndexManager {

        public LogAnalyticsIndexManager(final RestHighLevelClient restHighLevelClient,
                                                    final OpenSearchClient openSearchClient,
                                                    final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                                    final ClusterSettingsParser clusterSettingsParser,
                                                    final TemplateStrategy templateStrategy,
                                                    final String indexAlias) {
            super(restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
            this.ismPolicyManagementStrategy = new IsmPolicyManagement(
                    openSearchClient,
                    restHighLevelClient,
                    IndexConstants.LOGS_ISM_POLICY,
                    IndexConstants.LOGS_ISM_FILE_WITH_ISM_TEMPLATE,
                    IndexConstants.LOGS_ISM_FILE_NO_ISM_TEMPLATE);
        }
    }

    private static class MetricAnalyticsIndexManager extends AbstractIndexManager {

        public MetricAnalyticsIndexManager(final RestHighLevelClient restHighLevelClient,
                                                    final OpenSearchClient openSearchClient,
                                                    final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                                    final ClusterSettingsParser clusterSettingsParser,
                                                    final TemplateStrategy templateStrategy,
                                                    final String indexAlias) {
            super(restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
            this.ismPolicyManagementStrategy = new IsmPolicyManagement(
                    openSearchClient,
                    restHighLevelClient,
                    IndexConstants.METRICS_ISM_POLICY,
                    IndexConstants.METRICS_ISM_FILE_WITH_ISM_TEMPLATE,
                    IndexConstants.METRICS_ISM_FILE_NO_ISM_TEMPLATE);
        }
    }

    private class ManagementDisabledIndexManager extends AbstractIndexManager {
        protected ManagementDisabledIndexManager(final RestHighLevelClient restHighLevelClient,
                                                 final OpenSearchClient openSearchClient,
                                                 final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                                                 final ClusterSettingsParser clusterSettingsParser,
                                                 final TemplateStrategy templateStrategy,
                                                 final String indexAlias) {
            super(restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, indexAlias);
        }
        @Override
        public void setupIndex() {
        }

    }
}
