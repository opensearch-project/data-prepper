/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.plugins.source.opensearch.ClientRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DistributionVersion;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * SearchAccessorStrategy determines which {@link SearchAccessor} (Elasticsearch or OpenSearch) should be used based on
 * the {@link OpenSearchSourceConfiguration}.
 * @since 2.4
 */
public class SearchAccessorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SearchAccessorStrategy.class);

    static final String OPENSEARCH_DISTRIBUTION = "opensearch";
    static final String ELASTICSEARCH_DISTRIBUTION = "elasticsearch";
    static final String ELASTICSEARCH_OSS_BUILD_FLAVOR = "oss";

    private static final String OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF = "2.5.0";
    private static final String ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF = "7.10.0";


    private final OpenSearchClientFactory openSearchClientFactory;
    private final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final PluginConfigObservable pluginConfigObservable;

    public static SearchAccessorStrategy create(final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics,
                                                final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                final OpenSearchClientFactory openSearchClientFactory,
                                                final PluginConfigObservable pluginConfigObservable) {
        return new SearchAccessorStrategy(
                openSearchSourcePluginMetrics, openSearchSourceConfiguration, openSearchClientFactory, pluginConfigObservable);
    }

    private SearchAccessorStrategy(final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics,
                                   final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                   final OpenSearchClientFactory openSearchClientFactory,
                                   final PluginConfigObservable pluginConfigObservable) {
        this.openSearchSourcePluginMetrics = openSearchSourcePluginMetrics;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.openSearchClientFactory = openSearchClientFactory;
        this.pluginConfigObservable = pluginConfigObservable;
    }

    /**
     * Provides a {@link SearchAccessor} that is based on the {@link OpenSearchSourceConfiguration}
     * @return a {@link SearchAccessor}
     * @since 2.4
     */
    public SearchAccessor getSearchAccessor() {

        final PluginComponentRefresher<OpenSearchClient, OpenSearchSourceConfiguration> clientRefresher =
                new ClientRefresher<>(openSearchSourcePluginMetrics,
                        OpenSearchClient.class, openSearchClientFactory::provideOpenSearchClient,
                        openSearchSourceConfiguration);

        if (Objects.nonNull(openSearchSourceConfiguration.getAwsAuthenticationOptions()) &&
                openSearchSourceConfiguration.getAwsAuthenticationOptions().isServerlessCollection()) {
            return createSearchAccessorForServerlessCollection(clientRefresher);
        }

        InfoResponse infoResponse = null;

        PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> elasticsearchClientRefresher = null;
        
        try {
            infoResponse = clientRefresher.get().info();
            pluginConfigObservable.addPluginConfigObserver(newConfig -> clientRefresher.update(
                    (OpenSearchSourceConfiguration) newConfig));
        } catch (final Exception e) {

            if (DistributionVersion.OPENSEARCH.equals(openSearchSourceConfiguration.getDistributionVersion())) {
                LOG.info("distribution_version is opensearch. Forcing creation of OpenSearch client...");
                return new OpenSearchAccessor(clientRefresher,
                        openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType() != null ?
                                openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType() :
                                SearchContextType.SCROLL);
            }

            LOG.info("Detected Elasticsearch cluster. Constructing Elasticsearch client");

            try {
                elasticsearchClientRefresher = new ClientRefresher<>(openSearchSourcePluginMetrics,
                        ElasticsearchClient.class, openSearchClientFactory::provideElasticSearchClient,
                        openSearchSourceConfiguration);
                final PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration>
                        finalElasticsearchClientRefresher = elasticsearchClientRefresher;
                pluginConfigObservable.addPluginConfigObserver(
                        newConfig -> finalElasticsearchClientRefresher.update((OpenSearchSourceConfiguration) newConfig));
            } catch (final Exception ex) {
                throw new RuntimeException("There was an error looking up the OpenSearch cluster info: ", ex);
            }
        }

        final Pair<String, String> distributionAndVersion = getDistributionAndVersionNumber(infoResponse, elasticsearchClientRefresher);

        final String distribution = distributionAndVersion.getLeft();
        final String versionNumber = distributionAndVersion.getRight();

        validateDistribution(distribution);

        SearchContextType searchContextType;

        if (Objects.nonNull(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType())) {
            LOG.info("Using search_context_type set in the config: '{}'", openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType().toString().toLowerCase());
            validateSearchContextTypeOverride(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType(), distribution, versionNumber);
            searchContextType = openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType();
        } else if (versionSupportsPointInTime(distribution, versionNumber)) {
            LOG.info("{} distribution and version {} detected. Point in time APIs will be used to search documents", distribution, versionNumber);
            searchContextType = SearchContextType.POINT_IN_TIME;
        } else {
            LOG.info("{} distribution, version {} detected. Scroll contexts will be used to search documents. " +
                    "Upgrade your cluster to at least OpenSearch {} to use Point in Time APIs instead of scroll.", distribution, versionNumber,
                    OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
            searchContextType = SearchContextType.SCROLL;
        }

        if (OPENSEARCH_DISTRIBUTION.equals(distribution)) {
            LOG.debug("Using OpenSearchAccessor as the determined distribution is OpenSearch.");
            return new OpenSearchAccessor(clientRefresher, searchContextType);
        } else { 
            LOG.debug("Using ElasticsearchAccessor as the determined distribution is Elasticsearch.");
            PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> currentElasticsearchClientRefresher = elasticsearchClientRefresher;
            if (Objects.isNull(currentElasticsearchClientRefresher)) {
                LOG.info("Elasticsearch distribution determined, but no existing Elasticsearch client refresher found (e.g., OpenSearch client reported Elasticsearch, or distribution_version was set to elasticsearch). Creating a new Elasticsearch client refresher.");
                try {
                    currentElasticsearchClientRefresher = new ClientRefresher<>(openSearchSourcePluginMetrics,
                            ElasticsearchClient.class, openSearchClientFactory::provideElasticSearchClient,
                            openSearchSourceConfiguration);
                    final PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration>
                            finalNewElasticsearchClientRefresher = currentElasticsearchClientRefresher;
                    pluginConfigObservable.addPluginConfigObserver(
                            newConfig -> {
                                if (finalNewElasticsearchClientRefresher != null) {
                                    finalNewElasticsearchClientRefresher.update((OpenSearchSourceConfiguration) newConfig);
                                }
                            });
                } catch (final Exception e) {
                    LOG.error("Failed to create Elasticsearch client for an Elasticsearch distribution when one was not pre-existing.", e);
                    throw new RuntimeException("Failed to create Elasticsearch client for an Elasticsearch distribution: " + e.getMessage(), e);
                }
            }
            return new ElasticsearchAccessor(currentElasticsearchClientRefresher, searchContextType);
        }
    }

    private SearchAccessor createSearchAccessorForServerlessCollection(final PluginComponentRefresher clientRefresher) {
        if (Objects.isNull(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType())) {
            LOG.info("Configured with AOS serverless flag as true, defaulting to search_context_type as 'none', which uses search_after");
            return new OpenSearchAccessor(clientRefresher,
                    SearchContextType.NONE);
        } else {
            if (SearchContextType.POINT_IN_TIME.equals(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType()) ||
                SearchContextType.SCROLL.equals(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType())) {
                throw new InvalidPluginConfigurationException("A search_context_type of point_in_time or scroll is not supported for serverless collections");
            }

            LOG.info("Using search_context_type set in the config: '{}'", openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType().toString().toLowerCase());
            return new OpenSearchAccessor(clientRefresher,
                    openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType());
        }
    }

    private void validateSearchContextTypeOverride(final SearchContextType searchContextType, final String distribution, final String version) {

        if (searchContextType.equals(SearchContextType.POINT_IN_TIME) && !versionSupportsPointInTime(distribution, version)) {
            throw new IllegalArgumentException(
                    String.format("A search_context_type of point_in_time is only supported on OpenSearch versions %s and above. " +
                    "The version of the OpenSearch cluster passed is %s. Elasticsearch clusters with build-flavor %s do not support point in time",
                            distribution.startsWith(ELASTICSEARCH_DISTRIBUTION) ? ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF : OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF,
                            version, ELASTICSEARCH_OSS_BUILD_FLAVOR));
        }
    }

    private boolean versionSupportsPointInTime(final String distribution, final String version) {
        final DefaultArtifactVersion actualVersion = new DefaultArtifactVersion(version);

        DefaultArtifactVersion cutoffVersion;
        if (distribution.startsWith(ELASTICSEARCH_DISTRIBUTION)) {
            if (distribution.endsWith(ELASTICSEARCH_OSS_BUILD_FLAVOR)) {
                return false;
            }
            cutoffVersion = new DefaultArtifactVersion(ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
        } else {
            cutoffVersion = new DefaultArtifactVersion(OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
        }
        return actualVersion.compareTo(cutoffVersion) >= 0;
    }

    private Pair<String, String> getDistributionAndVersionNumber(final InfoResponse infoResponseOpenSearch,
                                                                 final PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> elasticsearchClientRefresher) {
        if (Objects.nonNull(infoResponseOpenSearch)) {
            final String distribution = infoResponseOpenSearch.version().distribution();
            final String versionNumber = infoResponseOpenSearch.version().number();
            if (Objects.nonNull(distribution) && Objects.nonNull(versionNumber)) {
                LOG.info("Detected OpenSearch distribution '{}' version '{}' from API response.", distribution, versionNumber);
                return Pair.of(distribution, versionNumber);
            }
            LOG.warn("Distribution or version number is null in OpenSearch API response. Proceeding to check Elasticsearch or default.");
        }

        if (elasticsearchClientRefresher != null) {
            try {
                final ElasticsearchClient elasticsearchClient = elasticsearchClientRefresher.get();
                if (elasticsearchClient == null) {
                    LOG.info("ElasticsearchClient from refresher is null. Cannot attempt Elasticsearch .info() call. Proceeding to default.");
                } else {
                    final co.elastic.clients.elasticsearch.core.InfoResponse infoResponseElasticsearch = elasticsearchClient.info();
                    final String esBuildFlavor = infoResponseElasticsearch.version().buildFlavor();
                    final String esVersionNumber = infoResponseElasticsearch.version().number();

                    if (Objects.nonNull(esVersionNumber)) {
                        String effectiveDistribution = ELASTICSEARCH_DISTRIBUTION;
                        LOG.info("Detected Elasticsearch (flavor '{}') version '{}' from API response.",
                                Objects.toString(esBuildFlavor, "unknown"), esVersionNumber);
                        if (ELASTICSEARCH_OSS_BUILD_FLAVOR.equalsIgnoreCase(esBuildFlavor)) {
                            effectiveDistribution = ELASTICSEARCH_DISTRIBUTION + "-" + ELASTICSEARCH_OSS_BUILD_FLAVOR;
                        }
                        return Pair.of(effectiveDistribution, esVersionNumber);
                    }
                    LOG.warn("Version number is null in Elasticsearch API response. Proceeding to default.");
                }
            } catch (final Exception e) {
                LOG.warn("Failed to get cluster info using Elasticsearch client. Will proceed with default behavior.", e);
            }
        } else {
            LOG.info("Elasticsearch client refresher is null, skipping Elasticsearch .info() call.");
        }

        if (openSearchSourceConfiguration.getDistributionVersion() == null) { 
            LOG.warn("Failed to auto-detect cluster distribution from API response for both OpenSearch and (if attempted) Elasticsearch clients. " +
                     "Defaulting to OpenSearch distribution. Consider setting 'distribution_version' in the configuration if this is not an OpenSearch cluster.");
            return Pair.of(OPENSEARCH_DISTRIBUTION, OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
        } else {
            final String configuredDistributionName = openSearchSourceConfiguration.getDistributionVersion().name();
            LOG.error("Failed to connect to the cluster and verify its distribution. The 'distribution_version' was configured as '{}', " +
                      "but communication attempts with the cluster failed or the cluster did not respond as expected for this distribution type. " +
                      "Please check the endpoint, network connectivity, and the specified 'distribution_version'.",
                      configuredDistributionName);
            throw new InvalidPluginConfigurationException(
                String.format("Failed to connect to the cluster and verify its distribution. The 'distribution_version' was configured as '%s', " +
                              "but communication attempts with the cluster failed or the cluster did not respond as expected for this distribution type. " +
                              "Please check the endpoint, network connectivity, and the specified 'distribution_version'.",
                              configuredDistributionName));
        }
    }

    private void validateDistribution(final String distribution) {
        if (!distribution.equals(OPENSEARCH_DISTRIBUTION) && !distribution.startsWith(ELASTICSEARCH_DISTRIBUTION)) {
            throw new IllegalArgumentException(String.format("Only %s or %s distributions are supported at this time. The cluster distribution being used is '%s'",
                    OPENSEARCH_DISTRIBUTION, ELASTICSEARCH_DISTRIBUTION, distribution));
        }
    }
}
