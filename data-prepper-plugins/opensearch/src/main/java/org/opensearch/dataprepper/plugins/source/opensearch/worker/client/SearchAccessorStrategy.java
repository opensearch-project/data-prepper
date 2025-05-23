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

    private SearchContextType determineSearchContextType(final String distribution, final String versionNumber) {
        if (Objects.nonNull(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType())) {
            validateSearchContextTypeOverride(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType(), distribution, versionNumber);
            return openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType();
        } else if (versionSupportsPointInTime(distribution, versionNumber)) {
            return SearchContextType.POINT_IN_TIME;
        } else {
            return SearchContextType.SCROLL;
        }
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

        final DistributionVersion configuredDistribution = openSearchSourceConfiguration.getDistributionVersion();
        if (Objects.nonNull(configuredDistribution)) {
            SearchContextType searchContextType = openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType();
            if (searchContextType == null) {
                searchContextType = SearchContextType.SCROLL;
            } else if (searchContextType == SearchContextType.POINT_IN_TIME) {
                LOG.warn("search_context_type is POINT_IN_TIME for a forced distribution ('{}'). Version compatibility cannot be verified without connecting to the cluster.", configuredDistribution);
            }

            if (configuredDistribution == DistributionVersion.OPENSEARCH) {
                pluginConfigObservable.addPluginConfigObserver(newConfig -> clientRefresher.update((OpenSearchSourceConfiguration) newConfig));
                return new OpenSearchAccessor(clientRefresher, searchContextType);
            } else {
                PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> elasticsearchClientRefresher =
                        new ClientRefresher<>(openSearchSourcePluginMetrics,
                                ElasticsearchClient.class, openSearchClientFactory::provideElasticSearchClient,
                                openSearchSourceConfiguration);
                pluginConfigObservable.addPluginConfigObserver(newConfig -> elasticsearchClientRefresher.update((OpenSearchSourceConfiguration) newConfig));
                return new ElasticsearchAccessor(elasticsearchClientRefresher, searchContextType);
            }
        }

        InfoResponse infoResponse = null;
        PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> elasticsearchClientRefresher = null;

        try {
            infoResponse = clientRefresher.get().info();
            pluginConfigObservable.addPluginConfigObserver(newConfig -> clientRefresher.update((OpenSearchSourceConfiguration) newConfig));
        } catch (final Exception e) {
            try {
                final PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> esRefresherInCatch =
                        new ClientRefresher<>(openSearchSourcePluginMetrics,
                                ElasticsearchClient.class, openSearchClientFactory::provideElasticSearchClient,
                                openSearchSourceConfiguration);
                esRefresherInCatch.get();
                pluginConfigObservable.addPluginConfigObserver(newConfig -> esRefresherInCatch.update((OpenSearchSourceConfiguration) newConfig));
                elasticsearchClientRefresher = esRefresherInCatch;
            } catch (final Exception ex) {
            }
        }

        final Pair<String, String> distributionAndVersion = getDistributionAndVersionNumber(infoResponse, elasticsearchClientRefresher);
        final String distribution = distributionAndVersion.getLeft();
        final String versionNumber = distributionAndVersion.getRight();

        validateDistribution(distribution);

        SearchContextType searchContextType = determineSearchContextType(distribution, versionNumber);

        String accessorTypeName;
        SearchAccessor accessor;

        if (OPENSEARCH_DISTRIBUTION.equals(distribution) || distribution.startsWith(OPENSEARCH_DISTRIBUTION + "-")) {
            accessorTypeName = "OpenSearch";
            accessor = new OpenSearchAccessor(clientRefresher, searchContextType);
        } else {
            accessorTypeName = "Elasticsearch";
            PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> finalEsRefresherForAccessor = elasticsearchClientRefresher;
            if (finalEsRefresherForAccessor == null) {
                final PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> newlyCreatedEsRefresher =
                        new ClientRefresher<>(openSearchSourcePluginMetrics,
                                ElasticsearchClient.class, openSearchClientFactory::provideElasticSearchClient,
                                openSearchSourceConfiguration);

                pluginConfigObservable.addPluginConfigObserver(newConfig -> newlyCreatedEsRefresher.update((OpenSearchSourceConfiguration) newConfig));
                finalEsRefresherForAccessor = newlyCreatedEsRefresher;
            }
            accessor = new ElasticsearchAccessor(finalEsRefresherForAccessor, searchContextType);
        }
        LOG.info("Using {}Accessor. Determined distribution: '{}', version: '{}'. Search_context_type: '{}'.",
                accessorTypeName, distribution, versionNumber, searchContextType);

        if (searchContextType == SearchContextType.SCROLL &&
                openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType() == null) {
            boolean isElasticsearch = distribution.startsWith(ELASTICSEARCH_DISTRIBUTION);
            String upgradeDistributionName = isElasticsearch ? "Elasticsearch" : "OpenSearch";
            String upgradeVersionCutoff = isElasticsearch ? ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF : OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF;

            LOG.info("Scroll contexts will be used because {} version {} does not support Point in Time APIs. " +
                            "To use Point in Time APIs, consider upgrading your cluster to at least {} {}.",
                    distribution, versionNumber, upgradeDistributionName, upgradeVersionCutoff);
        }

        return accessor;
    }

    private SearchAccessor createSearchAccessorForServerlessCollection(final PluginComponentRefresher<OpenSearchClient, OpenSearchSourceConfiguration> clientRefresher) {
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

    private Pair<String, String> getDistributionAndVersionNumber(
            final InfoResponse infoResponseFromInitialOpenSearchAttempt,
            final PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> elasticsearchRefresherAfterOpenSearchFailure) {

        final DistributionVersion configuredVersionEnum = openSearchSourceConfiguration.getDistributionVersion();
        if (Objects.nonNull(configuredVersionEnum)) {
            LOG.info("distribution_version is configured to '{}'. Trusting this configuration and skipping API-based auto-detection.", configuredVersionEnum.name());
            if (configuredVersionEnum == DistributionVersion.OPENSEARCH) {
                return Pair.of(OPENSEARCH_DISTRIBUTION, OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
            } else {
                return Pair.of(ELASTICSEARCH_DISTRIBUTION, ELASTICSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
            }
        }

        if (Objects.nonNull(infoResponseFromInitialOpenSearchAttempt)) {
            final String distribution = infoResponseFromInitialOpenSearchAttempt.version().distribution();
            final String versionNumber = infoResponseFromInitialOpenSearchAttempt.version().number();
            if (Objects.nonNull(distribution) && Objects.nonNull(versionNumber)) {
                LOG.info("Auto-detected OpenSearch distribution '{}' version '{}' from OpenSearch client API response.", distribution, versionNumber);
                return Pair.of(distribution, versionNumber);
            }
        } else {
            if (elasticsearchRefresherAfterOpenSearchFailure != null) {
                try {
                    final ElasticsearchClient elasticsearchClient = elasticsearchRefresherAfterOpenSearchFailure.get();
                    if (elasticsearchClient != null) {
                        final co.elastic.clients.elasticsearch.core.InfoResponse infoResponseElasticsearch = elasticsearchClient.info();
                        final String esBuildFlavor = infoResponseElasticsearch.version().buildFlavor();
                        final String esVersionNumber = infoResponseElasticsearch.version().number();

                        if (Objects.nonNull(esVersionNumber)) {
                            String effectiveDistribution = ELASTICSEARCH_DISTRIBUTION;
                            LOG.info("Auto-detected Elasticsearch (flavor '{}') version '{}' from Elasticsearch client API response.",
                                    Objects.toString(esBuildFlavor, "unknown"), esVersionNumber);
                            if (ELASTICSEARCH_OSS_BUILD_FLAVOR.equalsIgnoreCase(esBuildFlavor)) {
                                effectiveDistribution = ELASTICSEARCH_DISTRIBUTION + "-" + ELASTICSEARCH_OSS_BUILD_FLAVOR;
                            }
                            return Pair.of(effectiveDistribution, esVersionNumber);
                        }
                    }
                } catch (final Exception e) {
                }
            }
        }

        LOG.warn("Failed to auto-detect cluster distribution because API calls failed or returned inconclusive data, and 'distribution_version' was not configured. Defaulting to OpenSearch distribution.");
        return Pair.of(OPENSEARCH_DISTRIBUTION, OPENSEARCH_POINT_IN_TIME_SUPPORT_VERSION_CUTOFF);
    }

    private void validateDistribution(final String distribution) {
        if (!distribution.equals(OPENSEARCH_DISTRIBUTION) && !distribution.startsWith(ELASTICSEARCH_DISTRIBUTION)) {
            throw new IllegalArgumentException(String.format("Only %s or %s distributions are supported at this time. The cluster distribution being used is '%s'",
                    OPENSEARCH_DISTRIBUTION, ELASTICSEARCH_DISTRIBUTION, distribution));
        }
    }
}
