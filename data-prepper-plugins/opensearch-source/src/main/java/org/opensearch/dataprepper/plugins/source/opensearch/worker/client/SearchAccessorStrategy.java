/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.util.MissingRequiredPropertyException;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.plugins.source.opensearch.ElasticsearchClientRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchClientRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final PluginConfigObservable pluginConfigObservable;

    public static SearchAccessorStrategy create(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                final OpenSearchClientFactory openSearchClientFactory,
                                                final PluginConfigObservable pluginConfigObservable) {
        return new SearchAccessorStrategy(openSearchSourceConfiguration, openSearchClientFactory, pluginConfigObservable);
    }

    private SearchAccessorStrategy(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                   final OpenSearchClientFactory openSearchClientFactory,
                                   final PluginConfigObservable pluginConfigObservable) {
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

        final OpenSearchClientRefresher openSearchClientRefresher = new OpenSearchClientRefresher(
                openSearchClientFactory, openSearchSourceConfiguration);

        if (Objects.nonNull(openSearchSourceConfiguration.getAwsAuthenticationOptions()) &&
                openSearchSourceConfiguration.getAwsAuthenticationOptions().isServerlessCollection()) {
            return createSearchAccessorForServerlessCollection(openSearchClientRefresher);
        }

        InfoResponse infoResponse = null;

        ElasticsearchClientRefresher elasticsearchClientRefresher = null;
        try {
            infoResponse = openSearchClientRefresher.get().info();
            pluginConfigObservable.addPluginConfigSubscriber(newConfig -> openSearchClientRefresher.update(
                    (OpenSearchSourceConfiguration) newConfig));
        } catch (final MissingRequiredPropertyException e) {
            LOG.info("Detected Elasticsearch cluster. Constructing Elasticsearch client");
            elasticsearchClientRefresher = new ElasticsearchClientRefresher(
                    openSearchClientFactory, openSearchSourceConfiguration);
            final ElasticsearchClientRefresher finalElasticsearchClientRefresher = elasticsearchClientRefresher;
            pluginConfigObservable.addPluginConfigSubscriber(
                    newConfig -> finalElasticsearchClientRefresher.update((OpenSearchSourceConfiguration) newConfig));
        } catch (final IOException | OpenSearchException e) {
            throw new RuntimeException("There was an error looking up the OpenSearch cluster info: ", e);
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

        if (Objects.isNull(elasticsearchClientRefresher)) {
            return new OpenSearchAccessor(openSearchClientRefresher,
                    searchContextType);
        }

        return new ElasticsearchAccessor(elasticsearchClientRefresher, searchContextType);
    }

    private SearchAccessor createSearchAccessorForServerlessCollection(final OpenSearchClientRefresher openSearchClientRefresher) {
        if (Objects.isNull(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType())) {
            LOG.info("Configured with AOS serverless flag as true, defaulting to search_context_type as 'none', which uses search_after");
            return new OpenSearchAccessor(openSearchClientRefresher,
                    SearchContextType.NONE);
        } else {
            if (SearchContextType.POINT_IN_TIME.equals(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType()) ||
                SearchContextType.SCROLL.equals(openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType())) {
                throw new InvalidPluginConfigurationException("A search_context_type of point_in_time or scroll is not supported for serverless collections");
            }

            LOG.info("Using search_context_type set in the config: '{}'", openSearchSourceConfiguration.getSearchConfiguration().getSearchContextType().toString().toLowerCase());
            return new OpenSearchAccessor(openSearchClientRefresher,
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

    private Pair<String, String> getDistributionAndVersionNumber(final InfoResponse infoResponseOpenSearch, final ElasticsearchClientRefresher elasticsearchClientRefresher) {
        if (Objects.nonNull(infoResponseOpenSearch)) {
            return Pair.of(infoResponseOpenSearch.version().distribution(), infoResponseOpenSearch.version().number());
        }

        try {
            final co.elastic.clients.elasticsearch.core.InfoResponse infoResponseElasticsearch = elasticsearchClientRefresher.get().info();
            return Pair.of(ELASTICSEARCH_DISTRIBUTION + "-" + infoResponseElasticsearch.version().buildFlavor(), infoResponseElasticsearch.version().number());
        } catch (final Exception e) {
            throw new RuntimeException("Unable to call info API using the elasticsearch client", e);
        }
    }

    private void validateDistribution(final String distribution) {
        if (!distribution.equals(OPENSEARCH_DISTRIBUTION) && !distribution.startsWith(ELASTICSEARCH_DISTRIBUTION)) {
            throw new IllegalArgumentException(String.format("Only %s or %s distributions are supported at this time. The cluster distribution being used is '%s'",
                    OPENSEARCH_DISTRIBUTION, ELASTICSEARCH_DISTRIBUTION, distribution));
        }
    }
}
