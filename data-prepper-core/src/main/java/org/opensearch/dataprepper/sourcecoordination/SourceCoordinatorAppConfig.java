/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.SourceCoordinator;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.parser.model.ServiceDiscoveryConfiguration;
import org.opensearch.dataprepper.parser.model.sourcecoordination.DynamoDBSourceCoordinationStoreConfig;
import org.opensearch.dataprepper.parser.model.sourcecoordination.SourceCoordinationConfig;
import org.opensearch.dataprepper.parser.model.sourcecoordination.SourceCoordinationStoreConfig;
import org.opensearch.dataprepper.peerforwarder.HashRing;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;
import org.opensearch.dataprepper.peerforwarder.discovery.PeerListProvider;
import org.opensearch.dataprepper.sourcecoordination.dynamo.DynamoDbSourceCoordinator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Objects;

@Configuration
public class SourceCoordinatorAppConfig {

    public static final int NUM_VIRTUAL_NODES = 128;

    static final String COMPONENT_SCOPE = "source";
    static final String COMPONENT_ID = "coordinator";

    @Bean(name = "sourceCoordinationMetrics")
    public PluginMetrics pluginMetrics() {
        return PluginMetrics.fromNames(COMPONENT_ID, COMPONENT_SCOPE);
    }

    @Bean
    public SourceCoordinationConfig provideSourceCoordinationConfig(@Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {

        if (dataPrepperConfiguration != null && dataPrepperConfiguration.getSourceCoordinationConfig() != null) {
            return dataPrepperConfiguration.getSourceCoordinationConfig();
        }

        return new SourceCoordinationConfig();
    }

    @Bean
    public ServiceDiscoveryConfiguration provideServiceDiscoveryConfiguration(@Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration != null && dataPrepperConfiguration.getSourceCoordinationConfig() != null) {
            return dataPrepperConfiguration.getServiceDiscoveryConfiguration();
        }

        return new ServiceDiscoveryConfiguration();
    }

    @Bean
    public HashRing provideHashRing(final ServiceDiscoveryConfiguration serviceDiscoveryConfiguration,
                                    @Qualifier("sourceCoordinationMetrics") final PluginMetrics pluginMetrics) {

        final DiscoveryMode discoveryMode = serviceDiscoveryConfiguration.getDiscoveryMode();
        final PeerListProvider peerListProvider = discoveryMode.create(serviceDiscoveryConfiguration, pluginMetrics);
        return new HashRing(peerListProvider, NUM_VIRTUAL_NODES);
    }

    @Bean
    public SourceCoordinator provideSourceCoordinator(final HashRing hashRing,
                                                      final SourceCoordinationConfig sourceCoordinationConfig) {

        final SourceCoordinationStoreConfig sourceCoordinationStoreConfig = sourceCoordinationConfig.getSourceCoordinationStoreConfig();

        if (Objects.isNull(sourceCoordinationStoreConfig)) {
            return null;
        }

        if (sourceCoordinationStoreConfig instanceof DynamoDBSourceCoordinationStoreConfig) {
            return new DynamoDbSourceCoordinator(sourceCoordinationConfig, new PartitionManager(), hashRing);
        }

        return null;
    }
}
