/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;

/**
 * The Data Prepper configuration for source coordination to be used for sources that implement {@link UsesSourceCoordination}
 * @since 2.2
 */

public class SourceCoordinationConfig {

    private static final String SOURCE_COORDINATOR_METRIC_PREFIX = "source-coordinator";
    static final Duration DEFAULT_LEASE_TIMEOUT = Duration.ofMinutes(10);
    static final String DEFAULT_SOURCE_COORDINATION_STORE = "in_memory";
    private final PluginSetting sourceCoordinationStoreConfig;
    private final String partitionPrefix;
    private final Duration leaseTimeout;

    @JsonCreator
    public SourceCoordinationConfig(@JsonProperty("store") final PluginModel sourceCoordinationStoreConfig,
                                    @JsonProperty("partition_prefix") final String partitionPrefix,
                                    @JsonProperty("lease_timeout") final Duration leaseTimeout) {
        Objects.requireNonNull(sourceCoordinationStoreConfig, "source_coordination store must not be null");

        this.sourceCoordinationStoreConfig = new PluginSetting(sourceCoordinationStoreConfig.getPluginName(), sourceCoordinationStoreConfig.getPluginSettings());
        this.sourceCoordinationStoreConfig.setPipelineName(SOURCE_COORDINATOR_METRIC_PREFIX);

        this.partitionPrefix = partitionPrefix;
        if (leaseTimeout != null) {
            this.leaseTimeout = leaseTimeout;
        } else {
            this.leaseTimeout = DEFAULT_LEASE_TIMEOUT;
        }
    }

    public static SourceCoordinationConfig getDefaultSourceCoordinationConfig() {
        return new SourceCoordinationConfig(
                new PluginModel(DEFAULT_SOURCE_COORDINATION_STORE, Collections.emptyMap()),
                null,
                null);
    }

    public PluginSetting getSourceCoordinationStoreConfig() {
        return sourceCoordinationStoreConfig;
    }

    public String getPartitionPrefix() {
        return partitionPrefix;
    }

    public Duration getLeaseTimeout() {
        return leaseTimeout;
    }
}
