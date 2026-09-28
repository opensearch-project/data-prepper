/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.Min;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyNamingPolicy;

import java.util.Collections;
import java.util.List;

/**
 * Settings for synthesizing typed dependency nodes (databases, message brokers, external endpoints)
 * that do not emit their own SERVER span.
 */
public class DependencyNodesConfig {
    static final int DEFAULT_MAX_DEPENDENCIES_PER_SERVICE = 100;
    static final int DEFAULT_MAX_REMOTE_OPERATIONS_PER_SERVICE = 100;

    @JsonProperty("enabled")
    @JsonPropertyDescription("When true, CLIENT calls without a downstream SERVER span and PRODUCER/CONSUMER spans are " +
            "emitted as typed database, external and messaging nodes with RED metrics. Defaults to true. Set it to false " +
            "if your OpenSearch Dashboards version predates the dependency-aware APM UI, which would otherwise list " +
            "dependencies as services.")
    private boolean enabled = true;

    @Min(1)
    @JsonProperty("max_dependencies_per_service")
    @JsonPropertyDescription("Maximum distinct dependency names per source service per window. Further dependencies are " +
            "collapsed into a single OtherRemoteService node.")
    private int maxDependenciesPerService = DEFAULT_MAX_DEPENDENCIES_PER_SERVICE;

    @Min(1)
    @JsonProperty("max_remote_operations_per_service")
    @JsonPropertyDescription("Maximum distinct dependency remote operations per source service per window. Further " +
            "operations are collapsed into OtherRemoteOperation.")
    private int maxRemoteOperationsPerService = DEFAULT_MAX_REMOTE_OPERATIONS_PER_SERVICE;

    @JsonProperty("hostname_denylist_patterns")
    @JsonPropertyDescription("Regular expressions matched against a dependency host name. A matching host does not " +
            "create a node. The default matches IP-derived host names such as ip-10-0-0-5.ec2.internal.")
    private List<String> hostnameDenylistPatterns = DependencyNamingPolicy.DEFAULT_HOSTNAME_DENYLIST_PATTERNS;

    public DependencyNodesConfig() {
    }

    DependencyNodesConfig(final boolean enabled,
                          final int maxDependenciesPerService,
                          final int maxRemoteOperationsPerService,
                          final List<String> hostnameDenylistPatterns) {
        this.enabled = enabled;
        this.maxDependenciesPerService = maxDependenciesPerService;
        this.maxRemoteOperationsPerService = maxRemoteOperationsPerService;
        this.hostnameDenylistPatterns = hostnameDenylistPatterns;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getMaxDependenciesPerService() {
        return maxDependenciesPerService;
    }

    public int getMaxRemoteOperationsPerService() {
        return maxRemoteOperationsPerService;
    }

    public List<String> getHostnameDenylistPatterns() {
        return hostnameDenylistPatterns != null ? Collections.unmodifiableList(hostnameDenylistPatterns) : Collections.emptyList();
    }
}
