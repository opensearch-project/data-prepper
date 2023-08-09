/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.configuration.NamespacesListConfig;

import java.util.List;

public class CloudwatchMetricsSourceConfig {

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("batch_size")
    private Integer batchSize = 1000;

    @JsonProperty("namespaces")
    @NotNull
    @Valid
    private List<NamespacesListConfig> namespacesListConfig;

    /**
     * Aws Authentication configuration Options.
     * @return aws authentication options.
     */
    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Metrics configuration.
     * @return Metrics config options.
     */
    public List<NamespacesListConfig> getNamespacesListConfig() {
        return namespacesListConfig;
    }
}