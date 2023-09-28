/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.configuration.NamespaceConfig;

import java.time.Duration;
import java.util.List;

public class CloudwatchMetricsSourceConfig {

    static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(10);

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("namespaces")
    @NotNull
    @Valid
    private List<NamespaceConfig> namespaceConfigs;

    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_BUFFER_TIMEOUT;

    public Duration getBufferTimeout() {
        return bufferTimeout;
    }

    /**
     * Aws Authentication configuration Options.
     *
     * @return aws authentication options.
     */
    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }


    /**
     * Metrics configuration.
     *
     * @return Metrics config options.
     */
    public List<NamespaceConfig> getNamespaceConfigs() {
        return namespaceConfigs;
    }

}