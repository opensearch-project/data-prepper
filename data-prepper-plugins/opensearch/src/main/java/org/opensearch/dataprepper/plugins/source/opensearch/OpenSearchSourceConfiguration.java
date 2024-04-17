/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DistributionVersion;

import java.util.List;
import java.util.Objects;

public class OpenSearchSourceConfiguration {

    @NotNull
    @JsonProperty("hosts")
    private List<String> hosts;

    @Deprecated
    @JsonProperty("username")
    private String username;

    @Deprecated
    @JsonProperty("password")
    private String password;

    @JsonProperty("authentication")
    private AuthConfig authConfig;

    @JsonProperty("disable_authentication")
    private Boolean disableAuthentication = false;

    @JsonProperty("acknowledgments")
    private Boolean acknowledgments = false;

    @JsonProperty("connection")
    @Valid
    private ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();

    @JsonProperty("indices")
    @Valid
    private IndexParametersConfiguration indexParametersConfiguration;

    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationConfiguration awsAuthenticationOptions;

    @JsonProperty("scheduling")
    @Valid
    private SchedulingParameterConfiguration schedulingParameterConfiguration = new SchedulingParameterConfiguration();

    @JsonProperty("search_options")
    @Valid
    private SearchConfiguration searchConfiguration = new SearchConfiguration();

    @JsonProperty("distribution_version")
    private String distributionVersion;

    @JsonIgnore
    private DistributionVersion distributionVersionValue;

    public List<String> getHosts() {
        return hosts;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Boolean isAuthenticationDisabled() { return disableAuthentication; }

    public Boolean isAcknowledgmentsEnabled() {
        return acknowledgments;
    }

    public ConnectionConfiguration getConnectionConfiguration() {
        return connectionConfiguration;
    }

    public IndexParametersConfiguration getIndexParametersConfiguration() {
        return indexParametersConfiguration;
    }

    public AwsAuthenticationConfiguration getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public SchedulingParameterConfiguration getSchedulingParameterConfiguration() {
        return schedulingParameterConfiguration;
    }

    public SearchConfiguration getSearchConfiguration() {
        return searchConfiguration;
    }

    public DistributionVersion getDistributionVersion() { return distributionVersionValue; }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    void validateAuthConfigConflictWithDeprecatedUsernameAndPassword() {
        if (Objects.nonNull(authConfig) && (Objects.nonNull(username) || Objects.nonNull(password))) {
            throw new InvalidPluginConfigurationException(
                    "Deprecated username and password cannot be set when authentication is configured.");
        }
    }

    void validateAwsConfigWithUsernameAndPassword() {
        final String username;
        final String password;
        if (Objects.nonNull(authConfig)) {
            username = authConfig.getUsername();
            password = authConfig.getPassword();
        } else {
            username = this.username;
            password = this.password;
        }

        if (((Objects.nonNull(awsAuthenticationOptions) && ((Objects.nonNull(username) || Objects.nonNull(password)) || disableAuthentication)) ||
                (Objects.nonNull(username) || Objects.nonNull(password)) && disableAuthentication) ||
                (Objects.isNull(awsAuthenticationOptions) && (Objects.isNull(username) || Objects.isNull(password)) && !disableAuthentication)) {
            throw new InvalidPluginConfigurationException("Either username and password, or aws options must be specified. Both cannot be set at once. Authentication can be disabled by setting the disable_authentication flag to true.");
        }
    }

    @AssertTrue(message = "distribution_version must be one of [ 'es7', 'opensearch' ]")
    boolean isDistributionVersionValid() {
        try {
            if (distributionVersion != null) {
                distributionVersionValue = DistributionVersion.valueOf(distributionVersion.toUpperCase());
            }

            return true;
        } catch (final IllegalArgumentException e) {
            return false;
        }
    }

}
