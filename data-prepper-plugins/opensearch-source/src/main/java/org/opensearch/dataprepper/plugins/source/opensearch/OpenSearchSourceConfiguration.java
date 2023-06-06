/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;

import java.util.List;
import java.util.Objects;

public class OpenSearchSourceConfiguration {

    /**
     * 0 indicates infinite retries
     */
    @JsonProperty("max_retries")
    @Min(0)
    private Integer maxRetries = 0;

    @NotNull
    @JsonProperty("hosts")
    private List<String> hosts;

    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

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
    private SchedulingParameterConfiguration schedulingParameterConfiguration;

    @JsonProperty("search_options")
    @Valid
    private SearchConfiguration searchConfiguration;

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
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

    @AssertTrue(message = "Either username and password, or aws options must be specified. Both cannot be set at once.")
    boolean validateAwsConfigWithUsernameAndPassword() {

        return !((Objects.nonNull(awsAuthenticationOptions) && (Objects.nonNull(username) || Objects.nonNull(password))) ||
                (Objects.isNull(awsAuthenticationOptions) && (Objects.isNull(username) || Objects.isNull(password))));
    }

}
