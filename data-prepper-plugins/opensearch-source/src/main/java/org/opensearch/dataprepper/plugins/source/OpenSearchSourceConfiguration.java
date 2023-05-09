/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.configuration.QueryParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.configuration.RetryConfiguration;
import org.opensearch.dataprepper.plugins.source.configuration.SearchConfiguration;


import java.util.Map;

public class OpenSearchSourceConfiguration {

    @JsonProperty("connection")
    private ConnectionConfiguration connectionConfiguration;

    @JsonProperty("indices")
    private IndexParametersConfiguration indexParametersConfiguration;

    @JsonProperty("aws")
    private AwsAuthenticationConfiguration awsAuthenticationOptions;

    @JsonProperty("scheduling")
    private SchedulingParameterConfiguration schedulingParameterConfiguration;

    @JsonProperty("query")
    private QueryParameterConfiguration queryParameterConfiguration;

    @JsonProperty("search_options")
    private SearchConfiguration searchConfiguration;

    @JsonProperty("retry")
    private RetryConfiguration retryConfiguration;

    private Map<String,String> indexNames;

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

    public QueryParameterConfiguration getQueryParameterConfiguration() {
        return queryParameterConfiguration;
    }

    public SearchConfiguration getSearchConfiguration() {
        return searchConfiguration;
    }

    public Map<String, String> getIndexNames() {
        return indexNames;
    }

}
