/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.jira.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.FilterConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

import java.util.List;

@Getter
public class JiraSourceConfig implements CrawlerSourceConfig {

    private static final int DEFAULT_BATCH_SIZE = 50;

    /**
     * Jira account url
     */
    @JsonProperty("hosts")
    private List<String> hosts;

    @AssertTrue(message = "Jira hosts must be a list of length 1")
    boolean isValidHosts() {
        return hosts != null && hosts.size() == 1;
    }

    /**
     * Authentication Config to Access Jira
     */
    @JsonProperty("authentication")
    @Valid
    private AuthenticationConfig authenticationConfig;

    /**
     * Batch size for fetching tickets
     */
    @JsonProperty("batch_size")
    private int batchSize = DEFAULT_BATCH_SIZE;


    /**
     * Filter Config to filter what tickets get ingested
     */
    @JsonProperty("filter")
    private FilterConfig filterConfig;


    /**
     * Boolean property indicating end to end acknowledgments state
     */
    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    public String getAccountUrl() {
        return this.getHosts().get(0);
    }

    public String getAuthType() {
        return this.getAuthenticationConfig().getAuthType();
    }
}
