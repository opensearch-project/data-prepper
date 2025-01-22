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
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.jira.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.FilterConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

import java.time.Duration;
import java.util.List;

@Getter
public class JiraSourceConfig implements CrawlerSourceConfig {

    private static final Duration DEFAULT_BACKOFF_MILLIS = Duration.ofMinutes(2);

    /**
     * Jira account url
     */
    @JsonProperty("hosts")
    private List<String> hosts;

    /**
     * Authentication Config to Access Jira
     */
    @JsonProperty("authentication")
    @Valid
    private AuthenticationConfig authenticationConfig;


    /**
     * Filter Config to filter what tickets get ingested
     */
    @JsonProperty("filter")
    private FilterConfig filterConfig;


    /**
     * Number of worker threads to spawn to parallel source fetching
     */
    @JsonProperty("workers")
    private int numWorkers = DEFAULT_NUMBER_OF_WORKERS;

    /**
     * Default time to wait (with exponential backOff) in the case of
     * waiting for the source service to respond
     */
    @JsonProperty("backoff_time")
    private Duration backOff = DEFAULT_BACKOFF_MILLIS;

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
